//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package walrus

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/google/uuid"
)

const (
	SimulatedVBucketCount = 1024 // Used when hashing doc id -> vbno
)

var MaxDocSize = 0 // Used during the write function

type DocTooBigErr struct{}

func (err DocTooBigErr) Error() string {
	return "Document value was too large!"
}

// The persistent portion of a Bucket object (the stuff that gets archived to disk.)
type walrusData struct {
	LastSeq    uint64                         // Last sequence number assigned
	Docs       map[string]*walrusDoc          // Maps doc ID -> walrusDoc
	DesignDocs map[string]*sgbucket.DesignDoc // Stores source form of design docs
}

// Simple, inefficient in-memory implementation of Bucket interface.
// http://ihasabucket.com
type WalrusBucket struct {
	name         string                     // Name of the bucket
	uuid         string                     // UUID of the bucket
	path         string                     // Filesystem path, if it's persistent
	saving       bool                       // Is a pending save in progress?
	lastSeqSaved uint64                     // LastSeq at time of last save
	lock         sync.RWMutex               // For thread-safety
	views        map[string]walrusDesignDoc // Stores runtime view/index data
	vbSeqs       sgbucket.VbucketSeqCounter // Per-vb sequence couner
	tapFeeds     []*tapFeedImpl
	walrusData
}

// A document stored in a Bucket's .Docs map
type walrusDoc struct {
	Raw      []byte // Raw data content, or nil if deleted
	IsJSON   bool   // Is the data a JSON document?
	VbNo     uint32 // The vbno (just hash of doc id)
	VbSeq    uint64 // Vb seq -- only used for doc meta for views
	Sequence uint64 // Current sequence number assigned
}

// Creates a simple in-memory Bucket, suitable only for amusement purposes & testing.
// The Bucket is created empty. There is no way to save it persistently.
func NewBucket(bucketName string) *WalrusBucket {
	logg("NewBucket %s", bucketName)
	bucket := &WalrusBucket{
		name: bucketName,
		uuid: uuid.New().String(),
		walrusData: walrusData{
			Docs:       map[string]*walrusDoc{},
			DesignDocs: map[string]*sgbucket.DesignDoc{},
		},
		vbSeqs: sgbucket.NewMapVbucketSeqCounter(SimulatedVBucketCount),
		views:  map[string]walrusDesignDoc{},
	}
	runtime.SetFinalizer(bucket, (*WalrusBucket).Close)
	return bucket
}

var buckets map[[3]string]*WalrusBucket
var bucketsLock sync.Mutex

// Returns a Walrus-based Bucket specific to the given (url, pool, bucketname) tuple.
// That is, passing the same parameters will return the same Bucket.
//
// If the urlStr has any of the forms below it will be considered a filesystem directory
// path, and the bucket will use a persistent backing file in that directory.
//		walrus:/foo/bar
//		walrus:bar
//		file:///foo/bar
//		/foo/bar
//		./bar
// The bucket's filename will be "bucketName.walrus", or if the poolName is not
// "default", "poolName-bucketName.walrus".
//
// If the URL isn't considered a directory (e.g. "walrus:" or ""), the bucket will just be
// created in memory using NewBucket.
func GetBucket(url, poolName, bucketName string) (*WalrusBucket, error) {
	bucketsLock.Lock()
	defer bucketsLock.Unlock()

	if buckets == nil {
		buckets = make(map[[3]string]*WalrusBucket)
	}
	key := [3]string{url, poolName, bucketName}
	bucket := buckets[key]
	if bucket == nil {
		dir := bucketURLToDir(url)
		if dir == "" {
			bucket = NewBucket(bucketName)
		} else {
			var err error
			bucket, err = NewPersistentBucket(dir, poolName, bucketName)
			if err != nil {
				return nil, err
			}
		}
		buckets[key] = bucket
	}
	return bucket, nil
}

// Interprets a bucket urlStr as a directory, or returns "" if it's not.
func bucketURLToDir(urlStr string) (dir string) {
	if urlStr != "" {
		if strings.HasPrefix(urlStr, "/") || strings.HasPrefix(urlStr, ".") {
			return urlStr
		}
		urlobj, _ := url.Parse(urlStr)
		if urlobj != nil && (urlobj.Scheme == "walrus" || urlobj.Scheme == "file") {
			dir = urlobj.Path
			if dir == "" {
				dir = urlobj.Opaque
			} else if strings.HasPrefix(dir, "//") {
				dir = dir[2:]
			}
			if dir == "/" {
				dir = ""
			}
		}
	}
	return
}

func (bucket *WalrusBucket) VBHash(docID string) uint32 {
	return 0
}

// Generates the next sequence number to assign to a document update. (Use only while locked)
func (bucket *WalrusBucket) _nextSequence() uint64 {
	bucket._saveSoon()
	bucket.LastSeq++
	return bucket.LastSeq
}

func (bucket *WalrusBucket) GetName() string {
	return bucket.name // name is immutable so this needs no lock
}

func (bucket *WalrusBucket) Close() {
	// Remove the bucket from the global 'buckets' map:
	bucketsLock.Lock()
	defer bucketsLock.Unlock()
	for key, value := range buckets {
		if value == bucket {
			delete(buckets, key)
			break
		}
	}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if bucket.Docs != nil {
		logg("Close %s", bucket.GetName())
		bucket._closePersist()
		bucket.Docs = nil
		bucket.DesignDocs = nil
		bucket.views = nil
	}
}

func (bucket *WalrusBucket) CloseAndDelete() error {
	path := bucket.path
	bucket.Close()
	if path == "" {
		return nil
	}
	return os.Remove(path)
}

func (bucket *WalrusBucket) assertNotClosed() {
	if bucket.Docs == nil {
		panic(fmt.Sprintf("Accessing closed Walrus bucket %q", bucket.name))
	}
}

func (bucket *WalrusBucket) missingError(key string) error {
	bucket.assertNotClosed()
	return sgbucket.MissingError{key}
}

//////// GET:

func copySlice(slice []byte) []byte {
	if slice == nil {
		return nil
	}
	copied := make([]byte, len(slice))
	copy(copied, slice)
	return copied
}

// Private version of GetRaw returns an uncopied slice.
func (bucket *WalrusBucket) getRaw(k string) ([]byte, uint64, error) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	doc := bucket.Docs[k]
	if doc == nil || doc.Raw == nil {
		return nil, 0, bucket.missingError(k)
	}
	return copySlice(doc.Raw), doc.Sequence, nil
}

func (bucket *WalrusBucket) GetRaw(k string) (rv []byte, cas uint64, err error) {
	raw, cas, err := bucket.getRaw(k)
	return copySlice(raw), cas, err // Public API returns copied slice to avoid client altering doc
}

func (bucket *WalrusBucket) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {
	// Until walrus supports expiry, the exp value is ignored
	return bucket.GetRaw(k)
}

func (bucket *WalrusBucket) Touch(k string, exp uint32) (casOut uint64, err error) {
	// Until walrus supports expiry, the touch doesn't modify expiry but
	// otherwise has correct cas/error behaviour
	_, casOut, err = bucket.getRaw(k)
	return casOut, err
}

func (bucket *WalrusBucket) Get(k string, rv interface{}) (cas uint64, err error) {
	raw, cas, err := bucket.getRaw(k)
	if err != nil {
		return 0, err
	}

	switch typedRV := rv.(type) {
	case *[]byte:
		*typedRV = copySlice(raw)
		return cas, nil
	default:
		return cas, json.Unmarshal(raw, rv)
	}
}

func (bucket *WalrusBucket) GetBulkRaw(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	errs := multiError{}
	hadError := false

	for _, key := range keys {
		value, _, err := bucket.GetRaw(key)
		if err != nil {
			errs = append(errs, err)
		} else {
			result[key] = value
		}
	}

	if hadError {
		return result, errs
	} else {
		return result, nil
	}

}

//////// WRITE:

func (bucket *WalrusBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) (err error) {
	// Marshal JSON if the value is not raw:
	isJSON := (opt&sgbucket.Raw == 0)
	data, err := bucket.getData(v, isJSON)
	if err != nil {
		return err
	}

	// Now do the actual write:
	seq, err := bucket.write(k, exp, data, opt)
	if err != nil {
		return
	}

	// Wait for persistent save, if that flag is set:
	return bucket.waitAfterWrite(seq, opt)
}

func (bucket *WalrusBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {

	// Marshal JSON if the value is not raw:
	isJSON := (opt&sgbucket.Raw == 0)
	data, err := bucket.getData(v, isJSON)
	if err != nil {
		return 0, err
	}

	doc := &walrusDoc{}
	doc.Sequence = cas
	doc.Raw = data
	doc.IsJSON = isJSON

	// Update
	casOut, err = bucket.updateDoc(k, doc)
	if err != nil {
		return 0, err
	}
	if casOut == 0 {
		return casOut, errors.New("CAS mismatch")
	}

	// Wait for persistent save, if that flag is set:
	err = bucket.waitAfterWrite(casOut, opt)
	return casOut, err
}

func (bucket *WalrusBucket) Remove(k string, cas uint64) (casOut uint64, err error) {

	doc := &walrusDoc{}
	doc.Sequence = cas
	doc.Raw = nil
	doc.IsJSON = false

	casOut, err = bucket.updateDoc(k, doc)
	if err != nil {
		return 0, err
	}
	if casOut == 0 {
		return casOut, errors.New("CAS mismatch")
	}
	return casOut, nil
}

func (bucket *WalrusBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	for _, entry := range entries {
		casOut, err := bucket.WriteCas(
			entry.Key,
			0, // flags
			0, // expiry
			entry.Cas,
			entry.Value,
			sgbucket.Raw, // WriteOptions
		)
		entry.Cas = casOut
		entry.Error = err
	}
	return nil
}

func (bucket *WalrusBucket) WriteCasWithXattr(k string, xattrKey string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	return 0, errors.New("WriteCasWithXattr not implemented for walrus")
}

func (bucket *WalrusBucket) WriteWithXattr(k string, xattrKey string, exp uint32, cas uint64, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) {
	return 0, errors.New("WriteWithXattr not implemented for walrus")
}

func (bucket *WalrusBucket) GetWithXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	return 0, errors.New("GetWithXattr not implemented for walrus")
}

func (bucket *WalrusBucket) DeleteWithXattr(k string, xattrKey string) error {
	return errors.New("DeleteWithXattr not implemented for walrus")
}

func (bucket *WalrusBucket) GetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	return 0, errors.New("GetXattr not implemented for walrus")
}

func (bucket *WalrusBucket) WriteUpdateWithXattr(k string, xattrKey string, userXattrKey string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	return 0, errors.New("WriteUpdateWithXattr not implemented for walrus")
}

func (bucket *WalrusBucket) SetXattr(k string, xattrKey string, xv []byte) (casOut uint64, err error) {
	return 0, errors.New("WriteUpdateWithXattr not implemented for walrus")
}

func (bucket *WalrusBucket) SubdocInsert(docID string, fieldPath string, cas uint64, value interface{}) error {
	return errors.New("SubdocInsert not implemented for walrus")
}

func (bucket *WalrusBucket) getData(v interface{}, isJSON bool) (data []byte, err error) {
	if !isJSON {
		if v != nil {
			data = copySlice(v.([]byte))
		}
		return data, nil
	}

	// Check for already marshalled JSON
	switch typedVal := v.(type) {
	case []byte:
		if typedVal != nil {
			data = copySlice(typedVal)
		}
	case *[]byte:
		if typedVal != nil {
			data = copySlice(*typedVal)
		}
	default:
		data, err = json.Marshal(v)
	}

	return data, err
}

// Waits until the given sequence has been persisted or made indexable.
func (bucket *WalrusBucket) waitAfterWrite(seq uint64, opt sgbucket.WriteOptions) error {
	// This method ignores the Indexable option because all writes are immediately indexable.
	if opt&sgbucket.Persist != 0 {
		if bucket.path == "" {
			return errors.New("Bucket is non-persistent")
		}
		start := time.Now()
		for {
			time.Sleep(100 * time.Millisecond)
			if bucket.isSequenceSaved(seq) {
				break
			} else if time.Since(start) > 5*time.Second {
				return sgbucket.ErrTimeout
			}
		}
	}
	return nil
}

func (bucket *WalrusBucket) write(k string, exp uint32, raw []byte, opt sgbucket.WriteOptions) (seq uint64, err error) {
	if MaxDocSize > 0 && len(raw) > MaxDocSize {
		return 0, DocTooBigErr{}
	}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	doc := bucket.Docs[k]
	if doc == nil {
		bucket.assertNotClosed()
		if raw == nil || opt&sgbucket.Append != 0 {
			return 0, bucket.missingError(k)
		}
		doc = &walrusDoc{}
		bucket.Docs[k] = doc
	} else if doc.Raw == nil {
		if raw == nil || opt&sgbucket.Append != 0 {
			return 0, bucket.missingError(k)
		}
	} else {
		if opt&sgbucket.AddOnly != 0 {
			return 0, sgbucket.ErrKeyExists
		}
		if opt&sgbucket.Append != 0 {
			raw = append(doc.Raw, raw...)
		}
	}
	doc.Raw = raw
	doc.IsJSON = (opt&(sgbucket.Raw|sgbucket.Append) == 0)
	doc.Sequence = bucket._nextSequence()
	err = bucket.SetVbAndSeq(doc, k)
	if err != nil {
		return 0, err
	}

	// Post a TAP notification:
	if raw != nil {
		bucket._postTapMutationEvent(k, raw, doc.Sequence)
	} else {
		bucket._postTapDeletionEvent(k, doc.Sequence)
	}

	return doc.Sequence, nil
}

//////// ADD / SET / DELETE:

func (bucket *WalrusBucket) add(k string, exp uint32, v interface{}, opt sgbucket.WriteOptions) (added bool, err error) {
	err = bucket.Write(k, 0, exp, v, opt|sgbucket.AddOnly)
	if err == sgbucket.ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

func (bucket *WalrusBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	if v == nil {
		panic("nil value")
	}
	return bucket.add(k, exp, v, sgbucket.Raw)
}

func (bucket *WalrusBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	return bucket.add(k, exp, v, 0)
}

func (bucket *WalrusBucket) SetRaw(k string, exp uint32, v []byte) error {
	if v == nil {
		panic("nil value")
	}
	return bucket.Write(k, 0, exp, v, sgbucket.Raw)
}

func (bucket *WalrusBucket) Set(k string, exp uint32, v interface{}) error {
	return bucket.Write(k, 0, exp, v, 0)
}

func (bucket *WalrusBucket) Delete(k string) error {
	return bucket.Write(k, 0, 0, nil, sgbucket.Raw)
}

func (bucket *WalrusBucket) Append(k string, data []byte) error {
	if data == nil {
		panic("nil value")
	}
	return bucket.Write(k, 0, 0, data, sgbucket.Append|sgbucket.Raw)
}

//////// UPDATE:

func (bucket *WalrusBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (casOut uint64, err error) {

	var opts sgbucket.WriteOptions
	var seq uint64
	for {
		var doc walrusDoc = bucket.getDoc(k)
		doc.Raw, opts, _, err = callback(copySlice(doc.Raw))
		doc.IsJSON = doc.Raw != nil && ((opts & sgbucket.Raw) == 0)
		if err != nil {
			return doc.Sequence, err
		}

		seq, err = bucket.updateDoc(k, &doc)
		if err != nil {
			return 0, err
		}

		if seq > 0 {
			break
		}

	}
	// Document has been updated:
	err = bucket.waitAfterWrite(seq, opts)
	return seq, err
}

func (bucket *WalrusBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	writeCallback := func(current []byte) (updated []byte, opts sgbucket.WriteOptions, expiry *uint32, err error) {
		updated, expiry, _, err = callback(current)
		return updated, opts, expiry, err
	}
	return bucket.WriteUpdate(k, exp, writeCallback)
}

// Looks up a walrusDoc and returns a copy of it, or an empty doc if one doesn't exist yet
func (bucket *WalrusBucket) getDoc(k string) walrusDoc {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	docPtr := bucket.Docs[k]
	if docPtr == nil {
		bucket.assertNotClosed()
		return walrusDoc{}
	}
	return *docPtr
}

// Replaces a walrusDoc as long as its sequence number hasn't changed yet. (Used by Update)
func (bucket *WalrusBucket) updateDoc(k string, doc *walrusDoc) (uint64, error) {

	if MaxDocSize > 0 && len(doc.Raw) > MaxDocSize {
		return 0, DocTooBigErr{}
	}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	var curSequence uint64
	curDoc := bucket.Docs[k]
	if curDoc != nil {
		curSequence = curDoc.Sequence
	}

	if curSequence != doc.Sequence {
		if curDoc != nil && curDoc.Raw == nil && doc.Sequence == 0 {
			// curDoc.Raw == nil represents a deleted document.  Allow update
			// when incoming cas/sequence is zero in this case
		} else {
			return 0, nil
		}
	}
	doc.Sequence = bucket._nextSequence()

	err := bucket.SetVbAndSeq(doc, k)
	if err != nil {
		return 0, nil
	}

	bucket.Docs[k] = doc
	if doc.Raw != nil {
		bucket._postTapMutationEvent(k, doc.Raw, doc.Sequence)
	} else {
		bucket._postTapDeletionEvent(k, doc.Sequence)
	}

	return doc.Sequence, nil
}

//////// INCR:

func (bucket *WalrusBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	// Potential concurrency issue if using RLock when amt=0 - can turn into a write if
	// the key doesn't exist and so gets initialized to def.  Switching to a hard write lock
	// for all operations for the time being - could be refactored back to read-then-write, but
	// probably not a significant performance concern for walrus.
	/*
		if amt != 0 {
			bucket.lock.Lock()
			defer bucket.lock.Unlock()
		} else {
			bucket.lock.RLock()
			defer bucket.lock.RUnlock()
		}
	*/
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	var counter = def
	doc := bucket.Docs[k]
	if doc != nil && doc.Raw != nil {
		var err error
		counter, err = strconv.ParseUint(string(doc.Raw), 10, 64)
		if err != nil {
			return 0, err
		}
		if amt == 0 {
			return counter, nil // just reading existing value
		}
		counter += amt
	} else {
		bucket.assertNotClosed()
		if exp < 0 {
			return 0, sgbucket.MissingError{k}
		}
		counter = def
	}
	doc = &walrusDoc{
		Raw:      []byte(strconv.FormatUint(counter, 10)),
		IsJSON:   false,
		Sequence: bucket._nextSequence(),
	}
	bucket.Docs[k] = doc
	bucket._postTapMutationEvent(k, doc.Raw, doc.Sequence)
	return counter, nil
}

func (bucket *WalrusBucket) Refresh() error {
	return nil
}

func (bucket *WalrusBucket) SetVbAndSeq(doc *walrusDoc, k string) (err error) {
	doc.VbNo = sgbucket.VBHash(k, SimulatedVBucketCount)
	doc.VbSeq, err = bucket.vbSeqs.Incr(doc.VbNo)
	return err
}

func (bucket *WalrusBucket) GetMaxVbno() (uint16, error) {
	return 1024, nil
}

func (bucket *WalrusBucket) CouchbaseServerVersion() (major uint64, minor uint64, micro string) {
	return 0, 0, "error"
}

func (bucket *WalrusBucket) UUID() (string, error) {
	return bucket.uuid, nil
}

func (bucket *WalrusBucket) IsSupported(feature sgbucket.DataStoreFeature) bool {
	switch feature {
	case sgbucket.DataStoreFeatureSubdocOperations:
		return false
	case sgbucket.DataStoreFeatureXattrs:
		return false
	case sgbucket.DataStoreFeatureN1ql:
		return false
	case sgbucket.DataStoreFeatureCrc32cMacroExpansion:
		return false
	default:
		return false
	}
}

func (bucket *WalrusBucket) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	if err == nil {
		return false
	}
	switch errorType {
	case sgbucket.KeyNotFoundError:
		_, ok := err.(sgbucket.MissingError)
		return ok
	default:
		return false
	}
}
