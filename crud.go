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
	"log"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sg-bucket"
)

// The persistent portion of a lolrus object (the stuff that gets archived to disk.)
type lolrusData struct {
	LastSeq    uint64                // Last sequence number assigned
	Docs       map[string]*lolrusDoc // Maps doc ID -> lolrusDoc
	DesignDocs map[string]*DesignDoc // Stores source form of design docs
}

// Simple, inefficient in-memory implementation of Bucket interface.
// http://ihasabucket.com
type lolrus struct {
	name         string                     // Name of the bucket
	path         string                     // Filesystem path, if it's persistent
	saving       bool                       // Is a pending save in progress?
	lastSeqSaved uint64                     // LastSeq at time of last save
	lock         sync.RWMutex               // For thread-safety
	views        map[string]lolrusDesignDoc // Stores runtime view/index data
	tapFeeds     []*tapFeedImpl
	lolrusData
}

// A document stored in a lolrus's .Docs map
type lolrusDoc struct {
	Raw      []byte // Raw data content, or nil if deleted
	IsJSON   bool   // Is the data a JSON document?
	Sequence uint64 // Current sequence number assigned
}

// Creates a simple in-memory Bucket, suitable only for amusement purposes & testing.
// The Bucket is created empty. There is no way to save it persistently.
func NewBucket(bucketName string) sgbucket.Bucket {
	log.Printf("NewBucket %s", bucketName)
	bucket := &lolrus{
		name: bucketName,
		lolrusData: lolrusData{
			Docs:       map[string]*lolrusDoc{},
			DesignDocs: map[string]*DesignDoc{},
		},
		views: map[string]lolrusDesignDoc{},
	}
	runtime.SetFinalizer(bucket, (*lolrus).Close)
	return bucket
}

var buckets map[[3]string]sgbucket.Bucket
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
func GetBucket(url, poolName, bucketName string) (sgbucket.Bucket, error) {
	bucketsLock.Lock()
	defer bucketsLock.Unlock()

	if buckets == nil {
		buckets = make(map[[3]string]sgbucket.Bucket)
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

func (bucket *lolrus) VBHash(docID string) uint32 {
	return 0
}

// Generates the next sequence number to assign to a document update. (Use only while locked)
func (bucket *lolrus) _nextSequence() uint64 {
	bucket._saveSoon()
	bucket.LastSeq++
	return bucket.LastSeq
}

func (bucket *lolrus) GetName() string {
	return bucket.name // name is immutable so this needs no lock
}

func (bucket *lolrus) Close() {
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
		log.Printf("Close %s", bucket.GetName())
		bucket._closePersist()
		bucket.Docs = nil
		bucket.DesignDocs = nil
		bucket.views = nil
	}
}

func (bucket *lolrus) CloseAndDelete() error {
	path := bucket.path
	bucket.Close()
	if path == "" {
		return nil
	}
	return os.Remove(path)
}

func (bucket *lolrus) assertNotClosed() {
	if bucket.Docs == nil {
		panic(fmt.Sprintf("Accessing closed Walrus bucket %q", bucket.name))
	}
}

func (bucket *lolrus) missingError(key string) error {
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
func (bucket *lolrus) getRaw(k string) ([]byte, error) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	doc := bucket.Docs[k]
	if doc == nil || doc.Raw == nil {
		return nil, bucket.missingError(k)
	}
	return copySlice(doc.Raw), nil
}

func (bucket *lolrus) GetRaw(k string) ([]byte, error) {
	raw, err := bucket.getRaw(k)
	return copySlice(raw), err // Public API returns copied slice to avoid client altering doc
}

func (bucket *lolrus) Get(k string, rv interface{}) error {
	raw, err := bucket.getRaw(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, rv)
}

//////// WRITE:

func (bucket *lolrus) Write(k string, flags int, exp int, v interface{}, opt sgbucket.WriteOptions) (err error) {
	// Marshal JSON if the value is not raw:
	isJSON := (opt&sgbucket.Raw == 0)
	var data []byte
	if !isJSON {
		if v != nil {
			data = copySlice(v.([]byte))
		}
	} else {
		data, err = json.Marshal(v)
		if err != nil {
			return err
		}
	}

	// Now do the actual write:
	seq, err := bucket.write(k, exp, data, opt)
	if err != nil {
		return
	}

	// Wait for persistent save, if that flag is set:
	return bucket.waitAfterWrite(seq, opt)
}

// Waits until the given sequence has been persisted or made indexable.
func (bucket *lolrus) waitAfterWrite(seq uint64, opt sgbucket.WriteOptions) error {
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

func (bucket *lolrus) write(k string, exp int, raw []byte, opt sgbucket.WriteOptions) (seq uint64, err error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	doc := bucket.Docs[k]
	if doc == nil {
		bucket.assertNotClosed()
		if raw == nil || opt&sgbucket.Append != 0 {
			return 0, bucket.missingError(k)
		}
		doc = &lolrusDoc{}
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

	// Post a TAP notification:
	if raw != nil {
		bucket._postTapMutationEvent(k, raw, doc.Sequence)
	} else {
		bucket._postTapDeletionEvent(k, doc.Sequence)
	}

	return doc.Sequence, nil
}

//////// ADD / SET / DELETE:

func (bucket *lolrus) add(k string, exp int, v interface{}, opt sgbucket.WriteOptions) (added bool, err error) {
	err = bucket.Write(k, 0, exp, v, opt|sgbucket.AddOnly)
	if err == sgbucket.ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

func (bucket *lolrus) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	if v == nil {
		panic("nil value")
	}
	return bucket.add(k, exp, v, sgbucket.Raw)
}

func (bucket *lolrus) Add(k string, exp int, v interface{}) (added bool, err error) {
	return bucket.add(k, exp, v, 0)
}

func (bucket *lolrus) SetRaw(k string, exp int, v []byte) error {
	if v == nil {
		panic("nil value")
	}
	return bucket.Write(k, 0, exp, v, sgbucket.Raw)
}

func (bucket *lolrus) Set(k string, exp int, v interface{}) error {
	return bucket.Write(k, 0, exp, v, 0)
}

func (bucket *lolrus) Delete(k string) error {
	return bucket.Write(k, 0, 0, nil, sgbucket.Raw)
}

func (bucket *lolrus) Append(k string, data []byte) error {
	if data == nil {
		panic("nil value")
	}
	return bucket.Write(k, 0, 0, data, sgbucket.Append|sgbucket.Raw)
}

//////// UPDATE:

func (bucket *lolrus) WriteUpdate(k string, exp int, callback sgbucket.WriteUpdateFunc) error {
	var err error
	var opts sgbucket.WriteOptions
	var seq uint64
	for {
		var doc lolrusDoc = bucket.getDoc(k)
		doc.Raw, opts, err = callback(copySlice(doc.Raw))
		doc.IsJSON = doc.Raw != nil && ((opts & sgbucket.Raw) == 0)
		if err != nil {
			return err
		} else if seq = bucket.updateDoc(k, &doc); seq > 0 {
			break
		}
	}
	// Document has been updated:
	return bucket.waitAfterWrite(seq, opts)
}

func (bucket *lolrus) Update(k string, exp int, callback sgbucket.UpdateFunc) error {
	writeCallback := func(current []byte) (updated []byte, opts sgbucket.WriteOptions, err error) {
		updated, err = callback(current)
		return
	}
	return bucket.WriteUpdate(k, exp, writeCallback)
}

// Looks up a lolrusDoc and returns a copy of it, or an empty doc if one doesn't exist yet
func (bucket *lolrus) getDoc(k string) lolrusDoc {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	docPtr := bucket.Docs[k]
	if docPtr == nil {
		bucket.assertNotClosed()
		return lolrusDoc{}
	}
	return *docPtr
}

// Replaces a lolrusDoc as long as its sequence number hasn't changed yet. (Used by Update)
func (bucket *lolrus) updateDoc(k string, doc *lolrusDoc) uint64 {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	var curSequence uint64
	if curDoc := bucket.Docs[k]; curDoc != nil {
		curSequence = curDoc.Sequence
	}
	if curSequence != doc.Sequence {
		return 0
	}
	doc.Sequence = bucket._nextSequence()
	bucket.Docs[k] = doc
	bucket._postTapMutationEvent(k, doc.Raw, doc.Sequence)
	return doc.Sequence
}

//////// INCR:

func (bucket *lolrus) Incr(k string, amt, def uint64, exp int) (uint64, error) {
	if amt != 0 {
		bucket.lock.Lock()
		defer bucket.lock.Unlock()
	} else {
		bucket.lock.RLock()
		defer bucket.lock.RUnlock()
	}

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
	doc = &lolrusDoc{
		Raw:      []byte(strconv.FormatUint(counter, 10)),
		IsJSON:   false,
		Sequence: bucket._nextSequence(),
	}
	bucket.Docs[k] = doc
	bucket._postTapMutationEvent(k, doc.Raw, doc.Sequence)
	return counter, nil
}
