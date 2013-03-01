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
	"net/url"
	"strconv"
	"strings"
	"sync"
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
	name   string                     // Name of the bucket
	path   string                     // Filesystem path, if it's persistent
	saving bool                       // Is a pending save in progress?
	lock   sync.RWMutex               // For thread-safety
	views  map[string]lolrusDesignDoc // Stores runtime view/index data
	lolrusData
}

// A document stored in a lolrus's .Docs map
type lolrusDoc struct {
	Raw      []byte // Raw data content, or nil if deleted
	IsJSON   bool   // Is the data a JSON document?
	Sequence uint64 // Current sequence number assigned
}

// Creates a simple in-memory Bucket, suitable only for amusement purposes & testing.
// The Bucket is created empty. There is no way to save it persistently. The name is ignored.
func NewBucket(bucketName string) Bucket {
	return &lolrus{
		name: bucketName,
		lolrusData: lolrusData{
			Docs:       map[string]*lolrusDoc{},
			DesignDocs: map[string]*DesignDoc{},
		},
		views: map[string]lolrusDesignDoc{},
	}
}

var buckets map[[3]string]Bucket
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
func GetBucket(url, poolName, bucketName string) (Bucket, error) {
	bucketsLock.Lock()
	defer bucketsLock.Unlock()

	if buckets == nil {
		buckets = make(map[[3]string]Bucket)
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

// Generates the next sequence number to assign to a document update. (Use only while locked)
func (bucket *lolrus) _nextSequence() uint64 {
	bucket._saveSoon()
	bucket.LastSeq++
	return bucket.LastSeq
}

func (bucket *lolrus) GetName() string {
	return bucket.name // name is immutable so this needs no lock
}

//////// GET:

func (bucket *lolrus) GetRaw(k string) ([]byte, error) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	doc, found := bucket.Docs[k]
	if !found || doc.Raw == nil {
		return nil, MissingError{}
	}
	return doc.Raw, nil
}

func (bucket *lolrus) Get(k string, rv interface{}) error {
	raw, err := bucket.GetRaw(k)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, rv)
}

//////// ADD:

func (bucket *lolrus) add(k string, exp int, v []byte, isJSON bool) (added bool, err error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if _, found := bucket.Docs[k]; found {
		return false, nil
	}
	bucket.Docs[k] = &lolrusDoc{Raw: v, IsJSON: isJSON, Sequence: bucket._nextSequence()}
	return true, nil
}

func (bucket *lolrus) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	return bucket.add(k, exp, v, false)
}

func (bucket *lolrus) Add(k string, exp int, v interface{}) (added bool, err error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	return bucket.add(k, exp, raw, true)
}

//////// SET:

func (bucket *lolrus) set(k string, exp int, v []byte, isJSON bool) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	doc := bucket.Docs[k]
	if doc == nil {
		doc = &lolrusDoc{Raw: v, IsJSON: true}
		bucket.Docs[k] = doc
	} else {
		doc.Raw = v
		doc.IsJSON = true
	}
	doc.Sequence = bucket._nextSequence()
	return nil
}

func (bucket *lolrus) SetRaw(k string, exp int, v []byte) error {
	return bucket.set(k, exp, v, false)
}

func (bucket *lolrus) Set(k string, exp int, v interface{}) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return bucket.set(k, exp, raw, true)
}

//////// DELETE:

func (bucket *lolrus) Delete(k string) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	doc, found := bucket.Docs[k]
	if !found {
		return MissingError{}
	}
	doc.Raw = nil
	doc.Sequence = bucket._nextSequence()
	return nil
}

//////// UPDATE:

func (bucket *lolrus) Update(k string, exp int, callback UpdateFunc) error {
	var err error
	for {
		doc := bucket.getDoc(k)
		doc.Raw, err = callback(doc.Raw)
		if err != nil || bucket.updateDoc(k, &doc) {
			break
		}
	}
	return err
}

// Looks up a lolrusDoc and returns a copy of it, or an empty doc if one doesn't exist yet
func (bucket *lolrus) getDoc(k string) lolrusDoc {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	docPtr := bucket.Docs[k]
	if docPtr == nil {
		return lolrusDoc{}
	}
	return *docPtr
}

// Replaces a lolrusDoc as long as its sequence number hasn't changed yet. (Used by Update)
func (bucket *lolrus) updateDoc(k string, doc *lolrusDoc) bool {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	curDoc := bucket.Docs[k]
	if curDoc != nil && curDoc.Sequence != doc.Sequence {
		return false
	}
	doc.Sequence = bucket._nextSequence()
	doc.IsJSON = (doc.Raw != nil) // Doc is assumed to be JSON, unless deleted. (Used by Update)
	bucket.Docs[k] = doc
	return true
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
	doc, found := bucket.Docs[k]
	if found && doc.Raw != nil {
		var err error
		counter, err = strconv.ParseUint(string(doc.Raw), 10, 64)
		if err != nil {
			return 0, err
		}
	}
	if amt != 0 {
		counter += amt
		doc = &lolrusDoc{
			Raw:      []byte(strconv.FormatUint(counter, 10)),
			IsJSON:   false,
			Sequence: bucket._nextSequence(),
		}
		bucket.Docs[k] = doc
	}
	return counter, nil
}
