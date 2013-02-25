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
	"strconv"
	"sync"
)

// Simple, inefficient in-memory implementation of Bucket interface.
// http://ihasabucket.com
type lolrus struct {
	name       string                     // Name of the bucket
	lock       sync.RWMutex               // For thread-safety
	lastSeq    uint64                     // Last sequence number assigned
	docs       map[string]*lolrusDoc      // Maps doc ID -> lolrusDoc
	designDocs map[string]lolrusDesignDoc // Maps design doc name -> lolrusDesignDoc
}

// A document stored in a lolrus's .docs map
type lolrusDoc struct {
	raw      []byte // Raw data content, or nil if deleted
	isJSON   bool   // Is the data a JSON document?
	sequence uint64 // Current sequence number assigned
}

// Creates a simple in-memory Bucket, suitable only for amusement purposes & testing.
func NewLolrusBucket(bucketName string) Bucket {
	return &lolrus{
		name:       bucketName,
		docs:       map[string]*lolrusDoc{},
		designDocs: map[string]lolrusDesignDoc{},
	}
}

// Generates the next sequence number to assign to a document update.
func (bucket *lolrus) _nextSequence() uint64 {
	bucket.lastSeq++
	return bucket.lastSeq
}

func (bucket *lolrus) GetName() string {
	return bucket.name // name is immutable so this needs no lock
}

//////// GET:

func (bucket *lolrus) GetRaw(k string) ([]byte, error) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	doc, found := bucket.docs[k]
	if !found || doc.raw == nil {
		return nil, MissingError{}
	}
	return doc.raw, nil
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

	if _, found := bucket.docs[k]; found {
		return false, nil
	}
	bucket.docs[k] = &lolrusDoc{raw: v, isJSON: isJSON, sequence: bucket._nextSequence()}
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

	doc := bucket.docs[k]
	if doc == nil {
		doc = &lolrusDoc{raw: v, isJSON: true}
		bucket.docs[k] = doc
	} else {
		doc.raw = v
		doc.isJSON = true
	}
	doc.sequence = bucket._nextSequence()
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

	doc, found := bucket.docs[k]
	if !found {
		return MissingError{}
	}
	doc.raw = nil
	doc.sequence = bucket._nextSequence()
	return nil
}

//////// UPDATE:

func (bucket *lolrus) Update(k string, exp int, callback UpdateFunc) error {
	var err error
	for {
		doc := bucket.getDoc(k)
		doc.raw, err = callback(doc.raw)
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

	docPtr := bucket.docs[k]
	if docPtr == nil {
		return lolrusDoc{}
	}
	return *docPtr
}

// Replaces a lolrusDoc as long as its sequence number hasn't changed yet. (Used by Update)
func (bucket *lolrus) updateDoc(k string, doc *lolrusDoc) bool {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	curDoc := bucket.docs[k]
	if curDoc != nil && curDoc.sequence != doc.sequence {
		return false
	}
	doc.sequence = bucket._nextSequence()
	doc.isJSON = (doc.raw != nil) // Doc is assumed to be JSON, unless deleted. (Used by Update)
	bucket.docs[k] = doc
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
	doc, found := bucket.docs[k]
	if found && doc.raw != nil {
		var err error
		counter, err = strconv.ParseUint(string(doc.raw), 10, 64)
		if err != nil {
			return 0, err
		}
	}
	if amt != 0 {
		counter += amt
		doc = &lolrusDoc{
			raw:      []byte(strconv.FormatUint(counter, 10)),
			isJSON:   false,
			sequence: bucket._nextSequence(),
		}
		bucket.docs[k] = doc
	}
	return counter, nil
}
