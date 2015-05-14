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
	"sync"
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/go.assert"
)

func setJSON(bucket sgbucket.Bucket, docid string, jsonDoc string) error {
	var obj interface{}
	err := json.Unmarshal([]byte(jsonDoc), &obj)
	if err != nil {
		return err
	}
	return bucket.Set(docid, 0, obj)
}

func TestDeleteThenAdd(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()

	var value interface{}
	assert.DeepEquals(t, bucket.Get("key", &value), sgbucket.MissingError{"key"})
	added, err := bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)
	assertNoError(t, bucket.Get("key", &value), "Get")
	assert.Equals(t, value, "value")
	assertNoError(t, bucket.Delete("key"), "Delete")
	assert.DeepEquals(t, bucket.Get("key", &value), sgbucket.MissingError{"key"})
	added, err = bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)
}

func TestIncr(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()
	count, err := bucket.Incr("count1", 1, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(100))

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(100))

	count, err = bucket.Incr("count1", 10, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(110))

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(110))

	count, err = bucket.Incr("count2", 0, 0, -1)
	assertTrue(t, err != nil, "Expected error from Incr")
}

// Spawns 1000 goroutines that 'simultaneously' use Incr to increment the same counter by 1.
func TestIncrAtomic(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()
	var waiters sync.WaitGroup
	numIncrements := 1000
	waiters.Add(numIncrements)
	for i := uint64(1); i <= uint64(numIncrements); i++ {
		numToAdd := i // lock down the value for the goroutine
		go func() {
			_, err := bucket.Incr("key", numToAdd, numToAdd, 0)
			assertNoError(t, err, "Incr")
			waiters.Add(-1)
		}()
	}
	waiters.Wait()
	value, err := bucket.Incr("key", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, int(value), numIncrements*(numIncrements+1)/2)
}

func TestAppend(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()

	err := bucket.Append("key", []byte(" World"))
	assert.DeepEquals(t, err, sgbucket.MissingError{"key"})

	err = bucket.SetRaw("key", 0, []byte("Hello"))
	assertNoError(t, err, "SetRaw")
	err = bucket.Append("key", []byte(" World"))
	assertNoError(t, err, "Append")
	value, err := bucket.GetRaw("key")
	assertNoError(t, err, "GetRaw")
	assert.DeepEquals(t, value, []byte("Hello World"))
}

// Create a simple view and run it on some documents
func TestView(t *testing.T) {
	ddoc := DesignDoc{Views: ViewMap{"view1": ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	bucket := NewBucket("buckit")
	defer bucket.Close()
	err := bucket.PutDDoc("docname", ddoc)
	assertNoError(t, err, "PutDDoc failed")

	var echo DesignDoc
	err = bucket.GetDDoc("docname", &echo)
	assert.DeepEquals(t, echo, ddoc)

	setJSON(bucket, "doc1", `{"key": "k1", "value": "v1"}`)
	setJSON(bucket, "doc2", `{"key": "k2", "value": "v2"}`)
	setJSON(bucket, "doc3", `{"key": 17, "value": ["v3"]}`)
	setJSON(bucket, "doc4", `{"key": [17, false], "value": null}`)
	setJSON(bucket, "doc5", `{"key": [17, true], "value": null}`)

	// raw docs and counters should not be indexed by views
	bucket.AddRaw("rawdoc", 0, []byte("this is raw data"))
	bucket.Incr("counter", 1, 0, 0)

	options := map[string]interface{}{"stale": false}
	result, err := bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 5)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc3", Key: 17.0, Value: []interface{}{"v3"}})
	assert.DeepEquals(t, result.Rows[1], &sgbucket.ViewRow{ID: "doc1", Key: "k1", Value: "v1"})
	assert.DeepEquals(t, result.Rows[2], &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2"})
	assert.DeepEquals(t, result.Rows[3], &sgbucket.ViewRow{ID: "doc4", Key: []interface{}{17.0, false}})
	assert.DeepEquals(t, result.Rows[4], &sgbucket.ViewRow{ID: "doc5", Key: []interface{}{17.0, true}})

	// Try a startkey:
	options["startkey"] = "k2"
	options["include_docs"] = true
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 3)
	var expectedDoc interface{} = map[string]interface{}{"key": "k2", "value": "v2"}
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Try an endkey:
	options["endkey"] = "k2"
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Try an endkey out of range:
	options["endkey"] = "k999"
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Try without inclusive_end:
	options["endkey"] = "k2"
	options["inclusive_end"] = false
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 0)

	// Try a single key:
	options = map[string]interface{}{"stale": false, "key": "k2", "include_docs": true}
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 1)
	assert.DeepEquals(t, result.Rows[0], &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Delete the design doc:
	assertNoError(t, bucket.DeleteDDoc("docname"), "DeleteDDoc")
	assert.DeepEquals(t, bucket.GetDDoc("docname", &echo), sgbucket.MissingError{"docname"})
}

func TestCheckDDoc(t *testing.T) {
	ddoc := DesignDoc{Views: ViewMap{"view1": ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	_, err := CheckDDoc(&ddoc)
	assertNoError(t, err, "CheckDDoc should have worked")

	ddoc = DesignDoc{Language: "go"}
	_, err = CheckDDoc(&ddoc)
	assertTrue(t, err != nil, "CheckDDoc should have rejected non-JS")
}

//////// HELPERS:

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Fatalf("%s", message)
	}
}
