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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setJSON(bucket sgbucket.DataStore, docid string, jsonDoc string) error {
	var obj interface{}
	err := json.Unmarshal([]byte(jsonDoc), &obj)
	if err != nil {
		return err
	}
	return bucket.Set(docid, 0, nil, obj)
}

func TestDeleteThenAdd(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()

	var value interface{}
	_, err := bucket.Get("key", &value)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)
	addToBucket(t, bucket, "key", 0, "value")
	_, err = bucket.Get("key", &value)
	assertNoError(t, err, "Get")
	assert.Equal(t, "value", value)
	assertNoError(t, bucket.Delete("key"), "Delete")
	_, err = bucket.Get("key", &value)
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)
	addToBucket(t, bucket, "key", 0, "value")
}

func TestIncr(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()
	count, err := bucket.Incr("count1", 1, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equal(t, uint64(100), count)

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equal(t, uint64(100), count)

	count, err = bucket.Incr("count1", 10, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equal(t, uint64(110), count)

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equal(t, uint64(110), count)
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
	assert.Equal(t, numIncrements*(numIncrements+1)/2, int(value))
}

func TestAppend(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()

	err := bucket.Append("key", []byte(" World"))
	assert.Equal(t, sgbucket.MissingError{Key: "key"}, err)

	err = bucket.SetRaw("key", 0, nil, []byte("Hello"))
	assertNoError(t, err, "SetRaw")
	err = bucket.Append("key", []byte(" World"))
	assertNoError(t, err, "Append")
	value, _, err := bucket.GetRaw("key")
	assertNoError(t, err, "GetRaw")
	assert.Equal(t, []byte("Hello World"), value)
}

// Create a simple view and run it on some documents
func TestView(t *testing.T) {
	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	bucket := NewBucket("buckit")
	defer bucket.Close()
	err := bucket.PutDDoc("docname", &ddoc)
	assertNoError(t, err, "PutDDoc failed")

	var echo sgbucket.DesignDoc
	echo, err = bucket.GetDDoc("docname")
	assert.Equal(t, ddoc, echo)

	require.NoError(t, setJSON(bucket, "doc1", `{"key": "k1", "value": "v1"}`))
	require.NoError(t, setJSON(bucket, "doc2", `{"key": "k2", "value": "v2"}`))
	require.NoError(t, setJSON(bucket, "doc3", `{"key": 17, "value": ["v3"]}`))
	require.NoError(t, setJSON(bucket, "doc4", `{"key": [17, false], "value": null}`))
	require.NoError(t, setJSON(bucket, "doc5", `{"key": [17, true], "value": null}`))

	// raw docs and counters should not be indexed by views
	_, err = bucket.AddRaw("rawdoc", 0, []byte("this is raw data"))
	require.NoError(t, err)
	_, err = bucket.Incr("counter", 1, 0, 0)
	require.NoError(t, err)

	options := map[string]interface{}{"stale": false}
	result, err := bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equal(t, 5, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc3", Key: 17.0, Value: []interface{}{"v3"}}, result.Rows[0])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc1", Key: "k1", Value: "v1"}, result.Rows[1])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2"}, result.Rows[2])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc4", Key: []interface{}{17.0, false}}, result.Rows[3])
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc5", Key: []interface{}{17.0, true}}, result.Rows[4])

	// Try a startkey:
	options["startkey"] = "k2"
	options["include_docs"] = true
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equal(t, 3, result.TotalRows)
	var expectedDoc interface{} = map[string]interface{}{"key": "k2", "value": "v2"}
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Try an endkey:
	options["endkey"] = "k2"
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equal(t, 1, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Try an endkey out of range:
	options["endkey"] = "k999"
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equal(t, 1, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Try without inclusive_end:
	options["endkey"] = "k2"
	options["inclusive_end"] = false
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equal(t, 0, result.TotalRows)

	// Try a single key:
	options = map[string]interface{}{"stale": false, "key": "k2", "include_docs": true}
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equal(t, 1, result.TotalRows)
	assert.Equal(t, &sgbucket.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc}, result.Rows[0])

	// Delete the design doc:
	assertNoError(t, bucket.DeleteDDoc("docname"), "DeleteDDoc")
	_, getErr := bucket.GetDDoc("docname")
	assert.True(t, errors.Is(getErr, sgbucket.MissingError{Key: "docname"}))
}

func TestCheckDDoc(t *testing.T) {
	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	assertNoError(t, CheckDDoc(&ddoc), "CheckDDoc should have worked")

	ddoc = sgbucket.DesignDoc{Language: "go"}
	err := CheckDDoc(&ddoc)
	assertTrue(t, err != nil, "CheckDDoc should have rejected non-JS")
}

func TestGetDDocs(t *testing.T) {
	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	bucket := NewBucket("buckit")
	defer bucket.Close()
	err := bucket.PutDDoc("docname", &ddoc)
	assertNoError(t, err, "PutDDoc docnamefailed")
	err = bucket.PutDDoc("docname2", &ddoc)
	assertNoError(t, err, "PutDDoc docname2failed")

	ddocs, getErr := bucket.GetDDocs()
	assertNoError(t, getErr, "GetDDocs failed")
	assert.Equal(t, len(ddocs), 2)
}

func TestGetBulkRaw(t *testing.T) {

	bucket := NewBucket("buckit")
	defer bucket.Close()

	// add two keys
	err := bucket.SetRaw("key1", 0, nil, []byte("value1"))
	assertNoError(t, err, "SetRaw")
	err = bucket.SetRaw("key2", 0, nil, []byte("value2"))
	assertNoError(t, err, "SetRaw")

	// call bulk get raw
	resultRaw, err := bucket.GetBulkRaw([]string{"key1", "key2"})
	assertNoError(t, err, "GetBulkRaw")
	assert.Equal(t, 2, len(resultRaw))
	assert.Equal(t, []byte("value1"), resultRaw["key1"])
	assert.Equal(t, []byte("value2"), resultRaw["key2"])

}

func TestGets(t *testing.T) {

	bucket := NewBucket("buckit")

	defer bucket.Close()

	// Gets (JSON)
	addToBucket(t, bucket, "key", 0, "value")

	var value interface{}
	cas, err := bucket.Get("key", &value)
	assertNoError(t, err, "Gets")
	assert.True(t, cas > 0)
	assert.Equal(t, "value", value)

	// GetsRaw
	err = bucket.SetRaw("keyraw", 0, nil, []byte("Hello"))
	assertNoError(t, err, "SetRaw")

	value, cas, err = bucket.GetRaw("keyraw")
	assertNoError(t, err, "GetsRaw")
	assert.True(t, cas > 0)
	assert.Equal(t, []byte("Hello"), value)

}

func TestWriteSubDoc(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()
	var expectedCas uint64
	expectedCas = 2

	rawJson := []byte(`{
        "walrus":{
            "foo":"lol",
            "bar":"baz"}
        }`)

	addToBucket(t, bucket, "key", 0, rawJson)

	var fullDoc map[string]interface{}
	cas, err := bucket.Get("key", &fullDoc)
	assert.NoError(t, err)

	// update json
	rawJson = []byte(`{
        "walrus":{
            "foo":"lol1",
            "bar":"baz"}
        }`)
	// test update using incorrect cas value
	_, err = bucket.WriteSubDoc("key", "walrus", 10, rawJson)
	assert.Error(t, err)

	// test update using correct cas value
	cas, err = bucket.WriteSubDoc("key", "walrus", cas, rawJson)
	assert.Equal(t, expectedCas, cas)

	// test update using 0 cas value
	cas, err = bucket.WriteSubDoc("key", "walrus", 0, rawJson)
	expectedCas = 3
	assert.Equal(t, expectedCas, cas)
}

func TestWriteCas(t *testing.T) {

	bucket := NewBucket("buckit")

	defer bucket.Close()

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	err := json.Unmarshal([]byte(`{"value":"value1"}`), &obj)
	cas, err := bucket.WriteCas("key1", 0, 0, 0, obj, 0)
	assertNoError(t, err, "WriteCas")
	assertTrue(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	err = json.Unmarshal([]byte(`{"value":"value2"}`), &obj)
	newCas, err := bucket.WriteCas("key1", 0, 0, 0, obj, 0)
	assertTrue(t, err != nil, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	err = json.Unmarshal([]byte(`{"value":"value2"}`), &obj)
	newCas, err = bucket.WriteCas("key1", 0, 0, cas, obj, 0)
	assertTrue(t, err == nil, "Valid cas should not have returned error.")
	assertTrue(t, cas > 0, "Cas value should be greater than zero")
	assertTrue(t, cas != newCas, "Cas value should change on successful update")
	var result interface{}
	getCas, err := bucket.Get("key1", &result)
	assert.Equal(t, obj, result)
	assert.Equal(t, newCas, getCas)

	// Update document with obsolete case value
	err = json.Unmarshal([]byte(`{"value":"value3"}`), &obj)
	newCas, err = bucket.WriteCas("key1", 0, 0, cas, obj, 0)
	assertTrue(t, err != nil, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Add with WriteCas - raw docs
	// Insert
	cas, err = bucket.WriteCas("keyraw1", 0, 0, 0, []byte("value1"), sgbucket.Raw)
	assertNoError(t, err, "WriteCas")
	assertTrue(t, cas > 0, "Cas value should be greater than zero")

	// Update document with wrong (zero) cas value
	newCas, err = bucket.WriteCas("keyraw1", 0, 0, 0, []byte("value2"), sgbucket.Raw)
	assertTrue(t, err != nil, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Update document with correct cas value
	newCas, err = bucket.WriteCas("keyraw1", 0, 0, cas, []byte("value2"), sgbucket.Raw)
	assertTrue(t, err == nil, "Valid cas should not have returned error.")
	assertTrue(t, cas > 0, "Cas value should be greater than zero")
	assertTrue(t, cas != newCas, "Cas value should change on successful update")
	value, getCas, err := bucket.GetRaw("keyraw1")
	assert.Equal(t, []byte("value2"), value)
	assert.Equal(t, newCas, getCas)

	// Update document with obsolete cas value
	newCas, err = bucket.WriteCas("keyraw1", 0, 0, cas, []byte("value3"), sgbucket.Raw)
	assertTrue(t, err != nil, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Delete document, attempt to recreate w/ cas set to 0
	err = bucket.Delete("keyraw1")
	assertTrue(t, err == nil, "Delete failed")
	newCas, err = bucket.WriteCas("keyraw1", 0, 0, 0, []byte("resurrectValue"), sgbucket.Raw)
	assertTrue(t, err == nil, "Recreate with cas=0 should succeed.")
	assertTrue(t, cas > 0, "Cas value should be greater than zero")
	value, getCas, err = bucket.GetRaw("keyraw1")
	assert.Equal(t, []byte("resurrectValue"), value)
	assert.Equal(t, newCas, getCas)

}

func TestRemove(t *testing.T) {

	bucket := NewBucket("buckit")

	defer bucket.Close()

	// Add with WriteCas - JSON docs
	// Insert
	var obj interface{}
	err := json.Unmarshal([]byte(`{"value":"value1"}`), &obj)
	cas, err := bucket.WriteCas("key1", 0, 0, 0, obj, 0)
	assertNoError(t, err, "WriteCas")
	assertTrue(t, cas > 0, "Cas value should be greater than zero")

	// Update document with correct cas value
	err = json.Unmarshal([]byte(`{"value":"value2"}`), &obj)
	newCas, err := bucket.WriteCas("key1", 0, 0, cas, obj, 0)
	assertTrue(t, err == nil, "Valid cas should not have returned error.")
	assertTrue(t, cas > 0, "Cas value should be greater than zero")
	assertTrue(t, cas != newCas, "Cas value should change on successful update")
	var result interface{}
	getCas, err := bucket.Get("key1", &result)
	assert.Equal(t, obj, result)
	assert.Equal(t, newCas, getCas)

	// Remove document with incorrect cas value
	newCas, err = bucket.Remove("key1", cas)
	assertTrue(t, err != nil, "Invalid cas should have returned error.")
	assert.Equal(t, uint64(0), newCas)

	// Remove document with correct cas value
	newCas, err = bucket.Remove("key1", getCas)
	assertTrue(t, err == nil, "Valid cas should not have returned error on remove.")
	assertTrue(t, newCas != uint64(0), "Remove should return non-zero cas")

}

// Test read and write of json as []byte
func TestNonRawBytes(t *testing.T) {

	bucket := NewBucket("byteCheckBucket")

	defer bucket.Close()

	byteBody := []byte(`{"value":"value1"}`)

	// Add with WriteCas - JSON doc as []byte and *[]byte
	cas, err := bucket.WriteCas("writeCas1", 0, 0, 0, byteBody, 0)
	assertNoError(t, err, "WriteCas []byte")
	cas, err = bucket.WriteCas("writeCas2", 0, 0, 0, &byteBody, 0)
	assertNoError(t, err, "WriteCas *[]byte")

	// Add with Add - JSON doc as []byte and *[]byte
	addToBucket(t, bucket, "add1", 0, byteBody)
	addToBucket(t, bucket, "add2", 0, &byteBody)

	// Set - JSON doc as []byte
	// Set - JSON doc as *[]byte
	// Add with Add - JSON doc as []byte and *[]byte
	err = bucket.Set("set1", 0, nil, byteBody)
	assertNoError(t, err, "Set []byte")
	err = bucket.Set("set2", 0, nil, &byteBody)
	assertNoError(t, err, "Set *[]byte")

	keySet := []string{"writeCas1", "writeCas2", "add1", "add2", "set1", "set2"}
	for _, key := range keySet {
		// Verify retrieval as map[string]interface{}
		var result map[string]interface{}
		cas, err = bucket.Get(key, &result)
		assertNoError(t, err, fmt.Sprintf("Error for Get %s", key))
		assertTrue(t, cas > 0, fmt.Sprintf("CAS is zero for key: %s", key))
		assertTrue(t, result != nil, fmt.Sprintf("result is nil for key: %s", key))
		if result != nil {
			assert.Equal(t, "value1", result["value"])
		}

		// Verify retrieval as *[]byte
		var rawResult []byte
		cas, err = bucket.Get(key, &rawResult)
		assertNoError(t, err, fmt.Sprintf("Error for Get %s", key))
		assertTrue(t, cas > 0, fmt.Sprintf("CAS is zero for key: %s", key))
		assertTrue(t, result != nil, fmt.Sprintf("result is nil for key: %s", key))
		if result != nil {
			matching := bytes.Compare(rawResult, byteBody)
			assert.Equal(t, 0, matching)
		}
	}

	// Verify values are stored as JSON and can be retrieved via view
	ddoc := sgbucket.DesignDoc{Views: sgbucket.ViewMap{"view1": sgbucket.ViewDef{Map: `function(doc){if (doc.value) emit(doc.key,doc.value)}`}}}
	err = bucket.PutDDoc("docname", &ddoc)
	assertNoError(t, err, "PutDDoc failed")

	options := map[string]interface{}{"stale": false}
	result, err := bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equal(t, len(keySet), result.TotalRows)
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

func addToBucket(t *testing.T, bucket *WalrusBucket, key string, exp uint32, value interface{}) {
	added, err := bucket.Add(key, exp, value)
	require.NoError(t, err)
	require.True(t, added)
}
