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
	"os"
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/go.assert"
)

const kTestPath = "/tmp/lolrus_test_save.walrus"

func TestSave(t *testing.T) {
	os.Remove(kTestPath)

	bucket := NewBucket("persisty").(*lolrus)
	defer bucket.Close()
	bucket.Add("key1", 0, `{"value": 1}`)
	bucket.AddRaw("key2", 0, []byte("value2"))
	bucket.path = kTestPath
	err := bucket._save()
	assertNoError(t, err, "couldn't save")

	bucket2, err := load(kTestPath)
	assertNoError(t, err, "couldn't load")
	assert.DeepEquals(t, bucket2.lolrusData, bucket.lolrusData)

	bucket.Set("key2", 0, []byte("NEWVALUE2"))

	err = bucket._save()
	assertNoError(t, err, "couldn't re-save")

	bucket2, err = load(kTestPath)
	defer bucket2.Close()
	assertNoError(t, err, "couldn't re-load")
	assert.DeepEquals(t, bucket2.lolrusData, bucket.lolrusData)
}

func TestLoadOrNew(t *testing.T) {
	os.Remove(kTestPath)
	bucket, err := load(kTestPath)
	assertTrue(t, os.IsNotExist(err), "Unexpected error")

	bucket, err = loadOrNew(kTestPath, "lolrus_test_loadOrNew")
	assertNoError(t, err, "loadOrNew failed")
	assert.Equals(t, len(bucket.Docs), 0)

	bucket.Add("key9", 0, `{"value": 9}`)
	bucket.Close()

	bucket, err = loadOrNew(kTestPath, "lolrus_test_loadOrNew")
	assertNoError(t, err, "loadOrNew #2 failed")
	assert.DeepEquals(t, bucket.lolrusData, bucket.lolrusData)
	bucket.Close()
}

func TestBucketURLToDir(t *testing.T) {
	assert.Equals(t, bucketURLToDir(""), "")
	assert.Equals(t, bucketURLToDir("foo"), "")
	assert.Equals(t, bucketURLToDir("/tmp"), "/tmp")
	assert.Equals(t, bucketURLToDir("./data"), "./data")
	assert.Equals(t, bucketURLToDir("walrus:"), "")
	assert.Equals(t, bucketURLToDir("walrus://"), "")
	assert.Equals(t, bucketURLToDir("walrus:///"), "")
	assert.Equals(t, bucketURLToDir("walrus:///tmp"), "/tmp")
	assert.Equals(t, bucketURLToDir("walrus:data/walrus"), "data/walrus")
	assert.Equals(t, bucketURLToDir("walrus:/tmp/walrus"), "/tmp/walrus")
	assert.Equals(t, bucketURLToDir("file:///tmp"), "/tmp")
	assert.Equals(t, bucketURLToDir("file:data/walrus"), "data/walrus")
	assert.Equals(t, bucketURLToDir("file:/tmp/walrus"), "/tmp/walrus")
	assert.Equals(t, bucketURLToDir("http://example.com/tmp"), "")
}

func TestNewPersistentBucket(t *testing.T) {
	os.Remove("/tmp/pool-buckit.walrus")
	bucket, err := GetBucket("walrus:/tmp", "pool", "buckit")
	assertNoError(t, err, "NewPersistentBucket failed")
	assert.Equals(t, bucket.(*lolrus).path, "/tmp/pool-buckit.walrus")
	bucket.(*lolrus).Close()

	bucket, err = GetBucket("./temp", "default", "buckit")
	assertNoError(t, err, "NewPersistentBucket failed")
	assert.Equals(t, bucket.(*lolrus).path, "temp/buckit.walrus")
	bucket.(*lolrus).Close()
}

func TestWriteWithPersist(t *testing.T) {
	os.Remove("/tmp/pool-buckit.walrus")
	bucket, err := GetBucket("walrus:/tmp", "pool", "buckit")
	assertNoError(t, err, "NewPersistentBucket failed")

	assertNoError(t, bucket.Write("key1", 0, 0, []byte("value1"), sgbucket.Raw|sgbucket.Persist), "Write failed")

	// Load the file into a new bucket to make sure the value got saved to disk:
	bucket2, err := load(bucket.(*lolrus).path)
	value, err := bucket2.GetRaw("key1")
	assertNoError(t, err, "Get failed")
	assert.Equals(t, string(value), "value1")
	bucket2.Close()
	bucket.Close()
}
