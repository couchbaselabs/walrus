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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
)

func TestSave(t *testing.T) {
	tmpdir := t.TempDir()
	kTestPath := filepath.Join(tmpdir, "/walrus_test_save.walrus")

	ctx := context.Background()
	bucket := NewBucket("persisty")
	defer bucket.Close(ctx)
	bucket.Add("key1", 0, `{"value": 1}`)
	bucket.AddRaw("key2", 0, []byte("value2"))
	bucket.path = kTestPath
	err := bucket._save()
	assertNoError(t, err, "couldn't save")

	bucket2, err := load(kTestPath)
	assertNoError(t, err, "couldn't load")
	assert.Equal(t, bucket.walrusData, bucket2.walrusData)

	bucket.Set("key2", 0, nil, []byte("NEWVALUE2"))

	err = bucket._save()
	assertNoError(t, err, "couldn't re-save")

	bucket2, err = load(kTestPath)
	defer bucket2.Close(ctx)
	assertNoError(t, err, "couldn't re-load")
	assert.Equal(t, bucket.walrusData, bucket2.walrusData)
}

func TestLoadOrNew(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	kTestPath := filepath.Join(tmpdir, "/walrus_test_save.walrus")

	bucket, err := load(kTestPath)
	assertTrue(t, os.IsNotExist(err), "Unexpected error")

	bucket, err = loadOrNew(kTestPath, "walrus_test_loadOrNew")
	assertNoError(t, err, "loadOrNew failed")
	assert.Equal(t, 0, len(bucket.Docs))

	bucket.Add("key9", 0, `{"value": 9}`)
	bucket.Close(ctx)

	bucket, err = loadOrNew(kTestPath, "walrus_test_loadOrNew")
	assertNoError(t, err, "loadOrNew #2 failed")
	assert.Equal(t, bucket.walrusData, bucket.walrusData)
	bucket.Close(ctx)
}

func TestBucketURLToDir(t *testing.T) {
	assert.Equal(t, "", bucketURLToDir(""))
	assert.Equal(t, "", bucketURLToDir("foo"))
	assert.Equal(t, "/tmp", bucketURLToDir("/tmp"))
	assert.Equal(t, "./data", bucketURLToDir("./data"))
	assert.Equal(t, "", bucketURLToDir("walrus:"))
	assert.Equal(t, "", bucketURLToDir("walrus://"))
	assert.Equal(t, "", bucketURLToDir("walrus:///"))
	assert.Equal(t, "/tmp", bucketURLToDir("walrus:///tmp"))
	assert.Equal(t, "data/walrus", bucketURLToDir("walrus:data/walrus"))
	assert.Equal(t, "/tmp/walrus", bucketURLToDir("walrus:/tmp/walrus"))
	assert.Equal(t, "/tmp", bucketURLToDir("file:///tmp"))
	assert.Equal(t, "data/walrus", bucketURLToDir("file:data/walrus"))
	assert.Equal(t, "/tmp/walrus", bucketURLToDir("file:/tmp/walrus"))
	assert.Equal(t, "", bucketURLToDir("http://example.com/tmp"))
}

func TestNewPersistentBucket(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	bucket, err := GetBucket(fmt.Sprintf("walrus:%s", tmpdir), "pool", "buckit")
	assertNoError(t, err, "NewPersistentBucket failed")
	assert.Equal(t, filepath.Join(tmpdir, "pool-buckit.walrus"), bucket.path)
	bucket.Close(ctx)

	bucket, err = GetBucket("./temp", "default", "buckit")
	assertNoError(t, err, "NewPersistentBucket failed")
	assert.Equal(t, filepath.Join("temp", "buckit.walrus"), bucket.path)
	bucket.Close(ctx)
}

func TestWriteWithPersist(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	bucket, err := GetBucket(fmt.Sprintf("walrus:%s", tmpdir), "pool", "buckit")
	assertNoError(t, err, "NewPersistentBucket failed")

	assertNoError(t, bucket.Write("key1", 0, 0, []byte("value1"), sgbucket.Raw|sgbucket.Persist), "Write failed")

	// Load the file into a new bucket to make sure the value got saved to disk:
	bucket2, err := load(bucket.path)
	value, _, err := bucket2.GetRaw("key1")
	assertNoError(t, err, "Get failed")
	assert.Equal(t, "value1", string(value))
	bucket2.Close(ctx)
	bucket.Close(ctx)
}
