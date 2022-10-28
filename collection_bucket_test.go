package walrus

import (
	"errors"
	"fmt"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestMultiCollectionBucket(t *testing.T) {

	huddle := NewCollectionBucket("huddle1")
	c1 := huddle.NamedDataStore("scope1", "collection1")
	ok, err := c1.Add("doc1", 0, "c1_value")
	require.True(t, ok)
	require.NoError(t, err)
	c2 := huddle.NamedDataStore("scope1", "collection2")
	ok, err = c2.Add("doc1", 0, "c2_value")
	require.True(t, ok)
	require.NoError(t, err)

	var value interface{}
	_, err = c1.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)
	_, err = c2.Get("doc1", &value)
	assert.Equal(t, "c2_value", value)

	// reopen collection, verify retrieval
	c1copy := huddle.NamedDataStore("scope1", "collection1")
	_, err = c1copy.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)

	// drop collection
	err = huddle.DropDataStore("scope1", "collection1")
	require.NoError(t, err)

	// reopen collection, verify that previous data is not present
	newC1 := huddle.NamedDataStore("scope1", "collection1")
	_, err = newC1.Get("doc1", &value)
	require.Error(t, err)
	require.True(t, errors.As(err, &sgbucket.MissingError{}))
}

func TestDefaultCollection(t *testing.T) {

	huddle := NewCollectionBucket("huddle1")
	c1 := huddle.NamedDataStore("scope1", "collection1")
	ok, err := c1.Add("doc1", 0, "c1_value")
	require.True(t, ok)
	require.NoError(t, err)
	c2 := huddle.DefaultDataStore()
	ok, err = c2.Add("doc1", 0, "default_value")
	require.True(t, ok)
	require.NoError(t, err)

	var value interface{}
	_, err = c1.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "c1_value", value)
	_, err = c2.Get("doc1", &value)
	assert.Equal(t, "default_value", value)

	// reopen collection, verify retrieval
	c1copy := huddle.DefaultDataStore()
	_, err = c1copy.Get("doc1", &value)
	require.NoError(t, err)
	assert.Equal(t, "default_value", value)
}

func TestValidDataStoreName(t *testing.T) {

	validDataStoreNames := [][2]string{
		{"myScope", "myCollection"},
		{"ABCabc123_-%", "ABCabc123_-%"},
		{"_default", "myCollection"},
		{"_default", "_default"},
	}

	invalidDataStoreNames := [][2]string{
		{"a:1", "a:1"},
		{"_a", "b"},
		{"a", "_b"},
		{"%a", "b"},
		{"%a", "b"},
		{"a", "%b"},
		{"myScope", "_default"},
		{"_default", "a:1"},
	}

	for _, validPair := range validDataStoreNames {
		assert.True(t, isValidDataStoreName(validPair[0], validPair[1]))
	}
	for _, invalidPair := range invalidDataStoreNames {
		assert.False(t, isValidDataStoreName(invalidPair[0], invalidPair[1]))
	}
}

func TestCollectionMutations(t *testing.T) {

	huddle := NewCollectionBucket("huddle1")
	defer huddle.Close()

	collection1 := huddle.NamedDataStore("scope1", "collection1")
	collection2 := huddle.NamedDataStore("scope1", "collection2")
	numDocs := 50

	collectionID_1, _ := huddle.GetCollectionID("scope1", "collection1")
	collectionID_2, _ := huddle.GetCollectionID("scope1", "collection2")

	// Add n docs to two collections
	for i := 1; i <= numDocs; i++ {
		collection1.Add(fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
		collection2.Add(fmt.Sprintf("doc%d", i), 0, fmt.Sprintf("value%d", i))
	}

	var callbackMutex sync.Mutex
	var c1Count, c2Count int
	c1Keys := make(map[string]struct{})
	c2Keys := make(map[string]struct{})

	callback := func(event sgbucket.FeedEvent) bool {
		if event.Opcode != sgbucket.FeedOpMutation {
			return false
		}
		callbackMutex.Lock()
		defer callbackMutex.Unlock()
		if event.CollectionID == collectionID_1 {
			c1Count++
			key := string(event.Key)
			_, ok := c1Keys[key]
			assert.False(t, ok)
			c1Keys[key] = struct{}{}
		} else if event.CollectionID == collectionID_2 {
			c2Count++
			key := string(event.Key)
			_, ok := c2Keys[key]
			assert.False(t, ok)
			c2Keys[key] = struct{}{}
		}
		return true
	}

	args := sgbucket.FeedArguments{
		Scopes: map[string][]string{
			"scope1": {"collection1", "collection2"},
		},
	}
	err := huddle.StartDCPFeed(args, callback, nil)
	assertNoError(t, err, "StartTapFeed failed")

	// wait for mutation counts to reach expected
	expectedCountReached := false
	for i := 0; i < 100; i++ {
		if c1Count == numDocs && c2Count == numDocs {
			expectedCountReached = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, expectedCountReached)
	assert.Equal(t, len(c1Keys), numDocs)
	assert.Equal(t, len(c2Keys), numDocs)
}