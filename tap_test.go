package walrus

import (
	"context"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackfill(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close(context.TODO())

	addToBucket(t, bucket, "able", 0, "A")
	addToBucket(t, bucket, "baker", 0, "B")
	addToBucket(t, bucket, "charlie", 0, "C")

	feed, err := bucket.StartTapFeed(sgbucket.FeedArguments{Backfill: 0, Dump: true}, nil)
	assertNoError(t, err, "StartTapFeed failed")
	assert.True(t, feed != nil)

	event := <-feed.Events()
	assert.Equal(t, sgbucket.FeedOpBeginBackfill, event.Opcode)
	results := map[string]string{}
	for i := 0; i < 3; i++ {
		event := <-feed.Events()
		assert.Equal(t, sgbucket.FeedOpMutation, event.Opcode)
		results[string(event.Key)] = string(event.Value)
	}
	assert.Equal(t, map[string]string{
		"able": `"A"`, "baker": `"B"`, "charlie": `"C"`}, results)

	event = <-feed.Events()
	assert.Equal(t, sgbucket.FeedOpEndBackfill, event.Opcode)

	event, ok := <-feed.Events()
	assert.False(t, ok)
}

func TestMutations(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close(context.TODO())

	addToBucket(t, bucket, "able", 0, "A")
	addToBucket(t, bucket, "baker", 0, "B")
	addToBucket(t, bucket, "charlie", 0, "C")

	feed, err := bucket.StartTapFeed(sgbucket.FeedArguments{Backfill: sgbucket.FeedNoBackfill}, nil)
	assertNoError(t, err, "StartTapFeed failed")
	assert.True(t, feed != nil)
	defer feed.Close()

	addToBucket(t, bucket, "delta", 0, "D")
	addToBucket(t, bucket, "eskimo", 0, "E")

	go func() {
		addToBucket(t, bucket, "fahrvergnügen", 0, "F")
		err = bucket.Delete("eskimo")
		require.NoError(t, err)
	}()

	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("delta"), Value: []byte(`"D"`), Cas: 4}, <-feed.Events())
	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("eskimo"), Value: []byte(`"E"`), Cas: 5}, <-feed.Events())
	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpMutation, Key: []byte("fahrvergnügen"), Value: []byte(`"F"`), Cas: 6}, <-feed.Events())
	assert.Equal(t, sgbucket.FeedEvent{Opcode: sgbucket.FeedOpDeletion, Key: []byte("eskimo"), Cas: 7}, <-feed.Events())
}
