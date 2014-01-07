package walrus

import (
	"github.com/couchbaselabs/go.assert"
	"testing"
)

func TestBackfill(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()
	bucket.Add("able", 0, "A")
	bucket.Add("baker", 0, "B")
	bucket.Add("charlie", 0, "C")

	feed, err := bucket.StartTapFeed(TapArguments{Backfill: 0, Dump: true})
	assertNoError(t, err, "StartTapFeed failed")
	assert.True(t, feed != nil)

	event := <-feed.Events()
	assert.Equals(t, event.Opcode, TapBeginBackfill)
	results := map[string]string{}
	for i := 0; i < 3; i++ {
		event := <-feed.Events()
		assert.Equals(t, event.Opcode, TapMutation)
		results[string(event.Key)] = string(event.Value)
	}
	assert.DeepEquals(t, results, map[string]string{
		"able": `"A"`, "baker": `"B"`, "charlie": `"C"`})

	event = <-feed.Events()
	assert.Equals(t, event.Opcode, TapEndBackfill)

	event, ok := <-feed.Events()
	assert.False(t, ok)
}

func TestMutations(t *testing.T) {
	bucket := NewBucket("buckit")
	defer bucket.Close()
	bucket.Add("able", 0, "A")
	bucket.Add("baker", 0, "B")
	bucket.Add("charlie", 0, "C")

	feed, err := bucket.StartTapFeed(TapArguments{Backfill: TapNoBackfill})
	assertNoError(t, err, "StartTapFeed failed")
	assert.True(t, feed != nil)
	defer feed.Close()

	bucket.Add("delta", 0, "D")
	bucket.Add("eskimo", 0, "E")

	go func() {
		bucket.Add("fahrvergnügen", 0, "F")
		bucket.Delete("eskimo")
	}()

	assert.DeepEquals(t, <-feed.Events(), TapEvent{Opcode: TapMutation, Key: []byte("delta"), Value: []byte(`"D"`), Sequence: 4})
	assert.DeepEquals(t, <-feed.Events(), TapEvent{Opcode: TapMutation, Key: []byte("eskimo"), Value: []byte(`"E"`), Sequence: 5})
	assert.DeepEquals(t, <-feed.Events(), TapEvent{Opcode: TapMutation, Key: []byte("fahrvergnügen"), Value: []byte(`"F"`), Sequence: 6})
	assert.DeepEquals(t, <-feed.Events(), TapEvent{Opcode: TapDeletion, Key: []byte("eskimo"), Sequence: 7})
}
