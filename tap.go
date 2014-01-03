package walrus

import (
	"math"
)

// Tap operation type (found in TapEvent)
type TapOpcode uint8

const (
	TapBeginBackfill = TapOpcode(iota)
	TapEndBackfill
	TapMutation
	TapDeletion
	TapCheckpointStart
	TapCheckpointEnd
)

// A TAP notification of an operation on the server.
type TapEvent struct {
	Opcode     TapOpcode // Type of event
	Flags      uint32    // Item flags
	Expiry     uint32    // Item expiration time
	Key, Value []byte    // Item key/value
	Sequence   uint64    // Sequence identifier of document
}

// A Tap feed. Events from the bucket can be read from the channel returned by Events().
// Remember to call Close() on it when you're done, unless its channel has closed itself already.
type TapFeed interface {
	Events() <-chan TapEvent
	Close() error
}

// Parameters for requesting a TAP feed. Call DefaultTapArguments to get a default one.
type TapArguments struct {
	Backfill uint64 // Timestamp of oldest item to send. Use TapNoBackfill to suppress all past items.
	Dump     bool   // If set, server will disconnect after sending existing items.
	KeysOnly bool   // If true, client doesn't want values so server shouldn't send them.
}

// Value for TapArguments.Backfill denoting that no past events at all should be sent.
const TapNoBackfill = math.MaxUint64

type tapFeedImpl struct {
	bucket  *lolrus
	channel chan TapEvent
	args    TapArguments
}

// Starts a TAP feed on a client connection. The events can be read from the returned channel.
// To stop receiving events, call Close() on the feed.
func (bucket *lolrus) StartTapFeed(args TapArguments) (TapFeed, error) {
	channel := make(chan TapEvent, 10)
	feed := &tapFeedImpl{
		bucket:  bucket,
		channel: channel,
		args:    args,
	}

	go func() {
		// Send the backfill from a goroutine, to avoid deadlock
		if args.Backfill != TapNoBackfill {
			bucket.backfill(feed)
		}
		if args.Dump {
			close(feed.channel)
		} else {
			// Now that the backfill (if any) is over, listen for future events:
			bucket.lock.Lock()
			defer bucket.lock.Unlock()

			if feed.bucket != nil {
				bucket.tapFeeds = append(bucket.tapFeeds, feed)
			}
		}
	}()

	return feed, nil
}

func (feed *tapFeedImpl) Events() <-chan TapEvent {
	return feed.channel
}

// Closes a TapFeed. Call this if you stop using a TapFeed before its channel ends.
func (feed *tapFeedImpl) Close() error {
	feed.bucket.lock.Lock()
	defer feed.bucket.lock.Unlock()

	for i, afeed := range feed.bucket.tapFeeds {
		if afeed == feed {
			feed.bucket.tapFeeds[i] = nil
		}
	}
	close(feed.channel)
	feed.bucket = nil
	return nil
}

func (bucket *lolrus) backfill(feed *tapFeedImpl) {
	feed.channel <- TapEvent{Opcode: TapBeginBackfill}
	for _, event := range bucket.copyBackfillEvents(feed.args.Backfill) {
		feed.channel <- event
	}
	feed.channel <- TapEvent{Opcode: TapEndBackfill}
}

func (bucket *lolrus) copyBackfillEvents(startSequence uint64) []TapEvent {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	events := make([]TapEvent, 0, len(bucket.Docs))
	for docid, doc := range bucket.Docs {
		if doc.Raw != nil && doc.Sequence >= startSequence {
			events = append(events, TapEvent{
				Opcode:   TapMutation,
				Key:      []byte(docid),
				Value:    doc.Raw,
				Sequence: doc.Sequence,
			})
		}
	}
	return events
}

// Caller must have the bucket's RLock, because this method iterates bucket.tapFeeds
func (bucket *lolrus) _postTapEvent(event TapEvent) {
	eventNoValue := event
	eventNoValue.Value = nil
	for _, feed := range bucket.tapFeeds {
		if feed != nil && feed.channel != nil {
			if feed.args.KeysOnly {
				feed.channel <- eventNoValue
			} else {
				feed.channel <- event
			}
		}
	}
}

func (bucket *lolrus) _postTapMutationEvent(key string, value []byte, seq uint64) {
	bucket._postTapEvent(TapEvent{
		Opcode:   TapMutation,
		Key:      []byte(key),
		Value:    copySlice(value),
		Sequence: seq,
	})
}

func (bucket *lolrus) _postTapDeletionEvent(key string, seq uint64) {
	bucket._postTapEvent(TapEvent{
		Opcode:   TapDeletion,
		Key:      []byte(key),
		Sequence: seq,
	})
}
