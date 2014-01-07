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
	events  *queue
}

// Starts a TAP feed on a client connection. The events can be read from the returned channel.
// To stop receiving events, call Close() on the feed.
func (bucket *lolrus) StartTapFeed(args TapArguments) (TapFeed, error) {
	channel := make(chan TapEvent, 10)
	feed := &tapFeedImpl{
		bucket:  bucket,
		channel: channel,
		args:    args,
		events:  newQueue(),
	}

	if args.Backfill != TapNoBackfill {
		feed.events.push(&TapEvent{Opcode: TapBeginBackfill})
		bucket.enqueueBackfillEvents(args.Backfill, args.KeysOnly, feed.events)
		feed.events.push(&TapEvent{Opcode: TapEndBackfill})
	}

	if args.Dump {
		feed.events.push(nil) // push an eof
	} else {
		// Register the feed with the bucket for future notifications:
		bucket.lock.Lock()
		bucket.tapFeeds = append(bucket.tapFeeds, feed)
		bucket.lock.Unlock()
	}

	go feed.run()

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
	feed.events.close()
	feed.bucket = nil
	return nil
}

func (feed *tapFeedImpl) run() {
	defer close(feed.channel)
	for {
		event, _ := feed.events.pull().(*TapEvent)
		if event == nil {
			break
		}
		feed.channel <- *event
	}
}

func (bucket *lolrus) enqueueBackfillEvents(startSequence uint64, keysOnly bool, q *queue) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	for docid, doc := range bucket.Docs {
		if doc.Raw != nil && doc.Sequence >= startSequence {
			event := TapEvent{
				Opcode:   TapMutation,
				Key:      []byte(docid),
				Sequence: doc.Sequence,
			}
			if !keysOnly {
				event.Value = doc.Raw
			}
			q.push(&event)
		}
	}
}

// Caller must have the bucket's RLock, because this method iterates bucket.tapFeeds
func (bucket *lolrus) _postTapEvent(event TapEvent) {
	var eventNoValue TapEvent = event // copies the struct
	eventNoValue.Value = nil
	for _, feed := range bucket.tapFeeds {
		if feed != nil && feed.channel != nil {
			if feed.args.KeysOnly {
				feed.events.push(&eventNoValue)
			} else {
				feed.events.push(&event)
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
