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
	tapEndStream
)

// A TAP notification of an operation on the server.
type TapEvent struct {
	Opcode     TapOpcode // Type of event
	Flags      uint32    // Item flags
	Expiry     uint32    // Item expiration time
	Key, Value []byte    // Item key/value
}

// A Tap feed. Events from the bucket can be read from the channel 'C'.
// Remember to call Close() on it when you're done, unless its channel has closed itself already.
type TapFeed struct {
	C      <-chan TapEvent
	writer chan<- TapEvent
	closer chan bool
	args   TapArguments
}

// Parameters for requesting a TAP feed. Call DefaultTapArguments to get a default one.
type TapArguments struct {
	Backfill uint64 // Timestamp of oldest item to send. Use TapNoBackfill to suppress all past items.
	Dump     bool   // If set, server will disconnect after sending existing items.
	KeysOnly bool   // If true, client doesn't want values so server shouldn't send them.
}

// Value for TapArguments.Backfill denoting that no past events at all should be sent.
const TapNoBackfill = math.MaxUint64

// Starts a TAP feed on a client connection. The events can be read from the returned channel.
// To stop receiving events, call Close() on the feed.
func (bucket *lolrus) StartTapFeed(args TapArguments) (*TapFeed, error) {
	channel := make(chan TapEvent, 10)
	feed := &TapFeed{
		C:      channel,
		writer: channel,
		closer: make(chan bool),
		args:   args,
	}

	go func() {
		// Send the backfill from a goroutine, to avoid deadlock
		if args.Backfill != TapNoBackfill {
			bucket.backfill(feed)
		}
		if args.Dump {
			close(feed.writer)
		} else {
			// Now that the backfill (if any) is over, listen for future events:
			bucket.lock.RLock()
			defer bucket.lock.RUnlock()

			bucket.tapFeeds = append(bucket.tapFeeds, feed)
		}
	}()

	return feed, nil
}

// Closes a TapFeed. Call this if you stop using a TapFeed before its channel ends.
func (feed *TapFeed) Close() {
	close(feed.closer)
	feed.closer = nil
	feed.writer = nil
	feed.C = nil
}

func (bucket *lolrus) backfill(feed *TapFeed) {
	feed.writer <- TapEvent{Opcode: TapBeginBackfill}
	for _, event := range bucket.copyBackfillEvents(feed.args.Backfill) {
		feed.writer <- event
	}
	feed.writer <- TapEvent{Opcode: TapEndBackfill}
}

func (bucket *lolrus) copyBackfillEvents(startSequence uint64) []TapEvent {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	events := make([]TapEvent, 0, len(bucket.Docs))
	for docid, doc := range bucket.Docs {
		if doc.Raw != nil && doc.Sequence >= startSequence {
			events = append(events, TapEvent{
				Opcode: TapMutation,
				Key:    []byte(docid),
				Value:  doc.Raw,
			})
		}
	}
	return events
}

func (bucket *lolrus) postTapEvent(event TapEvent) {
	for _, feed := range bucket.tapFeeds {
		if feed.writer != nil {
			feed.writer <- event
		}
	}
}

func (bucket *lolrus) postTapMutationEvent(key string, value []byte) {
	bucket.postTapEvent(TapEvent{
		Opcode: TapMutation,
		Key:    []byte(key),
		Value:  value,
	})
}

func (bucket *lolrus) postTapDeletionEvent(key string) {
	bucket.postTapEvent(TapEvent{
		Opcode: TapDeletion,
		Key:    []byte(key),
	})
}
