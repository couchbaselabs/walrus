package walrus

import (
	"expvar"
	"fmt"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

type tapFeedImpl struct {
	bucket  *WalrusBucket
	channel chan sgbucket.FeedEvent
	args    sgbucket.FeedArguments
	events  *queue
}

// Starts a TAP feed on a client connection. The events can be read from the returned channel.
// To stop receiving events, call Close() on the feed.
func (bucket *WalrusBucket) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
	channel := make(chan sgbucket.FeedEvent, 10)
	feed := &tapFeedImpl{
		bucket:  bucket,
		channel: channel,
		args:    args,
		events:  newQueue(),
	}

	if args.Backfill != sgbucket.FeedNoBackfill {
		feed.events.push(&sgbucket.FeedEvent{Opcode: sgbucket.FeedOpBeginBackfill})
		bucket.enqueueBackfillEvents(args.Backfill, args.KeysOnly, feed.events)
		feed.events.push(&sgbucket.FeedEvent{Opcode: sgbucket.FeedOpEndBackfill})
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

// Until a full DCP implementation is available, walrus wraps tap feed to invoke callback
func (bucket *WalrusBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {

	tapFeed, err := bucket.StartTapFeed(args, dbStats)
	if err != nil {
		return err
	}

	go func() {
		fmt.Printf("StartDCPFeed before event loop\n")
	eventLoop:
		for {
			select {
			case event := <-tapFeed.Events():
				event.TimeReceived = time.Now()
				fmt.Printf("StartDCPFeed event loop for key %s\n", event.Key)
				callback(event)
			case <-args.Terminator:
				break eventLoop
			}
		}
		fmt.Printf("StartDCPFeed after event loop\n")
		if args.DoneChan != nil {
			fmt.Printf("StartDCPFeed before DoneChan close\n")
			close(args.DoneChan)
			fmt.Printf("StartDCPFeed after DoneChan close\n")
		} else {
			fmt.Printf("DoneChan was nil\n")
		}
	}()
	return nil
}

func (feed *tapFeedImpl) Events() <-chan sgbucket.FeedEvent {
	return feed.channel
}

func (feed *tapFeedImpl) WriteEvents() chan<- sgbucket.FeedEvent {
	return feed.channel
}

// Closes a TapFeed. Call this if you stop using a TapFeed before its channel ends.
func (feed *tapFeedImpl) Close() error {
	feed.bucket.lock.Lock()
	defer feed.bucket.lock.Unlock()

	fmt.Printf("tapFeedImpl.Close()\n")

	for i, afeed := range feed.bucket.tapFeeds {
		if afeed == feed {
			feed.bucket.tapFeeds[i] = nil
		}
	}
	fmt.Printf("tapFeedImpl.Close() closing events channel\n")
	feed.events.close()
	feed.bucket = nil
	return nil
}

func (feed *tapFeedImpl) run() {
	defer close(feed.channel)
	for {
		event, _ := feed.events.pull().(*sgbucket.FeedEvent)
		if event == nil {
			break
		}
		feed.channel <- *event
	}
}

func (bucket *WalrusBucket) enqueueBackfillEvents(startSequence uint64, keysOnly bool, q *queue) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	for docid, doc := range bucket.Docs {
		if doc.Raw != nil && doc.Sequence >= startSequence {
			event := sgbucket.FeedEvent{
				Opcode: sgbucket.FeedOpMutation,
				Key:    []byte(docid),
				Cas:    doc.Sequence,
			}
			if !keysOnly {
				event.Value = doc.Raw
			}
			q.push(&event)
		}
	}
}

// Caller must have the bucket's RLock, because this method iterates bucket.tapFeeds
func (bucket *WalrusBucket) _postTapEvent(event sgbucket.FeedEvent) {
	var eventNoValue sgbucket.FeedEvent = event // copies the struct
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

func (bucket *WalrusBucket) _postTapMutationEvent(key string, value []byte, seq uint64) {
	bucket._postTapEvent(sgbucket.FeedEvent{
		Opcode: sgbucket.FeedOpMutation,
		Key:    []byte(key),
		Value:  copySlice(value),
		Cas:    seq,
	})
}

func (bucket *WalrusBucket) _postTapDeletionEvent(key string, seq uint64) {
	bucket._postTapEvent(sgbucket.FeedEvent{
		Opcode: sgbucket.FeedOpDeletion,
		Key:    []byte(key),
		Cas:    seq,
	})
}
