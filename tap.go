package walrus

import "github.com/couchbase/sg-bucket"

type tapFeedImpl struct {
	bucket  *lolrus
	channel chan sgbucket.TapEvent
	args    sgbucket.TapArguments
	events  *queue
}

// Starts a TAP feed on a client connection. The events can be read from the returned channel.
// To stop receiving events, call Close() on the feed.
func (bucket *lolrus) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
	channel := make(chan sgbucket.TapEvent, 10)
	feed := &tapFeedImpl{
		bucket:  bucket,
		channel: channel,
		args:    args,
		events:  newQueue(),
	}

	if args.Backfill != sgbucket.TapNoBackfill {
		feed.events.push(&sgbucket.TapEvent{Opcode: sgbucket.TapBeginBackfill})
		bucket.enqueueBackfillEvents(args.Backfill, args.KeysOnly, feed.events)
		feed.events.push(&sgbucket.TapEvent{Opcode: sgbucket.TapEndBackfill})
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

func (feed *tapFeedImpl) Events() <-chan sgbucket.TapEvent {
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
		event, _ := feed.events.pull().(*sgbucket.TapEvent)
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
			event := sgbucket.TapEvent{
				Opcode:   sgbucket.TapMutation,
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
func (bucket *lolrus) _postTapEvent(event sgbucket.TapEvent) {
	var eventNoValue sgbucket.TapEvent = event // copies the struct
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
	bucket._postTapEvent(sgbucket.TapEvent{
		Opcode:   sgbucket.TapMutation,
		Key:      []byte(key),
		Value:    copySlice(value),
		Sequence: seq,
	})
}

func (bucket *lolrus) _postTapDeletionEvent(key string, seq uint64) {
	bucket._postTapEvent(sgbucket.TapEvent{
		Opcode:   sgbucket.TapDeletion,
		Key:      []byte(key),
		Sequence: seq,
	})
}
