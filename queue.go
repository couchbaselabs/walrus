package walrus

import (
	"container/list"
	"sync"
)

// Thread-safe producer/consumer queue.
type queue struct {
	list *list.List
	cond *sync.Cond
}

func newQueue() *queue {
	return &queue{
		list: list.New(),
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// Pushes a value into the queue. (Never blocks: the queue has no size limit.)
func (q *queue) push(value interface{}) {
	q.cond.L.Lock()
	q.list.PushFront(value)
	if q.list.Len() == 1 {
		q.cond.Signal()
	}
	q.cond.L.Unlock()
}

// Removes the last/oldest value from the queue; if the queue is empty, blocks.
func (q *queue) pull() interface{} {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.list != nil && q.list.Len() == 0 {
		q.cond.Wait()
	}
	if q.list == nil {
		return nil // queue is closed
	}
	last := q.list.Back()
	q.list.Remove(last)
	return last.Value
}

func (q *queue) close() {
	q.cond.L.Lock()
	if q.list != nil {
		q.list = nil
		q.cond.Broadcast()
	}
	q.cond.L.Unlock()
}
