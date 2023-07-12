// A thread-safe ring buffer wrapper around the standard library.
package buffer

import (
	"container/ring"
	"errors"
	"fmt"
	"sync"
)

var (
	taskNotCompleted  = errors.New("Task is not completed")
	emptyWriteAttempt = errors.New("Empty write attempt to task")
	bufferFull        = errors.New("Buffer is full")
	notATask          = errors.New("node value is not a task")
)

// RingBuffer is a wrapper around the container/ring for
// implementing thread-safe operations and
// writeAt functionality.
type RingBuffer struct {
	r           *ring.Ring // next pointer to read
	w           *ring.Ring // next pointer to write
	start       *ring.Ring // pointer for tracking the lapping
	wc          int        // number of writes to the buffer
	rc          int        // number of reades from the buffer
	size        int        // size of the buffer
	offsetWidth int        // offset width is used for calculating indexes
	chunks      int        // expected number of chunks to be written to buffer
	mu          *sync.Mutex
	waitCond    *sync.Cond  // condition to read&write
	fullCond    *sync.Cond  // condition to signal buffer state
	Items       chan []byte // processed items will be passed to this channel
}

func NewRingBuffer(size, chunks, offsetWidth int) *RingBuffer {
	r := ring.New(size)
	mu := &sync.Mutex{}
	return &RingBuffer{
		r:           r,
		w:           r,
		start:       r,
		wc:          0,
		rc:          0,
		size:        size,
		offsetWidth: offsetWidth,
		chunks:      chunks,
		mu:          mu,
		waitCond:    sync.NewCond(mu),
		fullCond:    sync.NewCond(mu),
		Items:       make(chan []byte, size),
	}
}

func (rb *RingBuffer) WriteAt(p []byte, offset int64) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// the buffer is full, wait for it to drain
	for rb.wc%rb.size == 0 && rb.wc != 0 {
		rb.fullCond.Wait()
	}

	// lap can be calculated as follows: the write count / buffer size
	// gives the lap, so wc/size floored is sufficient.

	lap := rb.wc / rb.size
	index := (offset - int64(lap*rb.offsetWidth*rb.size)) / int64(rb.offsetWidth)

	ptr := rb.w.Move(int(index))
	// there is a value that has not been yet read, wait
	for ptr.Value != nil {
		rb.waitCond.Wait()
	}
	ptr.Value = &p

	rb.wc++

	return len(p), nil
}

// Reads the next available chunks and writes to channel
func (rb *RingBuffer) ReadAvailableChunks() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for {
		ptr := rb.r.Value
		if ptr == nil {
			break
		}
		value := ptr.(*[]byte)
		rb.r.Value = nil
		rb.r = rb.r.Next()
		// if there is a writer in the queue that waits for this
		// index to be emptied, signal it to proceed
		rb.waitCond.Signal()
		rb.rc++
		fmt.Printf("Writing :%v\n", *value)
		rb.Items <- *value
		// all chunks have been written and read from the buffer, close the channel
		if rb.rc == rb.chunks {
			close(rb.Items)
			return true
		}
	}
	// if the buffer was full, it has drained, signal writers
	if rb.wc%rb.size == 0 && rb.wc != 0 {
		rb.fullCond.Broadcast()
	}

	return false

}
