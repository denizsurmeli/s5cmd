// A thread-safe ring buffer wrapper around the standard library.
package buffer

import (
	"container/ring"
	"errors"
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
	r          *ring.Ring // next ptr to read
	w          *ring.Ring // next ptr to write
	start      *ring.Ring // lap ptr
	wc         int        // number of writes to the buffer
	rc         int        // number of reads from the buffer
	lap        int        // number of laps in the ring
	bufferSize int        // concurrent writers * (concurrent writers + 1) / 2
	offsetSize int        // part size
	chunks     int64      // expected total chunk count
	mu         sync.Mutex

	Items chan []byte
}

func (rb *RingBuffer) IsFull() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return rb.r == rb.w && rb.wc != 0
}

func (rb *RingBuffer) WriteAt(p []byte, off int64) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.wc > 0 && rb.wc%rb.bufferSize == 0 {
		rb.lap++
	}
	if rb.r != rb.w || (rb.wc == 0 && rb.w == rb.r) || rb.wc-rb.rc <= rb.bufferSize {
		index := (int(off) - (rb.lap * rb.bufferSize * rb.offsetSize)) / rb.offsetSize
		var temp *ring.Ring
		if index == 0 {
			temp = rb.w
		} else {
			temp = rb.w
			for index > 0 {
				temp = temp.Next()
				index--
			}
		}
		temp.Value = p
		rb.wc += 1
		return rb.offsetSize, nil
	}
	return 0, bufferFull
}

func (rb *RingBuffer) Read() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.r != rb.w || (rb.r.Value != nil && rb.rc == 0) || (rb.wc-rb.rc <= rb.bufferSize && rb.r.Value != nil) {
		defer func() {
			// flush the item
			rb.r.Value = nil
			rb.r = rb.r.Next()
		}()
		if rb.r.Value != nil {
			rb.rc += 1
			rb.Items <- rb.r.Value.([]byte)
		}
	}

	if rb.rc == int(rb.chunks) {
		// you are done, close the channel
		close(rb.Items)
	}
}

// The guaranteed buffer size is (workerCount * (workerCount + 1) / 2)
func NewRing(size int, offsetSize int, fileSize int64) *RingBuffer {
	r := ring.New(size)
	rb := &RingBuffer{
		r:          r,
		w:          r,
		start:      r,
		mu:         sync.Mutex{},
		rc:         0,
		wc:         0,
		lap:        0,
		bufferSize: size,
		offsetSize: offsetSize,
		chunks:     fileSize,
		Items:      make(chan []byte, size)}

	return rb
}
