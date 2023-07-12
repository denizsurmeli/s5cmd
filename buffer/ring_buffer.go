// A thread-safe ring buffer wrapper around the standard library.
package buffer

import (
	"container/ring"
	"io"
	"sync"
)

// RingBuffer is a wrapper around the container/ring for
// implementing thread-safe operations and
// writeAt functionality.
type RingBuffer struct {
	r           *ring.Ring // next pointer to read
	w           io.Writer  // will be written to this in order
	wc          int        // number of writes to the buffer
	rc          int        // number of reades from the buffer
	size        int        // size of the buffer
	offsetWidth int        // offset width is used for calculating indexes
	Chunks      int        // expected number of Chunks to be written to buffer
	mu          *sync.Mutex
}

func NewRingBuffer(size, chunks, offsetWidth int, w io.Writer) *RingBuffer {
	r := ring.New(size)
	mu := &sync.Mutex{}
	return &RingBuffer{
		r:           r,
		w:           w,
		wc:          0,
		rc:          0,
		size:        size,
		offsetWidth: offsetWidth,
		Chunks:      chunks,
		mu:          mu,
	}
}

func (rb *RingBuffer) WriteAt(p []byte, offset int64) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	// lap can be calculated as follows: the write count / buffer size
	// gives the lap, so wc/size floored is sufficient.
	// lap := rb.wc / rb.size
	// index := (offset - int64(lap*rb.offsetWidth*rb.size)) / int64(rb.offsetWidth)
	// write the things you can write
	_rc := rb.rc
	for {
		val := rb.r.Value
		if val == nil {
			break
		}

		rb.w.Write(val.([]byte))
		rb.r.Value = nil
		rb.r = rb.r.Next()
		rb.rc++
	}
	diff := rb.rc - _rc
	rb.r = rb.r.Move(-diff)
	rb.r.Value = p
	rb.wc++

	if rb.rc == rb.Chunks-1 {
		for {
			val := rb.r.Value
			if val == nil {
				break
			}
	
			rb.w.Write(val.([]byte))
			rb.r.Value = nil
			rb.r = rb.r.Next()
			rb.rc++
		}
	}

	return len(p), nil
}
