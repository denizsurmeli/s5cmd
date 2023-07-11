package buffer_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/peak/s5cmd/v2/buffer"
)

type WriteAtCase struct {
	Order  []int
	Offset int
}

func fillAndReturn(n, size int) []byte {
	chunk := make([]byte, size)
	for i := 0; i < size; i++ {
		chunk[i] = byte(n)
	}
	return chunk
}

func calculateChunkCount(filesize, width int) int64 {
	return int64(math.Ceil(float64(filesize / width)))
}

func TestWriteAtInOrderNoLap(t *testing.T) {
	writerCount := 5
	size := writerCount * (writerCount + 1) / 2
	offset := 64

	rb := buffer.NewRing(size, offset, int64(size))
	for i := 1; i <= size; i++ {
		chunk := fillAndReturn(i, offset)
		_, err := rb.WriteAt(chunk, int64(offset*(i-1)))
		if err != nil {
			t.Errorf("Error while writing chunk\n")
		}
	}

	for i := 1; i <= size; i++ {
		rb.Read()
		chunk := <-rb.Items
		expected := fillAndReturn(i, offset)
		if !bytes.Equal(chunk, expected) {
			t.Errorf("Got %v, expected: %v\n", chunk, expected)
		}
	}
}

func TestWriteAtInOrderLap(t *testing.T) {
	writerCount := 5
	size := writerCount * (writerCount + 1) / 2
	offset := 64
	rb := buffer.NewRing(size, offset, int64(size))

	// first round, lap 0
	for i := 1; i <= size; i++ {
		_, err := rb.WriteAt(fillAndReturn(i, offset), int64(offset*(i-1)))
		if err != nil {
			t.Errorf("Error while writing the chunk\n")
		}
	}
	// drain the channel
	for i := 1; i <= size; i++ {
		rb.Read()
		<-rb.Items
	}

	// reopen the channel, since you have closed by reading all the chunks
	// this never happens when you concurrently both write and read,
	// the purpose of this test to is the behaviour as expected
	// when we lap in the buffer.
	rb.Items = make(chan []byte, size)

	// second round, lap 1
	for i := 1; i <= size; i++ {
		_, err := rb.WriteAt(fillAndReturn(i+size, offset), int64(offset*(i+size-1)))
		if err != nil {
			t.Errorf("Error while writing the chunk\n")
		}

	}

	// read size items
	for i := 1; i <= size; i++ {
		rb.Read()
		chunk := <-rb.Items
		expected := fillAndReturn(size+i, offset)

		if !bytes.Equal(chunk, expected) {
			t.Errorf("Got %v, expected: %v\n", chunk, expected)
		}
	}
}

func TestWriteAtReverseOrderNoLap(t *testing.T) {
	writerCount := 5
	size := writerCount * (writerCount + 1) / 2
	offset := 64
	rb := buffer.NewRing(size, offset, int64(size))

	// put items in reverse order
	for i := 1; i <= size; i++ {
		_, err := rb.WriteAt(fillAndReturn(size-i, offset), int64(offset*(size-i)))
		if err != nil {
			t.Errorf("Error while writing the chunk\n")
		}
	}

	// expect to have them in right order
	for i := 1; i <= size; i++ {
		rb.Read()
		chunk := <-rb.Items
		expected := fillAndReturn(i-1, offset)

		if !bytes.Equal(chunk, expected) {
			t.Errorf("Got %v, expected: %v\n", chunk, expected)
		}
	}
}
