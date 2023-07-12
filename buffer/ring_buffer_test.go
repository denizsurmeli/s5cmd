package buffer_test

import (
	"bytes"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/peak/s5cmd/v2/buffer"
	"gotest.tools/v3/assert"
)

func makeChunk(n, size int) []byte {
	chunk := make([]byte, size)
	for i := 0; i < size; i++ {
		chunk[i] = byte(n)
	}
	return chunk
}

func calculateChunkCount(filesize, width int) int64 {
	return int64(math.Ceil(float64(filesize / width)))
}

func bufferDrainer(t *testing.T, rb *buffer.RingBuffer, offset int) {
	// drain
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			time.Sleep(time.Millisecond * 50)
			done := rb.ReadAvailableChunks()
			if done {
				t.Logf("Read all chunks.\n")
				break
			}
		}
	}()
	go func() {
		defer wg.Done()
		i := 0
		for chunk := range rb.Items {
			expected := makeChunk(i, offset)
			t.Logf("Chunk:%v\n", chunk)
			if !bytes.Equal(chunk, expected) {
				t.Errorf("Got %v, expected: %v", chunk, expected)
			}
			i++
		}
	}()

	wg.Wait()
}

// Allocate gbs, fill whole, write seq, expect ordered seq
func TestSequentialWrite(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := workerCount * (workerCount + 1) / 2
	rb := buffer.NewRingBuffer(gbs, workerCount, offset)

	// nolap
	for i := 0; i < workerCount; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()

	// fill buffer at worst case
	chunks := workerCount * (workerCount + 1) / 2
	rb = buffer.NewRingBuffer(gbs, chunks, offset)

	// nolap
	for i := 0; i < chunks; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()
}

// Allocate gbs, fill whole, write random, expect ordered seq
func TestRandomWrite(t *testing.T) {
	offset, workerCount := 64, 5
	// chunks := workerCount * (workerCount + 1) / 2
	//guaranteed buffer size
	gbs := workerCount * (workerCount + 1) / 2
	rb := buffer.NewRingBuffer(gbs, workerCount, offset)

	// make a shuffled array - nolap
	for _, i := range rand.Perm(workerCount) {
		off := int64(i * offset)
		rb.WriteAt(makeChunk(i, offset), off)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()

	// fill buffer at worst case
	chunks := workerCount * (workerCount + 1) / 2
	rb = buffer.NewRingBuffer(gbs, chunks, offset)

	// make a shuffled array - nolap
	for _, i := range rand.Perm(chunks) {
		off := int64(i * offset)
		rb.WriteAt(makeChunk(i, offset), off)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()
}

// Allocate gbs, fill except the head,read nothing
// after 50 ms, put head, expect seq full read.
func TestBlockingHeadSequentialWrite(t *testing.T) {
	offset, workerCount := 64, 5
	//guaranteed buffer size
	gbs := workerCount * (workerCount + 1) / 2
	rb := buffer.NewRingBuffer(gbs, workerCount, offset)

	// all except first chunk
	for i := 1; i < workerCount; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	isComplete := rb.ReadAvailableChunks()
	assert.Equal(t, isComplete, false, "buffer says it has read all chunks, head chunk is missing")
	assert.Equal(t, len(rb.Items), 0, "buffer must have empty channel but it has items ")

	// give the missing chunk, write at offset 0
	rb.WriteAt(makeChunk(0, offset), int64(0))
	// expect all items to be completed and read from the channel
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()

	// fill the buffer
	chunks := workerCount * (workerCount + 1) / 2
	rb = buffer.NewRingBuffer(gbs, chunks, offset)

	// nolap
	for i := 1; i < chunks; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	isComplete = rb.ReadAvailableChunks()
	assert.Equal(t, isComplete, false, "buffer says it has read all chunks, head chunk is missing")
	assert.Equal(t, len(rb.Items), 0, "buffer must have empty channel but it has items ")

	// give the missing chunk, write at offset 0
	rb.WriteAt(makeChunk(0, offset), int64(0))
	// expect all items to be completed and read from the channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()

}

// Allocate gbs, fill except the head,read nothing
// after 50 ms, put head, expect seq full read.
func TestBlockingRandomWrite(t *testing.T) {
	offset, workerCount := 64, 5
	// chunks := workerCount * (workerCount + 1) / 2
	//guaranteed buffer size
	gbs := workerCount * (workerCount + 1) / 2
	rb := buffer.NewRingBuffer(gbs, workerCount, offset)

	// all except first chunk
	for _, i := range rand.Perm(workerCount) {
		// skip the head chunk
		if i != 0 {
			off := int64(i * offset)
			chunk := makeChunk(i, offset)
			rb.WriteAt(chunk, off)
		}
	}

	isComplete := rb.ReadAvailableChunks()
	assert.Equal(t, isComplete, false, "buffer says it has read all chunks, head chunk is missing")
	assert.Equal(t, len(rb.Items), 0, "buffer must have empty channel but it has items ")

	// give the missing chunk, write at offset 0
	rb.WriteAt(makeChunk(0, offset), int64(0))
	// expect all items to be completed and read from the channel
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()

	// fill the buffer
	chunks := workerCount * (workerCount + 1) / 2
	rb = buffer.NewRingBuffer(gbs, chunks, offset)

	// nolap
	for _, i := range rand.Perm(chunks) {
		// skip the head chunk
		if i != 0 {
			off := int64(i * offset)
			chunk := makeChunk(i, offset)
			rb.WriteAt(chunk, off)
		}
	}

	isComplete = rb.ReadAvailableChunks()
	assert.Equal(t, isComplete, false, "buffer says it has read all chunks, head chunk is missing")
	assert.Equal(t, len(rb.Items), 0, "buffer must have empty channel but it has items ")

	// give the missing chunk, write at offset 0
	rb.WriteAt(makeChunk(0, offset), int64(0))
	// expect all items to be completed and read from the channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()
}

// Allocate gbs, fill with gaps, read piece by piece
// at the end, expect all pieces sequentially.
func TestBlockingGapsSequentially(t *testing.T) {
	offset, workerCount := 64, 5
	// chunks := workerCount * (workerCount + 1) / 2
	//guaranteed buffer size
	gbs := workerCount * (workerCount + 1) / 2
	rb := buffer.NewRingBuffer(gbs, workerCount, offset)

	// you have 5 workers, so 5 chunks expected at the buffer, 0 index
	// is the head chunk
	// fill 1,2
	for i := 1; i <= 2; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}

	// assert no read, no item, no end
	isComplete := rb.ReadAvailableChunks()
	assert.Equal(t, isComplete, false, "buffer says it has read all chunks, head chunk is missing")
	assert.Equal(t, len(rb.Items), 0, "buffer must have empty channel but it has items ")
	// fill 3,4
	for i := 3; i <= 4; i++ {
		off := int64(i * offset)
		chunk := makeChunk(i, offset)
		rb.WriteAt(chunk, off)
	}
	// asssert no read, no item, no end
	isComplete = rb.ReadAvailableChunks()
	assert.Equal(t, isComplete, false, "buffer says it has read all chunks, head chunk is missing")
	assert.Equal(t, len(rb.Items), 0, "buffer must have empty channel but it has items ")

	// give the missing chunk
	rb.WriteAt(makeChunk(0, offset), int64(0))
	// expect everything to be ok
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferDrainer(t, rb, offset)
	}()
	wg.Wait()
}
