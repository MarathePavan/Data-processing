package sortpkg

import (
	"bufio"
	"container/heap"
	"os"
)

// item represents one entry in heap
type item struct {
	line []byte
	idx  int
}

type minHeap struct {
	data    []*item
	keyType string
}

func (h minHeap) Len() int { return len(h.data) }
func (h minHeap) Less(i, j int) bool {
	a := h.data[i].line
	b := h.data[j].line
	switch h.keyType {
	case "id":
		return parseIntPrefix(a) < parseIntPrefix(b)
	case "name":
		sa, ea := extractNameBounds(a)
		sb, eb := extractNameBounds(b)
		la := string(a[sa:ea])
		lb := string(b[sb:eb])
		return la < lb
	case "continent":
		sa, ea := extractContBounds(a)
		sb, eb := extractContBounds(b)
		return string(a[sa:ea]) < string(b[sb:eb])
	default:
		return string(a) < string(b)
	}
}
func (h minHeap) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }
func (h *minHeap) Push(x interface{}) {
	h.data = append(h.data, x.(*item))
}
func (h *minHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	it := old[n-1]
	h.data = old[:n-1]
	return it
}

// MergeRuns merges sorted run files and calls lineHandler for each merged line.
// keyType is "id", "name", or "continent" to control comparison semantics.
func MergeRuns(runFiles []string, lineHandler func(string) error, keyType string) error {
	files := make([]*os.File, len(runFiles))
	readers := make([]*bufio.Reader, len(runFiles))
	for i, fn := range runFiles {
		f, err := os.Open(fn)
		if err != nil {
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return err
		}
		files[i] = f
		readers[i] = bufio.NewReaderSize(f, 1<<20)
	}
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()

	h := &minHeap{data: make([]*item, 0, len(runFiles)), keyType: keyType}
	heap.Init(h)

	// read first line from each
	for i, r := range readers {
		line, err := r.ReadBytes('\n')
		if err != nil {
			// empty file or EOF
			continue
		}
		cp := make([]byte, len(line))
		copy(cp, line)
		heap.Push(h, &item{line: cp, idx: i})
	}

	for h.Len() > 0 {
		it := heap.Pop(h).(*item)
		if err := lineHandler(string(it.line)); err != nil {
			return err
		}
		// read next from same file
		next, err := readers[it.idx].ReadBytes('\n')
		if err != nil {
			continue // EOF
		}
		cp := make([]byte, len(next))
		copy(cp, next)
		heap.Push(h, &item{line: cp, idx: it.idx})
	}
	return nil
}
