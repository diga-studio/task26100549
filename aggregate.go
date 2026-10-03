package caseagg

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// mergeFanIn bounds how many run files are merged (and thus how many
// file handles and read buffers are held) in a single merge pass.
const mergeFanIn = 64

// Aggregate groups the dataset in dir by key and, for each group,
// emits COUNT(*) and COUNT(DISTINCT value) through the synchronous
// callback emit. A nil key denotes the NULL-key group. Temporary
// files may be created under tempDir. Output order is unspecified.
//
// Processing is external: records are sorted in bounded in-memory
// runs of at most MaxRunRecords, spilled to run files under tempDir,
// and k-way merged. Because each run is sorted by (key, value) and
// the merge preserves that order, equal keys and equal values become
// adjacent regardless of which run file they landed in, so grouping
// and de-duplication are exact across run boundaries.
func Aggregate(dir, tempDir string, emit func(key []byte, count, distinctValues int64)) error {
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return err
	}
	runs, err := splitRuns(dir, tempDir)
	if err != nil {
		return err
	}
	defer func() {
		for _, p := range runs {
			os.Remove(p)
		}
	}()
	if len(runs) == 0 {
		return nil // empty dataset: no groups
	}
	// Multi-pass merge keeps the number of simultaneously open run
	// files bounded by mergeFanIn.
	for len(runs) > mergeFanIn {
		var next []string
		for i := 0; i < len(runs); i += mergeFanIn {
			end := i + mergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			out := filepath.Join(tempDir, fmt.Sprintf("merge-%d.run", len(next)))
			if err := mergeRunFiles(runs[i:end], out); err != nil {
				return err
			}
			for _, p := range runs[i:end] {
				os.Remove(p)
			}
			next = append(next, out)
		}
		runs = next
	}
	return mergeAggregate(runs, emit)
}

// splitRuns reads the two column files in lockstep and writes sorted
// run files of at most MaxRunRecords records each.
func splitRuns(dir, tempDir string) ([]string, error) {
	kf, err := os.Open(filepath.Join(dir, keysFileName))
	if err != nil {
		return nil, err
	}
	defer kf.Close()
	vf, err := os.Open(filepath.Join(dir, valuesFileName))
	if err != nil {
		return nil, err
	}
	defer vf.Close()
	kr := bufio.NewReader(kf)
	vr := bufio.NewReader(vf)

	var runs []string
	var keyBuf []byte
	for {
		chunk := make([]Record, 0, MaxRunRecords)
		for len(chunk) < MaxRunRecords {
			key, nb, err := readKey(kr, keyBuf)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			keyBuf = nb
			val, has, err := readValue(vr)
			if err != nil {
				return nil, fmt.Errorf("caseagg: values column shorter than keys column: %w", err)
			}
			rec := Record{Value: val, HasValue: has}
			if key != nil {
				k := make([]byte, len(key))
				copy(k, key)
				rec.Key = k
			}
			chunk = append(chunk, rec)
		}
		if len(chunk) == 0 {
			break
		}
		sort.Slice(chunk, func(i, j int) bool { return lessRecord(chunk[i], chunk[j]) })
		path := filepath.Join(tempDir, fmt.Sprintf("run-%d.run", len(runs)))
		if err := writeRun(path, chunk); err != nil {
			return nil, err
		}
		runs = append(runs, path)
	}
	// A leftover value entry means the column files disagree.
	if _, _, err := readValue(vr); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("caseagg: values column longer than keys column")
		}
		return nil, err
	}
	return runs, nil
}

func writeRun(path string, recs []Record) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	var scratch []byte
	for i := range recs {
		if err := writeRecord(w, recs[i], scratch); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// runReader streams records from one sorted run file.
type runReader struct {
	f   *os.File
	r   *bufio.Reader
	buf []byte
	cur Record
	eof bool
}

func openRun(path string) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &runReader{f: f, r: bufio.NewReader(f)}
	rr.advance()
	return rr, nil
}

func (rr *runReader) advance() {
	rec, nb, err := readRecord(rr.r, rr.buf)
	rr.buf = nb
	if err != nil {
		rr.eof = true
		return
	}
	// cur must not alias buf: copy the key so the next advance is safe.
	if rec.Key != nil {
		k := make([]byte, len(rec.Key))
		copy(k, rec.Key)
		rec.Key = k
	}
	rr.cur = rec
}

func (rr *runReader) close() { rr.f.Close() }

// runHeap is a min-heap of run readers ordered by their current record.
type runHeap []*runReader

func (h runHeap) Len() int { return len(h) }
func (h runHeap) Less(i, j int) bool {
	return lessRecord(h[i].cur, h[j].cur)
}
func (h runHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x interface{}) { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() interface{} {
	old := *h
	n := len(old)
	r := old[n-1]
	*h = old[:n-1]
	return r
}

func openRunHeap(paths []string) (runHeap, error) {
	h := make(runHeap, 0, len(paths))
	for _, p := range paths {
		rr, err := openRun(p)
		if err != nil {
			for _, r := range h {
				r.close()
			}
			return nil, err
		}
		if !rr.eof {
			h = append(h, rr)
		} else {
			rr.close()
		}
	}
	heap.Init(&h)
	return h, nil
}

// mergeRunFiles k-way merges sorted run files into one sorted run file.
func mergeRunFiles(paths []string, out string) error {
	h, err := openRunHeap(paths)
	if err != nil {
		return err
	}
	defer func() {
		for _, rr := range h {
			rr.close()
		}
	}()
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	var scratch []byte
	for h.Len() > 0 {
		rr := h[0]
		if err := writeRecord(w, rr.cur, scratch); err != nil {
			f.Close()
			return err
		}
		rr.advance()
		if rr.eof {
			heap.Pop(&h)
			rr.close()
		} else {
			heap.Fix(&h, 0)
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// mergeAggregate k-way merges the final runs and emits one result per
// group in a single streaming pass.
func mergeAggregate(paths []string, emit func(key []byte, count, distinctValues int64)) error {
	h, err := openRunHeap(paths)
	if err != nil {
		return err
	}
	defer func() {
		for _, rr := range h {
			rr.close()
		}
	}()

	var groupKey []byte // valid only when groupNull is false
	var groupNull bool
	var haveGroup bool
	var count, distinct int64
	var lastVal int64
	var lastValSet bool

	flush := func() {
		if !haveGroup {
			return
		}
		var keyCopy []byte
		if !groupNull {
			keyCopy = make([]byte, len(groupKey))
			copy(keyCopy, groupKey)
		}
		emit(keyCopy, count, distinct)
	}

	for h.Len() > 0 {
		rr := h[0]
		rec := rr.cur
		if !haveGroup || groupNull != (rec.Key == nil) ||
			(!groupNull && compareBytes(groupKey, rec.Key) != 0) {
			flush()
			haveGroup = true
			if rec.Key == nil {
				groupNull = true
				groupKey = groupKey[:0]
			} else {
				groupNull = false
				groupKey = append(groupKey[:0], rec.Key...)
			}
			count = 0
			distinct = 0
			lastValSet = false
		}
		count++
		if rec.HasValue && (!lastValSet || rec.Value != lastVal) {
			distinct++
			lastVal = rec.Value
			lastValSet = true
		}
		rr.advance()
		if rr.eof {
			heap.Pop(&h)
			rr.close()
		} else {
			heap.Fix(&h, 0)
		}
	}
	flush()
	return nil
}

// readKey decodes one entry of the keys column file. The returned key
// aliases buf and must be consumed before the next call.
func readKey(r *bufio.Reader, buf []byte) ([]byte, []byte, error) {
	flag, err := r.ReadByte()
	if err != nil {
		return nil, buf, err
	}
	if flag == 0 {
		return nil, buf, nil
	}
	klen, err := r.ReadByte()
	if err != nil {
		return nil, buf, err
	}
	if buf == nil || cap(buf) < int(klen) {
		buf = make([]byte, int(klen))
	}
	buf = buf[:int(klen)]
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, buf, err
	}
	return buf, buf, nil
}

// readValue decodes one entry of the values column file.
func readValue(r *bufio.Reader) (int64, bool, error) {
	flag, err := r.ReadByte()
	if err != nil {
		return 0, false, err
	}
	if flag == 0 {
		return 0, false, nil
	}
	var vb [8]byte
	if _, err := io.ReadFull(r, vb[:]); err != nil {
		return 0, false, err
	}
	return int64(binary.LittleEndian.Uint64(vb[:])), true, nil
}
