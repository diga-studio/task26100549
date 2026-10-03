package caseagg

import (
	"bufio"
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// runSize 是单个内存排序段容纳的最大记录数，超过即落盘为一个有序段。
// 它决定了聚合过程的内存上界；测试可以调小它以触发外部处理。
var runSize = 1 << 20

// 排序段记录的可比较编码（自分隔格式）：
//
//	[keyFlag:1][keyLen:1][keyBytes][valFlag:1][val:8(大端、符号位翻转)]
//
// 字典序即分组序：NULL 键排在所有非 NULL 键之前；非 NULL 键按 (长度, 原始字节)
// 排序，相等键必然相邻；组内 NULL 值在前，非 NULL 值按数值升序，相等值相邻。
func encodeSortKey(rec Record, buf []byte) []byte {
	buf = buf[:0]
	if rec.Key == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1, byte(len(rec.Key)))
		buf = append(buf, rec.Key...)
	}
	if !rec.ValueValid {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		var b [8]byte
		// 符号位翻转后的大端编码，使字节序与 int64 数值序一致。
		v := uint64(rec.Value) ^ (1 << 63)
		for i := 0; i < 8; i++ {
			b[i] = byte(v >> (56 - 8*i))
		}
		buf = append(buf, b[:]...)
	}
	return buf
}

// splitSortKey 将编码拆为键部分与值部分（不含值部分）。
func splitSortKey(enc []byte) (keyPart, valPart []byte) {
	n := 1
	if enc[0] == 1 {
		n += 1 + int(enc[1])
	}
	return enc[:n], enc[n:]
}

// runReader 顺序读取一个有序段文件，返回编码后的记录。
type runReader struct {
	file *os.File
	buf  *bufio.Reader
}

func newRunReader(path string) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &runReader{file: f, buf: bufio.NewReader(f)}, nil
}

// next 读取下一条编码记录；io.EOF 表示段结束。
func (r *runReader) next() ([]byte, error) {
	flag, err := r.buf.ReadByte()
	if err != nil {
		return nil, err
	}
	rec := []byte{flag}
	if flag == 1 {
		l, err := r.buf.ReadByte()
		if err != nil {
			return nil, err
		}
		rec = append(rec, l)
		key := make([]byte, int(l))
		if _, err := io.ReadFull(r.buf, key); err != nil {
			return nil, err
		}
		rec = append(rec, key...)
	}
	vflag, err := r.buf.ReadByte()
	if err != nil {
		return nil, err
	}
	rec = append(rec, vflag)
	if vflag == 1 {
		var b [8]byte
		if _, err := io.ReadFull(r.buf, b[:]); err != nil {
			return nil, err
		}
		rec = append(rec, b[:]...)
	}
	return rec, nil
}

func (r *runReader) close() error { return r.file.Close() }

// mergeItem 是归并堆中的一项：某个段当前的队首记录。
type mergeItem struct {
	enc []byte
	run *runReader
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int            { return len(h) }
func (h mergeHeap) Less(i, j int) bool  { return bytes.Compare(h[i].enc, h[j].enc) < 0 }
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// Aggregate 读取 dir 中的数据集，按 key 分组计算 COUNT(*) 与 COUNT(DISTINCT value)，
// 通过同步回调逐组输出，每个分组恰好输出一次，输出顺序不限。
//
// 实现为有界内存的外部排序：记录被编码为可比较字节串，分块在内存中排序后
// 落入 tempDir 下的临时有序段，再经堆归并得到全局有序流，最后单次扫描
// 完成分组计数与去重计数。临时文件在返回前删除。
func Aggregate(dir, tempDir string, emit EmitFunc) error {
	if emit == nil {
		return errors.New("caseagg: emit 不能为 nil")
	}
	runDir, err := os.MkdirTemp(tempDir, "caseagg-runs-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(runDir)

	runs, err := spillRuns(dir, runDir)
	if err != nil {
		return err
	}
	defer func() {
		for _, r := range runs {
			r.close()
		}
	}()
	return mergeAndEmit(runs, emit)
}

// spillRuns 顺序扫描列文件，将记录分块排序后写入有序段，返回全部段的读取器。
func spillRuns(dir, runDir string) ([]*runReader, error) {
	cr, err := newColumnReader(dir)
	if err != nil {
		return nil, err
	}
	defer cr.close()

	var runs []*runReader
	var paths []string
	block := make([][]byte, 0, runSize)
	flush := func() error {
		if len(block) == 0 {
			return nil
		}
		sort.Slice(block, func(i, j int) bool {
			return bytes.Compare(block[i], block[j]) < 0
		})
		path := filepath.Join(runDir, fmt.Sprintf("run-%06d.bin", len(paths)))
		if err := writeRun(path, block); err != nil {
			return err
		}
		paths = append(paths, path)
		block = make([][]byte, 0, runSize)
		return nil
	}

	var scratch []byte
	for {
		rec, err := cr.read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		scratch = encodeSortKey(rec, scratch)
		enc := make([]byte, len(scratch))
		copy(enc, scratch)
		block = append(block, enc)
		if len(block) >= runSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}

	for _, p := range paths {
		r, err := newRunReader(p)
		if err != nil {
			for _, rr := range runs {
				rr.close()
			}
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func writeRun(path string, block [][]byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, enc := range block {
		if _, err := w.Write(enc); err != nil {
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

// mergeAndEmit 对全部有序段做堆归并，单次扫描输出每个分组。
func mergeAndEmit(runs []*runReader, emit EmitFunc) error {
	h := &mergeHeap{}
	for _, r := range runs {
		enc, err := r.next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		*h = append(*h, mergeItem{enc: enc, run: r})
	}
	heap.Init(h)

	var (
		curKey   []byte // 当前组的键部分编码；nil 表示尚未开始
		curCount int64
		curDist  int64
		lastVal  []byte // 当前组内上一个非 NULL 值的编码
		haveLast bool
	)
	flushGroup := func() error {
		if curKey == nil {
			return nil
		}
		var key []byte
		if curKey[0] == 1 {
			key = make([]byte, int(curKey[1]))
			copy(key, curKey[2:])
		}
		return emit(key, curCount, curDist)
	}

	for h.Len() > 0 {
		top := &(*h)[0]
		keyPart, valPart := splitSortKey(top.enc)
		if curKey == nil || !bytes.Equal(keyPart, curKey) {
			if err := flushGroup(); err != nil {
				return err
			}
			curKey = append(curKey[:0], keyPart...)
			curCount = 0
			curDist = 0
			haveLast = false
		}
		curCount++
		if len(valPart) > 0 && valPart[0] == 1 {
			if !haveLast || !bytes.Equal(valPart, lastVal) {
				curDist++
				lastVal = append(lastVal[:0], valPart...)
				haveLast = true
			}
		}
		enc, err := top.run.next()
		if err == io.EOF {
			heap.Pop(h)
			continue
		}
		if err != nil {
			return err
		}
		top.enc = enc
		heap.Fix(h, 0)
	}
	return flushGroup()
}
