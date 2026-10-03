package caseagg

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// 列文件格式：
// keys 文件：每行 1 字节标志（0=NULL，1=非 NULL），非 NULL 时跟 1 字节长度与原始字节。
// values 文件：每行 1 字节标志（0=NULL，1=非 NULL），非 NULL 时跟 8 字节小端 int64。
// 两个文件的第 i 行共同组成数据集的第 i 条记录。

const (
	keysFileName   = "keys.col"
	valuesFileName = "values.col"
)

// columnWriter 将记录追加写入 keys 与 values 两个列文件。
type columnWriter struct {
	keysFile   *os.File
	valuesFile *os.File
	keysBuf    *bufio.Writer
	valuesBuf  *bufio.Writer
	scratch    [1 + 1 + MaxKeyLen]byte
	count      int64
}

func newColumnWriter(dir string) (*columnWriter, error) {
	kf, err := os.Create(filepath.Join(dir, keysFileName))
	if err != nil {
		return nil, err
	}
	vf, err := os.Create(filepath.Join(dir, valuesFileName))
	if err != nil {
		kf.Close()
		return nil, err
	}
	w := &columnWriter{
		keysFile:   kf,
		valuesFile: vf,
		keysBuf:    bufio.NewWriter(kf),
		valuesBuf:  bufio.NewWriter(vf),
	}
	return w, nil
}

func (w *columnWriter) write(rec Record) error {
	if rec.Key == nil {
		if err := w.keysBuf.WriteByte(0); err != nil {
			return err
		}
	} else {
		if len(rec.Key) > MaxKeyLen {
			return fmt.Errorf("caseagg: key 长度 %d 超过上限 %d", len(rec.Key), MaxKeyLen)
		}
		w.scratch[0] = 1
		w.scratch[1] = byte(len(rec.Key))
		n := copy(w.scratch[2:], rec.Key)
		if _, err := w.keysBuf.Write(w.scratch[:2+n]); err != nil {
			return err
		}
	}
	if !rec.ValueValid {
		if err := w.valuesBuf.WriteByte(0); err != nil {
			return err
		}
	} else {
		w.scratch[0] = 1
		binary.LittleEndian.PutUint64(w.scratch[1:], uint64(rec.Value))
		if _, err := w.valuesBuf.Write(w.scratch[:9]); err != nil {
			return err
		}
	}
	w.count++
	return nil
}

func (w *columnWriter) close() error {
	err1 := w.keysBuf.Flush()
	err2 := w.valuesBuf.Flush()
	err3 := w.keysFile.Close()
	err4 := w.valuesFile.Close()
	return errors.Join(err1, err2, err3, err4)
}

// columnReader 顺序读取 keys 与 values 两个列文件。
type columnReader struct {
	keysFile   *os.File
	valuesFile *os.File
	keysBuf    *bufio.Reader
	valuesBuf  *bufio.Reader
	scratch    [9]byte
}

func newColumnReader(dir string) (*columnReader, error) {
	kf, err := os.Open(filepath.Join(dir, keysFileName))
	if err != nil {
		return nil, err
	}
	vf, err := os.Open(filepath.Join(dir, valuesFileName))
	if err != nil {
		kf.Close()
		return nil, err
	}
	r := &columnReader{
		keysFile:   kf,
		valuesFile: vf,
		keysBuf:    bufio.NewReader(kf),
		valuesBuf:  bufio.NewReader(vf),
	}
	return r, nil
}

// read 读取下一条记录。返回 io.EOF 表示读取完毕。
// 返回的 Record.Key 指向内部缓冲，下次调用 read 前有效。
func (r *columnReader) read() (Record, error) {
	var rec Record
	flag, err := r.keysBuf.ReadByte()
	if err != nil {
		return rec, err // 正常结束时为 io.EOF
	}
	switch flag {
	case 0:
		rec.Key = nil
	case 1:
		l, err := r.keysBuf.ReadByte()
		if err != nil {
			return rec, fmt.Errorf("caseagg: keys 列文件损坏（长度字节）: %w", err)
		}
		key := make([]byte, int(l))
		if _, err := io.ReadFull(r.keysBuf, key); err != nil {
			return rec, fmt.Errorf("caseagg: keys 列文件损坏（内容）: %w", err)
		}
		rec.Key = key
	default:
		return rec, fmt.Errorf("caseagg: keys 列文件损坏（非法标志 %d）", flag)
	}
	vflag, err := r.valuesBuf.ReadByte()
	if err != nil {
		return rec, fmt.Errorf("caseagg: values 列文件损坏（标志）: %w", err)
	}
	switch vflag {
	case 0:
		rec.ValueValid = false
	case 1:
		if _, err := io.ReadFull(r.valuesBuf, r.scratch[:8]); err != nil {
			return rec, fmt.Errorf("caseagg: values 列文件损坏（内容）: %w", err)
		}
		rec.Value = int64(binary.LittleEndian.Uint64(r.scratch[:8]))
		rec.ValueValid = true
	default:
		return rec, fmt.Errorf("caseagg: values 列文件损坏（非法标志 %d）", vflag)
	}
	return rec, nil
}

func (r *columnReader) close() error {
	err1 := r.keysFile.Close()
	err2 := r.valuesFile.Close()
	return errors.Join(err1, err2)
}
