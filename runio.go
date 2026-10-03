package caseagg

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Record wire format, shared by the key/value column files and the
// temporary run files:
//
//	keyFlag  byte   0 = NULL key, 1 = key present
//	keyLen   byte   only when keyFlag == 1; 0..MaxKeyLen
//	keyBytes []byte only when keyFlag == 1
//	valFlag  byte   0 = NULL value, 1 = value present
//	valBytes 8      little-endian int64, only when valFlag == 1

func writeRecord(w *bufio.Writer, rec Record, scratch []byte) error {
	scratch = scratch[:0]
	if rec.Key == nil {
		scratch = append(scratch, 0)
	} else {
		scratch = append(scratch, 1, byte(len(rec.Key)))
		scratch = append(scratch, rec.Key...)
	}
	if rec.HasValue {
		scratch = append(scratch, 1)
		var vb [8]byte
		binary.LittleEndian.PutUint64(vb[:], uint64(rec.Value))
		scratch = append(scratch, vb[:]...)
	} else {
		scratch = append(scratch, 0)
	}
	_, err := w.Write(scratch)
	return err
}

// readRecord decodes one record. It returns io.EOF exactly at a record
// boundary. The returned key aliases buf and must be consumed before
// the next call.
func readRecord(r *bufio.Reader, buf []byte) (Record, []byte, error) {
	var rec Record
	keyFlag, err := r.ReadByte()
	if err != nil {
		return rec, buf, err // io.EOF at boundary is fine
	}
	if keyFlag == 1 {
		klen, err := r.ReadByte()
		if err != nil {
			return rec, buf, err
		}
		if buf == nil || cap(buf) < int(klen) {
			buf = make([]byte, int(klen))
		}
		buf = buf[:int(klen)]
		if _, err := io.ReadFull(r, buf); err != nil {
			return rec, buf, err
		}
		rec.Key = buf
	} else {
		rec.Key = nil
	}
	valFlag, err := r.ReadByte()
	if err != nil {
		return rec, buf, err
	}
	if valFlag == 1 {
		var vb [8]byte
		if _, err := io.ReadFull(r, vb[:]); err != nil {
			return rec, buf, err
		}
		rec.Value = int64(binary.LittleEndian.Uint64(vb[:]))
		rec.HasValue = true
	}
	return rec, buf, nil
}

// lessRecord orders records by (NULL key last? no: NULL first), then
// key bytes, then NULL value first, then value. Only the relative
// order matters; it must be a total order so equal keys and equal
// values are adjacent after sorting.
func lessRecord(a, b Record) bool {
	if (a.Key == nil) != (b.Key == nil) {
		return a.Key == nil // NULL keys sort first
	}
	if a.Key != nil {
		if c := compareBytes(a.Key, b.Key); c != 0 {
			return c < 0
		}
	}
	if a.HasValue != b.HasValue {
		return !a.HasValue // NULL values sort first
	}
	return a.HasValue && a.Value < b.Value
}

func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}
