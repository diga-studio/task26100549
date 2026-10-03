package caseagg

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	keysFileName   = "keys.bin"
	valuesFileName = "values.bin"
)

// Build reads records from a one-shot batch iterator and creates an
// immutable columnar dataset in dir. nextBatch returns successive
// batches; it reports exhaustion by returning ok == false. Batch
// boundaries carry no query semantics.
//
// The two columns are stored in separate files: keys.bin holds one
// entry per row (flag byte; when present, a length byte plus the raw
// key bytes), values.bin holds one entry per row (flag byte; when
// present, 8 little-endian bytes).
func Build(dir string, nextBatch func() (batch []Record, ok bool)) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	kf, err := os.Create(filepath.Join(dir, keysFileName))
	if err != nil {
		return err
	}
	vf, err := os.Create(filepath.Join(dir, valuesFileName))
	if err != nil {
		kf.Close()
		return err
	}
	kw := bufio.NewWriter(kf)
	vw := bufio.NewWriter(vf)

	var keyScratch []byte
	var valScratch [9]byte
	for {
		batch, ok := nextBatch()
		if !ok {
			break
		}
		for i := range batch {
			rec := &batch[i]
			if rec.Key != nil && len(rec.Key) > MaxKeyLen {
				kf.Close()
				vf.Close()
				return fmt.Errorf("caseagg: key of %d bytes exceeds MaxKeyLen %d", len(rec.Key), MaxKeyLen)
			}
			keyScratch = keyScratch[:0]
			if rec.Key == nil {
				keyScratch = append(keyScratch, 0)
			} else {
				keyScratch = append(keyScratch, 1, byte(len(rec.Key)))
				keyScratch = append(keyScratch, rec.Key...)
			}
			if _, err := kw.Write(keyScratch); err != nil {
				kf.Close()
				vf.Close()
				return err
			}
			if rec.HasValue {
				valScratch[0] = 1
				binary.LittleEndian.PutUint64(valScratch[1:], uint64(rec.Value))
				if _, err := vw.Write(valScratch[:]); err != nil {
					kf.Close()
					vf.Close()
					return err
				}
			} else {
				if err := vw.WriteByte(0); err != nil {
					kf.Close()
					vf.Close()
					return err
				}
			}
		}
	}
	if err := kw.Flush(); err != nil {
		kf.Close()
		vf.Close()
		return err
	}
	if err := vw.Flush(); err != nil {
		kf.Close()
		vf.Close()
		return err
	}
	if err := kf.Close(); err != nil {
		vf.Close()
		return err
	}
	return vf.Close()
}
