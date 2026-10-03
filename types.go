// Package caseagg implements a disk-backed, bounded-memory grouping
// aggregation kernel over a two-column dataset: a nullable string key
// (at most 64 bytes) and a nullable int64 value.
package caseagg

// Record is a single row of the dataset.
type Record struct {
	// Key is nil for NULL. A non-nil key may be empty (empty string,
	// distinct from NULL) and must be at most MaxKeyLen bytes.
	Key []byte
	// Value is meaningful only when HasValue is true.
	Value    int64
	HasValue bool
}

// MaxKeyLen is the maximum byte length of a non-NULL key.
const MaxKeyLen = 64

// MaxRunRecords bounds how many records are held in memory at once
// during external processing. Tests may lower it to force spilling.
var MaxRunRecords = 1 << 20
