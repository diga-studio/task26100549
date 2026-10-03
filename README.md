# caseagg

Disk-backed, bounded-memory grouping aggregation kernel in pure Go
(standard library only). It manages a two-column dataset — a nullable
string key of at most 64 bytes and a nullable int64 value — stored as
two local column files, and computes `COUNT(*)` and
`COUNT(DISTINCT value)` per key group with external (spill-to-disk)
processing.

## API

```go
type Record struct {
    Key      []byte // nil = NULL; non-nil may be empty (empty string), max 64 bytes
    Value    int64  // meaningful only when HasValue
    HasValue bool   // false = NULL value
}

// Create an immutable columnar dataset in dir from a one-shot batch
// iterator. Batch boundaries carry no query semantics.
func Build(dir string, nextBatch func() (batch []Record, ok bool)) error

// Group by key; synchronously emit COUNT(*) and COUNT(DISTINCT value)
// per group (nil key = NULL-key group). Output order is unspecified.
func Aggregate(dir, tempDir string, emit func(key []byte, count, distinctValues int64)) error

// Bounds in-memory records during external processing (tests may lower it).
var MaxRunRecords = 1 << 20
```

## On-disk format

`Build` writes two column files into `dir`:

- `keys.bin` — per row: flag byte (`0` = NULL, `1` = present); when
  present, one length byte (0–64) followed by the raw key bytes.
- `values.bin` — per row: flag byte; when present, 8 little-endian
  bytes of the int64 value.

`Aggregate` spills sorted runs into `tempDir` (record format: key
entry as above followed by value entry as above), merges them k-way
with a bounded fan-in, and removes them when done.

## Semantics

- Each group is emitted exactly once; counts match running the query
  over all original rows regardless of data distribution, duplicate
  placement, batch splits, or run boundaries.
- Duplicate values are de-duplicated per group even when spread across
  different intermediate run files; different groups de-duplicate
  independently.
- NULL keys form one group, distinct from the empty-string key;
  non-NULL keys group by raw byte equality.
- NULL values count toward `COUNT(*)` but not toward the distinct
  count; all-NULL-value groups are still emitted with distinct = 0.
- An empty dataset emits no groups.

## How it works

1. `splitRuns` reads both column files in lockstep in chunks of at
   most `MaxRunRecords`, sorts each chunk by `(key, value)`, and
   writes it as a run file.
2. Runs are merged k-way (fan-in ≤ 64, multiple passes if needed), so
   memory stays bounded by the chunk size plus the merge fan-in.
3. The final merge streams records in global `(key, value)` order:
   group boundaries are detected on key change, and distinct values
   are counted by comparing each value with the previous one inside
   the group — duplicates are adjacent after sorting, no matter which
   run file they came from.

## Tests

```
go test ./...
```

- `case_test.go` — randomized small-data tests against a trivial
  in-memory reference, covering NULL keys, empty strings, negative
  values, all-NULL-value groups, and the same integer in different
  groups.
- `external_test.go` — forces external processing (`MaxRunRecords`
  lowered so ~170+ runs spill to disk, triggering multi-pass merges),
  with a giant group interleaved with thousands of small groups and
  duplicate key-value pairs pinned at the head, tail, and middle of
  the corpus. Results are verified to be identical across record
  orderings (identity / reversed / shuffled) and batch splits.
