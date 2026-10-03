package caseagg

import (
	"math/rand"
	"testing"
)

// groupID identifies a group: NULL key vs. key bytes.
type groupID struct {
	isNull bool
	key    string
}

func idOf(key []byte) groupID {
	if key == nil {
		return groupID{isNull: true}
	}
	return groupID{key: string(key)}
}

// groupResult holds COUNT(*) and COUNT(DISTINCT value) for a group.
type groupResult struct {
	count    int64
	distinct int64
}

// referenceAggregate computes the expected result with a trivial
// in-memory implementation over all original rows.
func referenceAggregate(recs []Record) map[groupID]groupResult {
	type g struct {
		count int64
		vals  map[int64]bool
	}
	acc := make(map[groupID]*g)
	for _, r := range recs {
		id := idOf(r.Key)
		gr, ok := acc[id]
		if !ok {
			gr = &g{vals: make(map[int64]bool)}
			acc[id] = gr
		}
		gr.count++
		if r.HasValue {
			gr.vals[r.Value] = true
		}
	}
	out := make(map[groupID]groupResult, len(acc))
	for id, gr := range acc {
		out[id] = groupResult{count: gr.count, distinct: int64(len(gr.vals))}
	}
	return out
}

// runAggregate builds a dataset from recs (in batches of batchSize)
// and collects the emitted groups, failing on duplicate emissions.
func runAggregate(t *testing.T, recs []Record, batchSize int) map[groupID]groupResult {
	t.Helper()
	dir := t.TempDir()
	tmp := t.TempDir()
	i := 0
	err := Build(dir, func() ([]Record, bool) {
		if i >= len(recs) {
			return nil, false
		}
		end := i + batchSize
		if end > len(recs) {
			end = len(recs)
		}
		b := recs[i:end]
		i = end
		return b, true
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := make(map[groupID]groupResult)
	err = Aggregate(dir, tmp, func(key []byte, count, distinct int64) {
		id := idOf(key)
		if _, dup := got[id]; dup {
			t.Fatalf("group %+v emitted more than once", id)
		}
		got[id] = groupResult{count: count, distinct: distinct}
	})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	return got
}

func compareToReference(t *testing.T, recs []Record, batchSize int) {
	t.Helper()
	got := runAggregate(t, recs, batchSize)
	want := referenceAggregate(recs)
	if len(got) != len(want) {
		t.Fatalf("got %d groups, want %d", len(got), len(want))
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Fatalf("missing group %+v", id)
		}
		if g != w {
			t.Fatalf("group %+v: got %+v, want %+v", id, g, w)
		}
	}
}

func TestRandomSmallAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	keySpace := [][]byte{
		nil,
		{},
		{0},
		[]byte("a"),
		[]byte("ab"),
		[]byte("group-with-longer-name"),
	}
	valSpace := []int64{-100, -1, 0, 1, 7, 100, 1 << 40, -(1 << 40)}

	for trial := 0; trial < 300; trial++ {
		n := rng.Intn(120)
		recs := make([]Record, 0, n)
		for i := 0; i < n; i++ {
			var rec Record
			k := keySpace[rng.Intn(len(keySpace))]
			if k != nil {
				kk := make([]byte, len(k))
				copy(kk, k)
				rec.Key = kk
			}
			// Bias toward NULL values so all-NULL-value groups appear.
			if rng.Intn(4) == 0 || (string(k) == "a" && rng.Intn(2) == 0) {
				rec.HasValue = false
			} else {
				rec.HasValue = true
				rec.Value = valSpace[rng.Intn(len(valSpace))]
			}
			recs = append(recs, rec)
		}
		compareToReference(t, recs, 1+rng.Intn(17))
	}
}

func TestEmptyDatasetEmitsNothing(t *testing.T) {
	if got := runAggregate(t, nil, 3); len(got) != 0 {
		t.Fatalf("expected no groups, got %d", len(got))
	}
}

func TestNullKeyDiffersFromEmptyString(t *testing.T) {
	recs := []Record{
		{Key: nil, Value: 5, HasValue: true},
		{Key: []byte{}, Value: 5, HasValue: true},
		{Key: nil, HasValue: false},
		{Key: []byte{}, HasValue: false},
	}
	got := runAggregate(t, recs, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(got))
	}
	if g := got[groupID{isNull: true}]; g.count != 2 || g.distinct != 1 {
		t.Fatalf("NULL-key group: got %+v, want count=2 distinct=1", g)
	}
	if g := got[groupID{key: ""}]; g.count != 2 || g.distinct != 1 {
		t.Fatalf("empty-string group: got %+v, want count=2 distinct=1", g)
	}
}

func TestAllNullValueGroupStillEmitted(t *testing.T) {
	recs := []Record{
		{Key: []byte("only-nulls"), HasValue: false},
		{Key: []byte("only-nulls"), HasValue: false},
		{Key: []byte("only-nulls"), HasValue: false},
	}
	got := runAggregate(t, recs, 10)
	g, ok := got[groupID{key: "only-nulls"}]
	if !ok {
		t.Fatal("all-NULL-value group not emitted")
	}
	if g.count != 3 || g.distinct != 0 {
		t.Fatalf("got %+v, want count=3 distinct=0", g)
	}
}

func TestSameValueInDifferentGroupsCountedSeparately(t *testing.T) {
	recs := []Record{
		{Key: []byte("g1"), Value: 9, HasValue: true},
		{Key: []byte("g2"), Value: 9, HasValue: true},
		{Key: []byte("g1"), Value: 9, HasValue: true},
		{Key: []byte("g2"), Value: 9, HasValue: true},
	}
	got := runAggregate(t, recs, 1)
	for _, name := range []string{"g1", "g2"} {
		if g := got[groupID{key: name}]; g.count != 2 || g.distinct != 1 {
			t.Fatalf("group %s: got %+v, want count=2 distinct=1", name, g)
		}
	}
}

func TestKeyTooLongRejected(t *testing.T) {
	dir := t.TempDir()
	long := make([]byte, MaxKeyLen+1)
	sent := false
	err := Build(dir, func() ([]Record, bool) {
		if sent {
			return nil, false
		}
		sent = true
		return []Record{{Key: long, HasValue: false}}, true
	})
	if err == nil {
		t.Fatal("expected error for oversized key")
	}
}
