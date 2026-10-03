package caseagg

import (
	"fmt"
	"math/rand"
	"testing"
)

// makeExternalCorpus builds a dataset that cannot be sorted in one
// in-memory run when MaxRunRecords is small: one giant group with few
// distinct values interleaved with many small groups, plus NULL-key,
// empty-string, and all-NULL-value groups. The same key-value pair is
// placed at the very head, the very tail, and scattered throughout.
func makeExternalCorpus(rng *rand.Rand) []Record {
	const (
		megaRows     = 60000
		megaDistinct = 37
		smallGroups  = 2000
		smallRows    = 10
	)
	recs := make([]Record, 0, megaRows+smallGroups*smallRows+8)

	// A duplicated key-value pair pinned to the head and the tail.
	head := Record{Key: []byte("edge"), Value: 123456789, HasValue: true}
	recs = append(recs, head)

	// Giant group: many rows, few distinct values, so de-duplication
	// must survive arbitrarily many run boundaries.
	for i := 0; i < megaRows; i++ {
		recs = append(recs, Record{
			Key:      []byte("mega"),
			Value:    int64(rng.Intn(megaDistinct)) - 18, // include negatives
			HasValue: true,
		})
	}
	// Small groups interleaved after the giant group's rows; each small
	// group repeats one value so distinct stays 1, and all share the
	// value 777 with each other and with the mega group range.
	for g := 0; g < smallGroups; g++ {
		key := []byte(fmt.Sprintf("small-%04d", g))
		for r := 0; r < smallRows; r++ {
			v := int64(777)
			if r == 0 && g%3 == 0 {
				recs = append(recs, Record{Key: key, HasValue: false})
				continue
			}
			recs = append(recs, Record{Key: key, Value: v, HasValue: true})
		}
	}
	// NULL-key group and empty-string group, including NULL values.
	for i := 0; i < 500; i++ {
		recs = append(recs, Record{Key: nil, Value: int64(i % 5), HasValue: i%2 == 0})
		recs = append(recs, Record{Key: []byte{}, Value: int64(i % 5), HasValue: i%3 == 0})
	}
	// A group whose values are all NULL.
	for i := 0; i < 100; i++ {
		recs = append(recs, Record{Key: []byte("all-null-vals"), HasValue: false})
	}

	// Scatter more copies of the head pair and the mega group's pairs
	// across the middle, then pin the duplicate at the tail.
	for i := 0; i < 2000; i++ {
		recs = append(recs, head)
		recs = append(recs, Record{Key: []byte("mega"), Value: int64(rng.Intn(megaDistinct)) - 18, HasValue: true})
	}
	recs = append(recs, head)
	return recs
}

func shuffled(recs []Record, rng *rand.Rand) []Record {
	out := make([]Record, len(recs))
	copy(out, recs)
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func TestExternalProcessing(t *testing.T) {
	old := MaxRunRecords
	defer func() { MaxRunRecords = old }()
	MaxRunRecords = 500 // force ~170 runs and a multi-pass merge

	rng := rand.New(rand.NewSource(7))
	base := makeExternalCorpus(rng)
	want := referenceAggregate(base)

	// Sanity: the corpus really exercises the required shapes.
	if want[groupID{key: "mega"}].count < 60000 {
		t.Fatal("corpus lost its giant group")
	}
	if want[groupID{key: "all-null-vals"}].distinct != 0 {
		t.Fatal("corpus lost its all-NULL-value group")
	}

	type variant struct {
		name      string
		recs      []Record
		batchSize int
	}
	variants := []variant{
		{"identity-order-large-batches", base, 100000},
		{"identity-order-tiny-batches", base, 1},
		{"identity-order-odd-batches", base, 977},
		{"reversed", reverse(base), 313},
		{"shuffled-a", shuffled(base, rand.New(rand.NewSource(1))), 977},
		{"shuffled-b", shuffled(base, rand.New(rand.NewSource(2))), 4096},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			got := runAggregate(t, v.recs, v.batchSize)
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
		})
	}
}

func reverse(recs []Record) []Record {
	out := make([]Record, len(recs))
	for i, r := range recs {
		out[len(recs)-1-i] = r
	}
	return out
}

// TestExternalRunsAreCreated proves the test above really spills to
// disk instead of silently fitting in one in-memory run.
func TestExternalRunsAreCreated(t *testing.T) {
	old := MaxRunRecords
	defer func() { MaxRunRecords = old }()
	MaxRunRecords = 64

	rng := rand.New(rand.NewSource(3))
	recs := makeExternalCorpus(rng)
	dir := t.TempDir()
	tmp := t.TempDir()
	i := 0
	if err := Build(dir, func() ([]Record, bool) {
		if i >= len(recs) {
			return nil, false
		}
		end := i + 8192
		if end > len(recs) {
			end = len(recs)
		}
		b := recs[i:end]
		i = end
		return b, true
	}); err != nil {
		t.Fatalf("Build: %v", err)
	}
	runs, err := splitRuns(dir, tmp)
	if err != nil {
		t.Fatalf("splitRuns: %v", err)
	}
	if len(runs) < 2 {
		t.Fatalf("expected multiple spill runs, got %d", len(runs))
	}
	t.Logf("spilled to %d runs", len(runs))
}
