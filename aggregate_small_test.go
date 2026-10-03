package caseagg

import (
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// groupKey 是参考实现与结果比对使用的分组键。
// null 为 true 表示 NULL 键；否则按 k 的原始字节区分（空字符串与 NULL 不同）。
type groupKey struct {
	null bool
	k    string
}

type groupStat struct {
	count    int64
	distinct int64
}

// referenceAggregate 是简单的内存参考实现：对全部原始行执行分组聚合。
func referenceAggregate(recs []Record) map[groupKey]groupStat {
	counts := make(map[groupKey]int64)
	seen := make(map[groupKey]map[int64]bool)
	for _, r := range recs {
		gk := groupKey{null: r.Key == nil, k: string(r.Key)}
		counts[gk]++
		if r.ValueValid {
			m := seen[gk]
			if m == nil {
				m = make(map[int64]bool)
				seen[gk] = m
			}
			m[r.Value] = true
		}
	}
	out := make(map[groupKey]groupStat, len(counts))
	for gk, c := range counts {
		out[gk] = groupStat{count: c, distinct: int64(len(seen[gk]))}
	}
	return out
}

// runAggregate 构建数据集并收集 Aggregate 的全部输出，
// 同时校验每个分组恰好输出一次。
func runAggregate(t *testing.T, dir string, recs []Record, batchSize int) map[groupKey]groupStat {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	i := 0
	next := func() ([]Record, error) {
		if i >= len(recs) {
			return nil, nil
		}
		j := i + batchSize
		if j > len(recs) {
			j = len(recs)
		}
		b := recs[i:j]
		i = j
		return b, nil
	}
	if err := Build(dir, next); err != nil {
		t.Fatalf("Build: %v", err)
	}
	tempDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatal(err)
	}
	got := make(map[groupKey]groupStat)
	emit := func(key []byte, count, distinct int64) error {
		gk := groupKey{null: key == nil, k: string(key)}
		if _, dup := got[gk]; dup {
			t.Errorf("分组 %q(null=%v) 被输出了多次", gk.k, gk.null)
		}
		got[gk] = groupStat{count: count, distinct: distinct}
		return nil
	}
	if err := Aggregate(dir, tempDir, emit); err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	return got
}

func checkAgainstReference(t *testing.T, got, want map[groupKey]groupStat) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("分组数不一致: got %d, want %d", len(got), len(want))
	}
	for gk, ws := range want {
		gs, ok := got[gk]
		if !ok {
			t.Fatalf("缺少分组 %q(null=%v)", gk.k, gk.null)
		}
		if gs != ws {
			t.Errorf("分组 %q(null=%v): got %+v, want %+v", gk.k, gk.null, gs, ws)
		}
	}
}

// TestSmallRandomAgainstReference 小数据随机对照测试：
// 覆盖 NULL 键、空字符串、负数、全 NULL 值组、同一整数出现在不同组。
func TestSmallRandomAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	keys := [][]byte{
		nil, // NULL 键
		{},  // 空字符串，与 NULL 不同
		[]byte("a"),
		[]byte("b"),
		[]byte("hello"),
		[]byte{0x00, 0x01}, // 含不可见字节
	}
	// 取值池刻意压小，让同一整数必然同时出现在不同组。
	values := []int64{-100, -1, 0, 1, 7, 42, 100}

	for trial := 0; trial < 200; trial++ {
		n := rng.Intn(60)
		recs := make([]Record, n)
		for i := range recs {
			k := keys[rng.Intn(len(keys))]
			var key []byte
			if k != nil {
				key = append([]byte(nil), k...)
			}
			recs[i].Key = key
			// 约 1/4 的值为 NULL；"c" 组（下标 2 的 "a" 之外的组）也可能全 NULL。
			if rng.Intn(4) == 0 {
				recs[i].ValueValid = false
			} else {
				recs[i].Value = values[rng.Intn(len(values))]
				recs[i].ValueValid = true
			}
		}
		// 保证存在全 NULL 值的组：追加若干 NULL 键 + NULL 值的行。
		if trial%3 == 0 {
			for j := 0; j < 3; j++ {
				recs = append(recs, Record{Key: nil, ValueValid: false})
			}
		}
		batchSize := 1 + rng.Intn(17)
		dir := filepath.Join(t.TempDir(), "data")
		got := runAggregate(t, dir, recs, batchSize)
		checkAgainstReference(t, got, referenceAggregate(recs))
	}
}

// TestEmptyDataset 空数据集不输出任何组。
func TestEmptyDataset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	got := runAggregate(t, dir, nil, 4)
	if len(got) != 0 {
		t.Fatalf("空数据集输出了 %d 个分组", len(got))
	}
}

// TestAllNullValuesGroup 全 NULL 值的组仍须输出，去重计数为零。
func TestAllNullValuesGroup(t *testing.T) {
	recs := []Record{
		{Key: []byte("x"), ValueValid: false},
		{Key: []byte("x"), ValueValid: false},
		{Key: nil, ValueValid: false},
		{Key: []byte("y"), Value: -5, ValueValid: true},
		{Key: []byte("y"), Value: -5, ValueValid: true},
		{Key: []byte("y"), ValueValid: false},
	}
	dir := filepath.Join(t.TempDir(), "data")
	got := runAggregate(t, dir, recs, 2)
	want := map[groupKey]groupStat{
		{null: false, k: "x"}: {count: 2, distinct: 0},
		{null: true}:          {count: 1, distinct: 0},
		{null: false, k: "y"}: {count: 3, distinct: 1},
	}
	checkAgainstReference(t, got, want)
}

// TestNullKeyDistinctFromEmpty NULL 键与空字符串键属于不同分组。
func TestNullKeyDistinctFromEmpty(t *testing.T) {
	recs := []Record{
		{Key: nil, Value: 1, ValueValid: true},
		{Key: []byte{}, Value: 1, ValueValid: true},
		{Key: []byte{}, Value: 1, ValueValid: true},
	}
	dir := filepath.Join(t.TempDir(), "data")
	got := runAggregate(t, dir, recs, 10)
	want := map[groupKey]groupStat{
		{null: true}:         {count: 1, distinct: 1},
		{null: false, k: ""}: {count: 2, distinct: 1},
	}
	checkAgainstReference(t, got, want)
}

// TestKeyLengthBoundary 64 字节键可写入，超长键报错。
func TestKeyLengthBoundary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	k64 := make([]byte, MaxKeyLen)
	for i := range k64 {
		k64[i] = byte(i)
	}
	recs := []Record{{Key: k64, Value: 1, ValueValid: true}}
	got := runAggregate(t, dir, recs, 1)
	if len(got) != 1 {
		t.Fatalf("got %d groups, want 1", len(got))
	}

	dir2 := filepath.Join(t.TempDir(), "data2")
	k65 := make([]byte, MaxKeyLen+1)
	i := 0
	next := func() ([]Record, error) {
		if i > 0 {
			return nil, nil
		}
		i++
		return []Record{{Key: k65, ValueValid: false}}, nil
	}
	if err := Build(dir2, next); err == nil {
		t.Fatal("超长键未报错")
	}
	// 失败的 Build 不应留下半成品数据集。
	for _, name := range []string{keysFileName, valuesFileName} {
		if _, err := os.Stat(filepath.Join(dir2, name)); !os.IsNotExist(err) {
			t.Fatalf("失败的 Build 留下了 %s", name)
		}
	}
}

// TestSortKeyEncodingOrder 验证编码序与分组语义一致：
// NULL 键独立于空字符串；相等键相邻；组内相等值相邻。
func TestSortKeyEncodingOrder(t *testing.T) {
	recs := []Record{
		{Key: nil, ValueValid: false},
		{Key: []byte{}, ValueValid: false},
		{Key: []byte("a"), Value: -1, ValueValid: true},
		{Key: []byte("a"), Value: 1, ValueValid: true},
		{Key: []byte("a"), ValueValid: false},
		{Key: []byte("ab"), Value: 1, ValueValid: true},
		{Key: []byte("b"), Value: 1, ValueValid: true},
	}
	encs := make([][]byte, len(recs))
	for i, r := range recs {
		encs[i] = encodeSortKey(r, nil)
	}
	sorted := make([][]byte, len(encs))
	copy(sorted, encs)
	sort.Slice(sorted, func(i, j int) bool {
		return string(sorted[i]) < string(sorted[j])
	})
	// 排序后相同键必须连续。
	seen := make(map[string]bool)
	var prev string
	for _, e := range sorted {
		kp, _ := splitSortKey(e)
		ks := string(kp)
		if ks != prev {
			if seen[ks] {
				t.Fatalf("键编码 %x 不连续", kp)
			}
			seen[ks] = true
			prev = ks
		}
	}
}
