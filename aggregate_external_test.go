package caseagg

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// withRunSize 临时把内存排序段调小，使测试数据无法全部装入内存，
// 从而真正触发外部处理（多个有序段 + 堆归并）。
func withRunSize(t *testing.T, n int) {
	t.Helper()
	old := runSize
	runSize = n
	t.Cleanup(func() { runSize = old })
}

// makeExternalRecords 构造交错数据集：
//   - 一个超大组 "huge"，含 hugeDistinct 个不同值，每个值重复多次；
//   - smallGroups 个小组，每组少量行；
//   - 大组与小组逐行交错，使任何中间边界都同时切开大组与小组；
//   - 指定的重复对 (key,value) 被钉在序列首尾，并散布在中间多个批次。
func makeExternalRecords(hugeDistinct, hugeRepeats, smallGroups int) []Record {
	var recs []Record
	// 钉在文件首尾的重复对：大组与小组各一，外加 NULL 键组。
	headPairs := []Record{
		{Key: []byte("huge"), Value: 7, ValueValid: true},
		{Key: []byte("small-0"), Value: -9, ValueValid: true},
		{Key: nil, Value: 12345, ValueValid: true},
	}
	recs = append(recs, headPairs...)
	for r := 0; r < hugeRepeats; r++ {
		for v := 0; v < hugeDistinct; v++ {
			recs = append(recs, Record{
				Key:        []byte("huge"),
				Value:      int64(v - hugeDistinct/2), // 覆盖负数
				ValueValid: true,
			})
			// 大组中也混入 NULL 值，只计入总行数。
			if v%10 == 0 {
				recs = append(recs, Record{Key: []byte("huge"), ValueValid: false})
			}
			// 每行大组记录后交错一个小组记录。
			g := (r*hugeDistinct + v) % smallGroups
			recs = append(recs, Record{
				Key:        []byte(fmt.Sprintf("small-%d", g)),
				Value:      int64(g % 5), // 小组间共享相同的值，验证分别去重
				ValueValid: true,
			})
			if g%7 == 0 {
				recs = append(recs, Record{Key: []byte(fmt.Sprintf("small-%d", g)), ValueValid: false})
			}
			// NULL 键组散布在各处。
			if (r*hugeDistinct+v)%13 == 0 {
				recs = append(recs, Record{Key: nil, Value: 12345, ValueValid: true})
				recs = append(recs, Record{Key: nil, ValueValid: false})
			}
		}
	}
	recs = append(recs, headPairs...) // 同样的重复对再钉到文件尾
	return recs
}

// buildWithBatches 以指定批次大小构建数据集。
func buildWithBatches(t *testing.T, dir string, recs []Record, batchSize int) {
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
}

// collectGroups 运行 Aggregate 并收集结果，校验每组恰好输出一次。
func collectGroups(t *testing.T, dir, tempDir string) map[groupKey]groupStat {
	t.Helper()
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

// TestExternalProcessing 真正触发外部处理的端到端测试。
func TestExternalProcessing(t *testing.T) {
	withRunSize(t, 257) // 内存排序段仅 257 条，数据量远超此数

	const (
		hugeDistinct = 40
		hugeRepeats  = 200 // 大组约 40*200*2 ≈ 1.6 万行
		smallGroups  = 300
	)
	recs := makeExternalRecords(hugeDistinct, hugeRepeats, smallGroups)
	if len(recs) < runSize*10 {
		t.Fatalf("数据量 %d 不足以触发外部处理", len(recs))
	}

	root := t.TempDir()
	dir := filepath.Join(root, "data")
	tempDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// 白盒验证：外部排序确实产生了多个有序段（而非全部装入内存）。
	runs, err := spillRunsProbe(dir, recs, 997, tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if runs < 2 {
		t.Fatalf("只产生了 %d 个有序段，外部处理未被触发", runs)
	}

	buildWithBatches(t, dir, recs, 997)
	got := collectGroups(t, dir, tempDir)
	checkAgainstReference(t, got, referenceAggregate(recs))

	// 显式核对关键分组：
	// 大组的去重计数等于不同值个数，尽管每个值重复多次且散布在所有段中。
	huge := got[groupKey{k: "huge"}]
	if huge.distinct != hugeDistinct {
		t.Errorf("huge 组 distinct = %d, want %d", huge.distinct, hugeDistinct)
	}
	// 小组之间分别去重：每个小组的值都来自 {0..4}，distinct 不超过 5。
	for g := 0; g < smallGroups; g++ {
		gs, ok := got[groupKey{k: fmt.Sprintf("small-%d", g)}]
		if !ok {
			t.Fatalf("缺少分组 small-%d", g)
		}
		if gs.distinct > 5 {
			t.Errorf("small-%d distinct = %d, 应 <= 5", g, gs.distinct)
		}
	}
	// 临时文件在返回前清理干净。
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("临时目录残留 %d 个文件", len(entries))
	}
}

// spillRunsProbe 用给定批次切分构建数据集并返回产生的有序段数量。
func spillRunsProbe(dir string, recs []Record, batchSize int, tempDir string) (int, error) {
	probeDir := dir + "-probe"
	probeTmp, err := os.MkdirTemp(tempDir, "probe-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(probeTmp)
	if err := os.MkdirAll(probeDir, 0o755); err != nil {
		return 0, err
	}
	defer os.RemoveAll(probeDir)
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
	if err := Build(probeDir, next); err != nil {
		return 0, err
	}
	runs, err := spillRuns(probeDir, probeTmp)
	if err != nil {
		return 0, err
	}
	for _, r := range runs {
		r.close()
	}
	return len(runs), nil
}

// TestExternalInvariance 验证重复位置与批次切分改变后结果不变。
func TestExternalInvariance(t *testing.T) {
	withRunSize(t, 131)

	base := makeExternalRecords(25, 60, 120)
	want := referenceAggregate(base)

	// 变体 1：同样的多重集合，不同顺序（打乱），不同批次大小。
	rng := rand.New(rand.NewSource(7))
	shuffled := make([]Record, len(base))
	copy(shuffled, base)
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	// 变体 2：把文件首尾的重复对改放到正中间。
	moved := make([]Record, 0, len(base))
	head := base[:3]
	tail := base[len(base)-3:]
	mid := base[3 : len(base)-3]
	moved = append(moved, mid[:len(mid)/2]...)
	moved = append(moved, head...)
	moved = append(moved, tail...)
	moved = append(moved, mid[len(mid)/2:]...)

	variants := []struct {
		name      string
		recs      []Record
		batchSize int
	}{
		{"原始顺序/批次997", base, 997},
		{"原始顺序/批次1", base, 1},
		{"原始顺序/整批", base, len(base)},
		{"打乱顺序/批次513", shuffled, 513},
		{"重复对移到中间/批次4096", moved, 4096},
	}
	var first map[groupKey]groupStat
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "data")
			tempDir := filepath.Join(root, "tmp")
			if err := os.MkdirAll(tempDir, 0o755); err != nil {
				t.Fatal(err)
			}
			buildWithBatches(t, dir, v.recs, v.batchSize)
			got := collectGroups(t, dir, tempDir)
			checkAgainstReference(t, got, want)
			if first == nil {
				first = got
			} else {
				checkAgainstReference(t, got, mapFromStats(first))
			}
		})
	}
}

func mapFromStats(m map[groupKey]groupStat) map[groupKey]groupStat {
	out := make(map[groupKey]groupStat, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
