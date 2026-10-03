# caseagg

纯 Go 标准库实现的磁盘分组聚合内核，采用有界内存的外部处理。

## 数据模型

两列数据集：`key` 为最多 64 字节的可空字符串，`value` 为可空 int64。
两列分别保存在数据集目录下的 `keys.col` 与 `values.col` 中。

## API

```go
// 从一次性批次迭代器构建不可变列式数据集；批次边界无查询语义。
err := caseagg.Build(dir, func() ([]caseagg.Record, error) { ... })

// 按 key 分组计算 COUNT(*) 与 COUNT(DISTINCT value)，同步回调逐组输出。
err := caseagg.Aggregate(dir, tempDir, func(key []byte, count, distinct int64) error { ... })
```

- `Record.Key == nil` 表示 NULL 键（与空字符串不同组）；`Record.ValueValid == false` 表示 NULL 值。
- NULL 值计入 `COUNT(*)`，不计入去重计数；空数据集不输出任何组。
- 每个分组恰好输出一次，输出顺序不限；临时文件在返回前删除。

## 实现

外部排序：记录编码为可比较字节串，分块（默认每段 2^20 条）在内存排序后
落入临时有序段，再经堆归并得到全局有序流，单次扫描完成分组与去重计数。
内存占用与数据集大小无关。

## 测试

```sh
go test ./...
```

包含小数据随机对照测试（内存参考实现）与真正触发外部处理的测试
（缩小排序段强制落盘，验证跨段去重、分组独立计数、每组恰好输出一次、
重复位置与批次切分改变后结果不变）。
