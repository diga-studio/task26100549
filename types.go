// Package caseagg 提供基于磁盘的有界内存分组聚合内核。
// 数据集由两列组成：可空字符串 key（最多 64 字节）与可空 int64 value，
// 两列分别保存在本地文件中。支持 Build 构建不可变数据集，
// 以及 Aggregate 按 key 分组计算 COUNT(*) 与 COUNT(DISTINCT value)。
package caseagg

// MaxKeyLen 是 key 的最大字节长度。
const MaxKeyLen = 64

// Record 是数据集中的一行。
// Key 为 nil 表示 NULL 键；非 nil（包括空切片）表示非 NULL 键，按原始字节比较。
// ValueValid 为 false 表示 NULL 值。
type Record struct {
	Key        []byte
	Value      int64
	ValueValid bool
}

// BatchIter 是一次性批次迭代器。
// 每次调用返回一个批次；返回 nil 批次与 nil 错误表示数据结束。
// 批次边界不具有查询语义。
type BatchIter func() ([]Record, error)

// EmitFunc 是聚合结果的同步回调，每个分组恰好调用一次。
// key 为 nil 表示 NULL 键组。
// count 是该组总行数（COUNT(*)），distinct 是该组非 NULL 值的去重计数。
type EmitFunc func(key []byte, count, distinct int64) error
