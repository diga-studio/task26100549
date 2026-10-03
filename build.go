package caseagg

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Build 从一次性批次迭代器读取全部记录，在 dir 中创建不可变的列式数据集。
// 数据集由 keys.col 与 values.col 两个列文件组成；批次边界不会被记录。
// 若迭代过程中出错，已创建的列文件会被删除，dir 中不留下半成品数据集。
func Build(dir string, nextBatch BatchIter) error {
	if nextBatch == nil {
		return errors.New("caseagg: nextBatch 不能为 nil")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	w, err := newColumnWriter(dir)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			os.Remove(filepath.Join(dir, keysFileName))
			os.Remove(filepath.Join(dir, valuesFileName))
		}
	}()
	for {
		batch, err := nextBatch()
		if err != nil {
			w.close()
			return fmt.Errorf("caseagg: 读取批次失败: %w", err)
		}
		if batch == nil {
			break
		}
		for i, rec := range batch {
			if err := w.write(rec); err != nil {
				w.close()
				return fmt.Errorf("caseagg: 写入第 %d 行（批次内第 %d 行）失败: %w", w.count, i, err)
			}
		}
	}
	if err := w.close(); err != nil {
		return err
	}
	ok = true
	return nil
}
