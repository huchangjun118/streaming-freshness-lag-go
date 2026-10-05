package monitor

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// checkpointStore 持久化引擎状态，用于中途失败后的恢复。
type checkpointStore interface {
	// Save 原子地写入一份完整快照，不得留下部分写入。
	Save(data []byte) error
	// Load 读取最近一次成功保存的快照；不存在时返回 nil, nil。
	Load() ([]byte, error)
}

// fileCheckpointStore 基于本地文件的快照存储。
// 写入采用 临时文件 + rename，保证崩溃时要么看到旧快照，
// 要么看到新快照，不会看到部分更新。
type fileCheckpointStore struct {
	path string
}

func newFileCheckpointStore(path string) *fileCheckpointStore {
	return &fileCheckpointStore{path: path}
}

func (f *fileCheckpointStore) Save(data []byte) error {
	dir := filepath.Dir(f.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".checkpoint-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, f.path)
}

func (f *fileCheckpointStore) Load() ([]byte, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return data, err
}
