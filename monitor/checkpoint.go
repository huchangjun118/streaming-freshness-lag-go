package monitor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// checkpoint 是持久化到本地文件的监控状态快照。
// 原子写入（临时文件 + rename），保证不留部分更新的分区状态。
type checkpoint struct {
	Partitions map[string]partitionState `json:"partitions"`
	Committed  int64                     `json:"committed"` // 已提交的上报计数
}

// checkpointStore 负责 checkpoint 的原子读写。
type checkpointStore struct {
	path string
}

func newCheckpointStore(path string) *checkpointStore {
	return &checkpointStore{path: path}
}

// load 读取最近一次已提交的 checkpoint；不存在时返回 (nil, nil)。
func (s *checkpointStore) load() (*checkpoint, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var c checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	return &c, nil
}

// save 原子写入 checkpoint：先写临时文件并 fsync，再 rename 覆盖，
// 任何时刻文件要么是旧版本要么是新版本，不会出现部分更新。
func (s *checkpointStore) save(c *checkpoint) error {
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open tmp checkpoint: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write tmp checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync tmp checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close tmp checkpoint: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	// fsync 目录，保证 rename 落盘。
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		dir.Sync()
		dir.Close()
	}
	return nil
}
