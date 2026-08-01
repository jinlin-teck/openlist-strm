package app

import (
	"encoding/json"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// snapshotFile 是单个任务持久化的树快照。
// Key 为监控配置指纹（watchKey），配置变更后旧快照自动作废。
type snapshotFile struct {
	Key     uint64   `json:"key"`
	Entries []string `json:"entries"` // 排序的 "路径:大小" 条目
}

// snapshotStore 管理各任务的树快照文件（与 config.yaml 同目录的 watch-snapshots/ 下）。
// 快照既作为 tree_diff 监控的变更对比基准，也作为 only_new 任务的基线。
type snapshotStore struct {
	dir string
	log *slog.Logger

	mu sync.Mutex
}

func newSnapshotStore(dir string, log *slog.Logger) *snapshotStore {
	return &snapshotStore{dir: dir, log: log}
}

// path 返回任务快照文件路径；taskID 越出快照目录（含路径分隔符等）时返回 false，
// 调用方按「无快照」降级处理。
func (s *snapshotStore) path(taskID string) (string, bool) {
	p := filepath.Join(s.dir, taskID+".json")
	if filepath.Dir(p) != filepath.Clean(s.dir) {
		return "", false
	}
	return p, true
}

// Load 读取任务快照；文件不存在、损坏、taskID 非法或 key 不匹配时返回 false。
func (s *snapshotStore) Load(taskID string, key uint64) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.path(taskID)
	if !ok {
		return nil, false
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, false
	}
	var f snapshotFile
	if err := json.Unmarshal(raw, &f); err != nil {
		s.log.Warn("树快照文件损坏，已忽略", "path", p, "err", err)
		return nil, false
	}
	if f.Key != key {
		return nil, false
	}
	return f.Entries, true
}

// Save 写入任务快照（临时文件 + rename 原子写）。
func (s *snapshotStore) Save(taskID string, key uint64, entries []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.path(taskID)
	if !ok {
		s.log.Warn("任务 ID 无法映射为快照文件名，快照未保存", "task", taskID)
		return
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		s.log.Warn("创建快照目录失败", "dir", s.dir, "err", err)
		return
	}
	raw, err := json.Marshal(snapshotFile{Key: key, Entries: entries})
	if err != nil {
		return
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		s.log.Warn("写入树快照文件失败", "path", p, "err", err)
		return
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		s.log.Warn("写入树快照文件失败", "path", p, "err", err)
	}
}

// Delete 删除任务快照文件。
func (s *snapshotStore) Delete(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.path(taskID)
	if !ok {
		return
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		s.log.Warn("删除树快照文件失败", "path", p, "err", err)
	}
}

// Prune 移除不在 keep 集合中的任务快照（如任务被删除、关闭监控且未开启 only_new）。
func (s *snapshotStore) Prune(keep map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return // 目录不存在属正常（尚无快照）
	}
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		taskID := strings.TrimSuffix(e.Name(), ".json")
		if keep[taskID] {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, e.Name())); err != nil {
			s.log.Warn("清理树快照文件失败", "name", e.Name(), "err", err)
		}
	}
}

// hashEntries 计算快照条目列表的 FNV-1a 指纹，存入 watch-state.json 用于快速比较：
// 指纹一致时无需读取快照文件做 diff。
func hashEntries(entries []string) uint64 {
	h := fnv.New64a()
	for _, e := range entries {
		_, _ = h.Write([]byte(e))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}
