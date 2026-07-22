package app

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"
)

// watchEntry 是单个任务持久化的监控状态。
type watchEntry struct {
	Key uint64 `json:"key"` // 监控配置指纹（watchKey），配置变更后旧状态自动作废
	Fp  uint64 `json:"fp"`  // 上次探测到的远端指纹（dir_count 方式为子项数量）
}

// watchState 把各任务的上次监控指纹持久化到 JSON 文件（与 config.yaml 同目录的
// watch-state.json），避免服务/容器重启后被误判为「首次启动」或「数量变化」而全量重扫。
type watchState struct {
	path string
	log  *slog.Logger

	mu   sync.Mutex
	data map[string]watchEntry
}

// loadWatchState 从 path 加载监控状态；文件不存在或损坏时返回空状态。
func loadWatchState(path string, log *slog.Logger) *watchState {
	s := &watchState{path: path, log: log, data: map[string]watchEntry{}}
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Warn("读取监控状态文件失败", "path", path, "err", err)
		}
		return s
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		log.Warn("监控状态文件损坏，已重置", "path", path, "err", err)
		s.data = map[string]watchEntry{}
	}
	return s
}

// Get 返回任务上次持久化的指纹；仅当监控配置指纹一致时有效。
func (s *watchState) Get(taskID string, key uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[taskID]
	if !ok || e.Key != key {
		return 0, false
	}
	return e.Fp, true
}

// Set 更新任务指纹并落盘。
func (s *watchState) Set(taskID string, key, fp uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[taskID] = watchEntry{Key: key, Fp: fp}
	s.saveLocked()
}

// Prune 移除已没有监控的任务的状态（如任务被删除或关闭了变动监控）。
func (s *watchState) Prune(keep map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for id := range s.data {
		if !keep[id] {
			delete(s.data, id)
			changed = true
		}
	}
	if changed {
		s.saveLocked()
	}
}

// saveLocked 原子写回状态文件（调用方须持有锁）。
func (s *watchState) saveLocked() {
	raw, err := json.Marshal(s.data)
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		s.log.Warn("写入监控状态文件失败", "path", s.path, "err", err)
		return
	}
	if err := os.Rename(tmp, s.path); err != nil {
		s.log.Warn("写入监控状态文件失败", "path", s.path, "err", err)
	}
}
