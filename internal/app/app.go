// Package app 把配置、Alist 客户端、STRM 执行器和 cron 调度组装在一起。
package app

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"

	"openlist-strm/internal/alist"
	"openlist-strm/internal/config"
	"openlist-strm/internal/strm"
)

// TaskStatus 是任务的运行时状态（仅存内存）。
type TaskStatus struct {
	Running   bool        `json:"running"`
	LastStart *time.Time  `json:"last_start,omitempty"`
	LastEnd   *time.Time  `json:"last_end,omitempty"`
	LastError string      `json:"last_error,omitempty"`
	Stats     *strm.Stats `json:"stats,omitempty"`
}

// App 是应用核心，供 HTTP 层调用。
type App struct {
	cfgPath string
	log     *slog.Logger

	mu     sync.RWMutex
	cfg    *config.Config
	runner *strm.Runner

	sched   *cron.Cron
	entries map[string]cron.EntryID

	watchMu      sync.Mutex
	watchCancels map[string]context.CancelFunc
	watchKeys    map[string]uint64 // 各任务监控配置的指纹，用于热加载时判断是否需要重启监控
	state        *watchState       // 持久化的上次监控指纹，避免重启后误触发全量扫描

	statusMu sync.Mutex
	status   map[string]*TaskStatus
}

// New 加载配置并启动调度器。
func New(cfgPath string, log *slog.Logger) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	a := &App{
		cfgPath:      cfgPath,
		log:          log,
		sched:        cron.New(cron.WithSeconds()),
		entries:      map[string]cron.EntryID{},
		watchCancels: map[string]context.CancelFunc{},
		watchKeys:    map[string]uint64{},
		state:        loadWatchState(filepath.Join(filepath.Dir(cfgPath), "watch-state.json"), log),
		status:       map[string]*TaskStatus{},
	}
	a.applyConfig(cfg)
	a.sched.Start()
	return a, nil
}

// applyConfig 用新配置重建客户端与调度（调用方须持有写锁或处于初始化阶段）。
func (a *App) applyConfig(cfg *config.Config) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cfg = cfg
	a.runner = strm.New(alist.New(cfg.Alist.BaseURL, cfg.Alist.Token, cfg.Alist.WaitTime, cfg.Alist.UserAgent))

	for _, id := range a.entries {
		a.sched.Remove(id)
	}
	a.entries = map[string]cron.EntryID{}
	for _, t := range cfg.Tasks {
		if t.Cron == "" || !t.IsEnabled() {
			continue
		}
		taskID := t.ID
		entryID, err := a.sched.AddFunc(t.Cron, func() {
			if _, err := a.RunTask(context.Background(), taskID); err != nil {
				a.log.Warn("定时任务执行失败", "task", taskID, "err", err)
			}
		})
		if err != nil {
			a.log.Error("cron 表达式非法，任务未注册", "task", t.ID, "cron", t.Cron, "err", err)
			continue
		}
		a.entries[t.ID] = entryID
		a.log.Info("注册定时任务", "task", t.ID, "cron", t.Cron)
	}

	// 重建变动监控：配置未变化的任务保留原有 goroutine，避免热加载（如新增任务）导致全部任务重跑。
	a.watchMu.Lock()
	newCancels := map[string]context.CancelFunc{}
	newKeys := map[string]uint64{}
	for _, t := range cfg.Tasks {
		if t.WatchInterval <= 0 || !t.IsEnabled() {
			continue
		}
		key := watchKey(cfg.Alist, t)
		if cancel, ok := a.watchCancels[t.ID]; ok && a.watchKeys[t.ID] == key {
			newCancels[t.ID] = cancel // 配置未变，保留监控
			newKeys[t.ID] = key
			delete(a.watchCancels, t.ID)
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		newCancels[t.ID] = cancel
		newKeys[t.ID] = key
		go a.watchTask(ctx, t, a.runner, key)
		a.log.Info("注册变动监控", "task", t.ID, "interval", t.WatchInterval, "mode", t.WatchMode)
	}
	for _, cancel := range a.watchCancels { // 剩余的为已删除或配置变更的任务
		cancel()
	}
	a.watchCancels = newCancels
	a.watchKeys = newKeys
	keep := make(map[string]bool, len(newKeys))
	for id := range newKeys {
		keep[id] = true
	}
	a.state.Prune(keep)
	a.watchMu.Unlock()
}

// watchKey 计算任务监控配置的指纹（含 Alist 连接配置），用于热加载时判断监控 goroutine 是否需要重启。
func watchKey(alistCfg config.AlistConfig, t config.TaskConfig) uint64 {
	data, _ := yaml.Marshal(struct {
		Alist config.AlistConfig `yaml:"alist"`
		Task  config.TaskConfig  `yaml:"task"`
	}{alistCfg, t})
	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64()
}

// watchTask 按间隔扫描远端目录指纹，变化时触发任务。无持久化状态的监控（新建/配置变更）启动后立即执行一次；
// 重启后若能恢复上次指纹且远端无变化，则不触发。
func (a *App) watchTask(ctx context.Context, task config.TaskConfig, runner *strm.Runner, key uint64) {
	log := a.log.With("task", task.ID, "watch", task.WatchMode)
	interval := time.Duration(task.WatchInterval) * time.Second

	// 按监控方式选择探测函数：递归指纹（本地存储）或目录计数（网盘存储）。
	probe := runner.Fingerprint
	if task.WatchMode == config.WatchDirCount {
		probe = runner.DirCount
	}

	var last uint64
	first := true
	if fp, ok := a.state.Get(task.ID, key); ok {
		last, first = fp, false
		log.Debug("恢复上次监控指纹", "fingerprint", fp)
	}
	check := func() {
		fp, err := probe(ctx, task)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("变更探测失败", "err", err)
			}
			return
		}
		if first || fp != last {
			first = false
			last = fp
			a.state.Set(task.ID, key, fp)
			log.Info("检测到文件变化，触发任务")
			if _, err := a.RunTask(ctx, task.ID); err != nil && ctx.Err() == nil {
				log.Warn("监控触发任务失败", "err", err)
			}
		}
	}

	check() // 启动时先跑一次
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

// Config 返回当前配置副本。
func (a *App) Config() config.Config {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return *a.cfg
}

// UpdateConfig 校验、落盘并热加载新配置。
func (a *App) UpdateConfig(cfg *config.Config) error {
	if err := config.Save(a.cfgPath, cfg); err != nil {
		return err
	}
	a.applyConfig(cfg)
	return nil
}

// NextRuns 返回各任务的下一次定时触发时间。
func (a *App) NextRuns() map[string]time.Time {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := map[string]time.Time{}
	for taskID, entryID := range a.entries {
		e := a.sched.Entry(entryID)
		if !e.Next.IsZero() {
			out[taskID] = e.Next
		}
	}
	return out
}

// RunTask 同步执行一次任务；同一任务并发执行会被拒绝。
func (a *App) RunTask(ctx context.Context, taskID string) (*strm.Stats, error) {
	a.mu.RLock()
	cfg := a.cfg
	runner := a.runner
	a.mu.RUnlock()

	t := cfg.Task(taskID)
	if t == nil {
		return nil, fmt.Errorf("任务 %q 不存在", taskID)
	}
	if !t.IsEnabled() {
		return nil, fmt.Errorf("任务 %q 已禁用", taskID)
	}
	task := *t

	st := a.taskStatus(taskID)
	a.statusMu.Lock()
	if st.Running {
		a.statusMu.Unlock()
		return nil, fmt.Errorf("任务 %q 正在运行中", taskID)
	}
	now := time.Now()
	st.Running = true
	st.LastStart = &now
	st.LastError = ""
	st.Stats = nil
	a.statusMu.Unlock()

	log := a.log.With("task", taskID)
	log.Info("任务开始", "source", task.SourceDir, "target", task.TargetDir, "mode", task.Mode)
	stats, err := runner.Run(ctx, task, log)

	a.statusMu.Lock()
	end := time.Now()
	st.Running = false
	st.LastEnd = &end
	if err != nil {
		st.LastError = err.Error()
	} else {
		st.Stats = stats
	}
	a.statusMu.Unlock()

	if err != nil {
		log.Error("任务失败", "err", err)
	} else {
		log.Info("任务完成", "scanned", stats.Scanned, "created", stats.Created,
			"skipped", stats.Skipped, "deleted", stats.Deleted, "downloaded", stats.Downloaded, "failed", stats.Failed)
	}
	return stats, err
}

// Statuses 返回全部任务状态。
func (a *App) Statuses() map[string]TaskStatus {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	out := make(map[string]TaskStatus, len(a.status))
	for k, v := range a.status {
		out[k] = *v
	}
	return out
}

func (a *App) taskStatus(id string) *TaskStatus {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	if a.status[id] == nil {
		a.status[id] = &TaskStatus{}
	}
	return a.status[id]
}

// TestAlist 用给定连接参数探测服务器（验证令牌与连通性）。
func TestAlist(ctx context.Context, baseURL, token, userAgent string) error {
	c := alist.New(baseURL, token, 0, userAgent)
	_, err := c.Me(ctx)
	return err
}

// Close 停止调度器与全部监控。
func (a *App) Close() {
	a.sched.Stop()
	a.watchMu.Lock()
	for _, cancel := range a.watchCancels {
		cancel()
	}
	a.watchMu.Unlock()
}
