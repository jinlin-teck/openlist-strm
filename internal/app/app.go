// Package app 把配置、Alist 客户端、STRM 执行器和 cron 调度组装在一起。
package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

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
	a.runner = strm.New(alist.New(cfg.Alist.BaseURL, cfg.Alist.Token, cfg.Alist.WaitTime))

	for _, id := range a.entries {
		a.sched.Remove(id)
	}
	a.entries = map[string]cron.EntryID{}
	for _, t := range cfg.Tasks {
		if t.Cron == "" {
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

	// 重建变动监控。
	a.watchMu.Lock()
	for _, cancel := range a.watchCancels {
		cancel()
	}
	a.watchCancels = map[string]context.CancelFunc{}
	for _, t := range cfg.Tasks {
		if t.WatchInterval <= 0 {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		a.watchCancels[t.ID] = cancel
		go a.watchTask(ctx, t, a.runner)
		a.log.Info("注册变动监控", "task", t.ID, "interval", t.WatchInterval, "mode", t.WatchMode)
	}
	a.watchMu.Unlock()
}

// watchTask 按间隔扫描远端目录指纹，变化时触发任务。首次扫描后立即执行一次。
func (a *App) watchTask(ctx context.Context, task config.TaskConfig, runner *strm.Runner) {
	log := a.log.With("task", task.ID, "watch", task.WatchMode)
	interval := time.Duration(task.WatchInterval) * time.Second

	// 按监控方式选择探测函数：递归指纹（本地存储）或目录计数（网盘存储）。
	probe := runner.Fingerprint
	if task.WatchMode == config.WatchDirCount {
		probe = runner.DirCount
	}

	var last uint64
	first := true
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
func TestAlist(ctx context.Context, baseURL, token string) error {
	c := alist.New(baseURL, token, 0)
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
