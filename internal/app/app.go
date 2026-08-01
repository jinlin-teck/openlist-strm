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

	updateMu sync.Mutex // 串行化配置更新（落盘 + 热加载），防止并发 PUT 导致磁盘与内存配置不一致

	sched   *cron.Cron
	entries map[string]cron.EntryID

	watchMu      sync.Mutex
	watchCancels map[string]context.CancelFunc
	watchKeys    map[string]uint64 // 各任务监控配置的指纹，用于热加载时判断是否需要重启监控
	state        *watchState       // 持久化的上次监控指纹，避免重启后误触发全量扫描
	snapshots    *snapshotStore    // 各任务的树快照（tree_diff 对比基准 + only_new 基线）

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
		snapshots:    newSnapshotStore(filepath.Join(filepath.Dir(cfgPath), "watch-snapshots"), log),
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
		a.log.Info("注册变动监控", "task", t.ID, "interval", t.WatchInterval)
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
	// 树快照的保留范围：监控中的任务 ∪ 开启 only_new 的任务（后者无监控也需要基线）。
	snapKeep := make(map[string]bool, len(newKeys))
	for id := range newKeys {
		snapKeep[id] = true
	}
	for _, t := range cfg.Tasks {
		if t.OnlyNew {
			snapKeep[t.ID] = true
		}
	}
	a.snapshots.Prune(snapKeep)
	a.watchMu.Unlock()
}

// watchKey 计算任务监控配置的指纹（含 Alist 连接配置），用于热加载时判断监控 goroutine 是否需要重启。
// 只纳入影响扫描/取链行为的字段：修改名称、cron、并发数等无关配置不会重启监控，也就不会触发重跑。
func watchKey(alistCfg config.AlistConfig, t config.TaskConfig) uint64 {
	t.Name = ""
	t.Cron = ""
	t.Concurrency = 0
	data, _ := yaml.Marshal(struct {
		Alist config.AlistConfig `yaml:"alist"`
		Task  config.TaskConfig  `yaml:"task"`
	}{alistCfg, t})
	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64()
}

// watchTask 按间隔对源目录做树快照（tree_diff），与上次快照 diff 出新增/消失明细后
// 增量触发任务。无持久化快照的监控（新建/配置变更）启动后立即探测一次：
// only_new 任务首次探测仅建立基线（存量不生成），其他任务首次探测触发一次全量运行；
// 重启后若快照 hash 无变化则不触发。
func (a *App) watchTask(ctx context.Context, task config.TaskConfig, runner *strm.Runner, key uint64) {
	log := a.log.With("task", task.ID, "watch", config.WatchTreeDiff)
	interval := time.Duration(task.WatchInterval) * time.Second

	check := func() {
		entries, err := runner.Snapshot(ctx, task)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("变更探测失败", "err", err)
			}
			return
		}
		fp := hashEntries(entries)
		last, hasLast := a.state.Get(task.ID, key)

		if hasLast && fp == last {
			return // 快照无变化
		}

		if !hasLast {
			// 首次探测：only_new 只建基线，其他任务触发一次全量运行。
			if task.OnlyNew {
				a.snapshots.Save(task.ID, key, entries)
				a.state.Set(task.ID, key, fp)
				log.Info("已建立基线快照，后续仅处理新增/变化的文件", "files", len(entries))
				return
			}
			log.Info("首次探测，触发任务", "files", len(entries))
			if _, err := a.RunTask(ctx, task.ID); err != nil {
				if ctx.Err() == nil {
					log.Warn("监控触发任务失败，将在下次探测时重试", "err", err)
				}
				return // 快照不落盘，保留下轮重试机会
			}
			a.snapshots.Save(task.ID, key, entries)
			a.state.Set(task.ID, key, fp)
			return
		}

		// 有变化：加载旧快照做 diff。旧快照缺失/损坏时退化为全量运行。
		old, ok := a.snapshots.Load(task.ID, key)
		if !ok {
			log.Warn("旧树快照缺失或损坏，退化为全量运行")
			if _, err := a.RunTask(ctx, task.ID); err != nil {
				if ctx.Err() == nil {
					log.Warn("监控触发任务失败，将在下次探测时重试", "err", err)
				}
				return
			}
			a.snapshots.Save(task.ID, key, entries)
			a.state.Set(task.ID, key, fp)
			return
		}
		// 空快照保护：远端突然为空而旧快照非空，判定为远端异常（如网盘挂载掉线），
		// 跳过本次变更，防止误判为「全部删除」而清空本地。
		if len(entries) == 0 && len(old) > 0 {
			log.Warn("探测到远端为空而旧快照非空，疑似远端异常，已跳过本次变更")
			return
		}
		added, removed := strm.DiffSnapshots(old, entries)
		log.Info("检测到文件变化，触发增量任务", "added", len(added), "removed", len(removed))
		if _, err := a.RunTaskIncremental(ctx, task.ID, added, removed, len(old)); err != nil {
			if ctx.Err() == nil {
				log.Warn("监控触发任务失败，将在下次探测时重试", "err", err)
			}
			return // 快照不落盘，保留下轮重试机会
		}
		// 任务成功后才更新快照与指纹：失败时保持旧值，下轮探测会再次触发。
		a.snapshots.Save(task.ID, key, entries)
		a.state.Set(task.ID, key, fp)
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

// Config 返回当前配置副本。注意是浅拷贝（与 live 配置共享底层切片），调用方只读使用，不得修改。
func (a *App) Config() config.Config {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return *a.cfg
}

// UpdateConfig 校验、落盘并热加载新配置。整体串行化，防止并发更新交错导致磁盘与内存配置不一致。
func (a *App) UpdateConfig(cfg *config.Config) error {
	a.updateMu.Lock()
	defer a.updateMu.Unlock()
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

// RunTask 同步执行一次全量任务；同一任务并发执行会被拒绝。
// only_new 任务无有效基线快照时，本次仅建立基线，不生成任何文件。
func (a *App) RunTask(ctx context.Context, taskID string) (*strm.Stats, error) {
	a.mu.RLock()
	cfg := a.cfg
	runner := a.runner
	a.mu.RUnlock()

	t := cfg.Task(taskID)
	if t == nil {
		return nil, fmt.Errorf("任务 %q 不存在", taskID)
	}
	key := watchKey(cfg.Alist, *t)

	return a.executeTask(ctx, taskID, func(ctx context.Context, task config.TaskConfig, log *slog.Logger) (*strm.Stats, error) {
		var baseline []string
		if task.OnlyNew {
			if old, ok := a.snapshots.Load(taskID, key); ok {
				baseline = old
			} else {
				// 无基线：本次仅建立基线快照，存量不生成 strm。
				entries, err := runner.Snapshot(ctx, task)
				if err != nil {
					return nil, fmt.Errorf("建立基线快照失败: %w", err)
				}
				a.snapshots.Save(taskID, key, entries)
				log.Info("已建立基线快照，后续仅处理新增/变化的文件", "files", len(entries))
				return &strm.Stats{Scanned: len(entries)}, nil
			}
		}
		log.Info("任务开始", "source", task.SourceDir, "target", task.TargetDir, "mode", task.Mode)
		stats, entries, err := runner.Run(ctx, task, baseline, log)
		if err != nil {
			return stats, err
		}
		// 任务成功后才更新基线快照：失败保留旧值，失败的文件下轮仍会被处理。
		if task.OnlyNew {
			a.snapshots.Save(taskID, key, entries)
		}
		return stats, nil
	})
}

// RunTaskIncremental 依据 tree_diff 的 diff 结果同步执行一次增量任务；
// 同一任务并发执行会被拒绝。
func (a *App) RunTaskIncremental(ctx context.Context, taskID string, added, removed []string, oldTotal int) (*strm.Stats, error) {
	a.mu.RLock()
	runner := a.runner
	a.mu.RUnlock()

	return a.executeTask(ctx, taskID, func(ctx context.Context, task config.TaskConfig, log *slog.Logger) (*strm.Stats, error) {
		log.Info("增量任务开始", "added", len(added), "removed", len(removed), "mode", task.Mode)
		return runner.RunIncremental(ctx, task, added, removed, oldTotal, log)
	})
}

// executeTask 是 RunTask / RunTaskIncremental 的共用骨架：
// 校验任务存在与启用、同任务并发互斥、运行状态记录与日志。
func (a *App) executeTask(ctx context.Context, taskID string, run func(ctx context.Context, task config.TaskConfig, log *slog.Logger) (*strm.Stats, error)) (*strm.Stats, error) {
	if err := ctx.Err(); err != nil { // 热加载取消的旧监控可能带着已取消的 ctx 进入，直接拒绝
		return nil, err
	}
	a.mu.RLock()
	cfg := a.cfg
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
	stats, err := run(ctx, task, log)

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
			"skipped", stats.Skipped, "deleted", stats.Deleted, "downloaded", stats.Downloaded,
			"failed", stats.Failed, "baseline_skipped", stats.BaselineSkipped)
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
