// Package strm 实现从 OpenList 目录扫描并生成 .strm 文件的核心逻辑。
package strm

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"openlist-strm/internal/alist"
	"openlist-strm/internal/config"
)

// Stats 是一次任务运行的统计结果。
type Stats struct {
	Scanned    int `json:"scanned"`    // 扫描到的视频文件数
	Created    int `json:"created"`    // 新生成/覆盖的 strm 数
	Skipped    int `json:"skipped"`    // 已存在且未覆盖而跳过的 strm 数
	Deleted    int `json:"deleted"`    // 同步删除的本地 strm 数
	Downloaded int `json:"downloaded"` // 下载的伴生文件数
	Failed     int `json:"failed"`     // 处理失败的文件数
}

// Runner 执行 STRM 生成任务。
type Runner struct {
	client *alist.Client
	hc     *http.Client
}

func New(client *alist.Client) *Runner {
	return &Runner{client: client, hc: &http.Client{Timeout: 10 * time.Minute}}
}

// 跳过扫描的系统文件/目录。
var skipNames = map[string]bool{
	"@eaDir":    true,
	"Thumbs.db": true,
	".DS_Store": true,
}

// Run 执行一次任务，返回统计。ctx 取消时尽快退出。
func (r *Runner) Run(ctx context.Context, task config.TaskConfig, log *slog.Logger) (*Stats, error) {
	stats := &Stats{}

	// path_replace / alist_url 模式与伴生文件下载都需要用户 base_path 来拼下载地址。
	basePath := ""
	if task.Mode == config.ModeAlistURL || task.Mode == config.ModePathReplace || task.Download.Enable {
		bp, err := r.client.Me(ctx)
		if err != nil {
			return nil, fmt.Errorf("获取用户信息失败: %w", err)
		}
		basePath = bp
	}

	if err := os.MkdirAll(task.TargetDir, 0o755); err != nil {
		return nil, fmt.Errorf("创建目标目录失败: %w", err)
	}

	exts := map[string]bool{}
	for _, e := range task.Exts() {
		exts[e] = true
	}
	downloadExts := task.DownloadExts()

	sem := make(chan struct{}, task.Concurrency)
	dlSem := make(chan struct{}, max(task.Download.Concurrency, 1))
	var wg sync.WaitGroup
	var mu sync.Mutex
	// 本次扫描生成的全部 strm 的本地绝对路径，用于同步删除。
	generated := map[string]bool{}

	// 显式栈 DFS 遍历目录，避免深递归。
	stack := []string{task.SourceDir}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return stats, err
		}
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		items, err := r.client.List(ctx, dir, false)
		if err != nil {
			log.Warn("列目录失败，跳过", "dir", dir, "err", err)
			mu.Lock()
			stats.Failed++
			mu.Unlock()
			continue
		}
		for _, it := range items {
			if skipNames[it.Name] {
				continue
			}
			full := joinPath(dir, it.Name)
			if it.IsDir {
				stack = append(stack, full)
				continue
			}
			ext := strings.ToLower(path.Ext(it.Name))
			if !exts[ext] {
				// 非视频文件：按需作为伴生文件下载。
				if downloadExts[ext] {
					wg.Add(1)
					dlSem <- struct{}{}
					go func(item alist.FsItem, remotePath string) {
						defer wg.Done()
						defer func() { <-dlSem }()
						downloaded, err := r.downloadOne(ctx, task, basePath, item, remotePath, log)
						mu.Lock()
						defer mu.Unlock()
						switch {
						case err != nil:
							stats.Failed++
							log.Warn("下载伴生文件失败", "path", remotePath, "err", err)
						case downloaded:
							stats.Downloaded++
						}
					}(it, full)
				}
				continue
			}
			mu.Lock()
			stats.Scanned++
			mu.Unlock()

			wg.Add(1)
			sem <- struct{}{}
			go func(item alist.FsItem, remotePath string) {
				defer wg.Done()
				defer func() { <-sem }()
				local, ok, err := r.processOne(ctx, task, basePath, item, remotePath, log)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err != nil:
					stats.Failed++
					log.Warn("生成 strm 失败", "path", remotePath, "err", err)
				case ok:
					stats.Created++
					generated[local] = true
				default:
					stats.Skipped++
					generated[local] = true
				}
			}(it, full)
		}
	}
	wg.Wait()

	if task.SyncDelete {
		deleted, err := syncDelete(task.TargetDir, generated, stats.Scanned, log)
		if err != nil {
			log.Warn("同步删除失败", "err", err)
		} else {
			stats.Deleted = deleted
		}
	}
	return stats, nil
}

// processOne 处理单个视频文件。返回本地 strm 路径与是否新写入。
func (r *Runner) processOne(ctx context.Context, task config.TaskConfig, basePath string, item alist.FsItem, remotePath string, log *slog.Logger) (string, bool, error) {
	rel, err := filepath.Rel(task.SourceDir, filepath.FromSlash(remotePath))
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false, fmt.Errorf("非法相对路径 %q", remotePath)
	}
	local := filepath.Join(task.TargetDir, rel)
	local = strings.TrimSuffix(local, filepath.Ext(local)) + ".strm"

	if !task.Overwrite {
		if _, err := os.Stat(local); err == nil {
			return local, false, nil
		}
	}

	content, err := r.strmContent(ctx, task, basePath, item, remotePath)
	if err != nil {
		return local, false, err
	}

	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return local, false, err
	}
	if err := os.WriteFile(local, []byte(content), 0o644); err != nil {
		return local, false, err
	}
	log.Debug("生成 strm", "local", local, "content", content)
	return local, true, nil
}

// strmContent 按模式计算 .strm 文件内容。
func (r *Runner) strmContent(ctx context.Context, task config.TaskConfig, basePath string, item alist.FsItem, remotePath string) (string, error) {
	switch task.Mode {
	case config.ModeAlistPath:
		return remotePath, nil
	case config.ModeRawURL:
		return r.client.RawURL(ctx, remotePath)
	case config.ModeAlistURL:
		return r.downloadURL(task, basePath, item, remotePath, true, true), nil
	case config.ModePathReplace:
		raw := r.downloadURL(task, basePath, item, remotePath, false, task.EncodeEnabled())
		if !strings.HasPrefix(raw, task.URLPrefix) {
			return "", fmt.Errorf("URL %q 不包含前缀 %q", raw, task.URLPrefix)
		}
		return task.PrefixTo + strings.TrimPrefix(raw, task.URLPrefix), nil
	}
	return "", fmt.Errorf("未知 mode %q", task.Mode)
}

// Fingerprint 递归扫描任务源目录，对全部受管文件（视频 + 伴生）的
// "路径:大小" 计算 FNV-1a 指纹，用于变更检测。使用 refresh 绕过服务端缓存。
func (r *Runner) Fingerprint(ctx context.Context, task config.TaskConfig) (uint64, error) {
	exts := map[string]bool{}
	for _, e := range task.Exts() {
		exts[e] = true
	}
	for e := range task.DownloadExts() {
		exts[e] = true
	}

	var entries []string
	stack := []string{task.SourceDir}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		items, err := r.client.List(ctx, dir, true)
		if err != nil {
			return 0, fmt.Errorf("列目录 %s 失败: %w", dir, err)
		}
		for _, it := range items {
			if skipNames[it.Name] {
				continue
			}
			full := joinPath(dir, it.Name)
			if it.IsDir {
				stack = append(stack, full)
				continue
			}
			if exts[strings.ToLower(path.Ext(it.Name))] {
				entries = append(entries, fmt.Sprintf("%s:%d", full, it.Size))
			}
		}
	}
	sort.Strings(entries)
	h := fnv.New64a()
	for _, e := range entries {
		h.Write([]byte(e))
		h.Write([]byte{0})
	}
	return h.Sum64(), nil
}

// DirCount 返回源目录直属子项数量，作为轻量变更指纹（dir_count 监控方式）。
// 无法检出目录内部的新增/删除，但每次轮询只需 1 次 API 调用，适合网盘存储。
func (r *Runner) DirCount(ctx context.Context, task config.TaskConfig) (uint64, error) {
	total, err := r.client.Total(ctx, task.SourceDir, true)
	if err != nil {
		return 0, fmt.Errorf("列目录 %s 失败: %w", task.SourceDir, err)
	}
	return uint64(total), nil
}

// downloadOne 下载伴生文件到目标目录同名位置。返回是否真正写入。
func (r *Runner) downloadOne(ctx context.Context, task config.TaskConfig, basePath string, item alist.FsItem, remotePath string, log *slog.Logger) (bool, error) {
	rel, err := filepath.Rel(task.SourceDir, filepath.FromSlash(remotePath))
	if err != nil || strings.HasPrefix(rel, "..") {
		return false, fmt.Errorf("非法相对路径 %q", remotePath)
	}
	local := filepath.Join(task.TargetDir, rel)

	if !task.Overwrite {
		if fi, err := os.Stat(local); err == nil && fi.Size() == item.Size {
			return false, nil
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		r.downloadURL(task, basePath, item, remotePath, true, true), nil)
	if err != nil {
		return false, err
	}
	resp, err := r.hc.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return false, err
	}
	f, err := os.Create(local)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return false, err
	}
	log.Debug("下载伴生文件", "local", local)
	return true, nil
}

// downloadURL 拼接 {base_url}/d{base_path}{full_path}，withSign 时附加签名参数，
// encode 为 false 时路径不做 URL 编码（用于生成明文 Linux 路径）。
func (r *Runner) downloadURL(task config.TaskConfig, basePath string, item alist.FsItem, remotePath string, withSign, encode bool) string {
	abs := strings.TrimRight(basePath, "/") + ensureSlash(remotePath)
	if encode {
		abs = encodePath(abs)
	}
	u := r.client.BaseURL() + "/d" + abs
	if withSign && item.Sign != "" {
		u += "?sign=" + url.QueryEscape(item.Sign)
	}
	return u
}

// encodePath 按段 URL 编码路径，保留 "/" 分隔符。
func encodePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

func joinPath(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return strings.TrimRight(dir, "/") + "/" + name
}

func ensureSlash(p string) string {
	if !strings.HasPrefix(p, "/") {
		return "/" + p
	}
	return p
}

// syncDelete 删除 targetDir 下不在 generated 集合中的 .strm 文件并清理空目录。
// scanned==0 时拒绝删除，防止 Alist 异常导致误清空。
func syncDelete(targetDir string, generated map[string]bool, scanned int, log *slog.Logger) (int, error) {
	if scanned == 0 {
		return 0, fmt.Errorf("本次扫描到 0 个视频文件，已跳过删除以保护数据")
	}
	deleted := 0
	err := filepath.WalkDir(targetDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.ToLower(filepath.Ext(p)) != ".strm" {
			return nil
		}
		if !generated[p] {
			if err := os.Remove(p); err != nil {
				log.Warn("删除失败", "path", p, "err", err)
				return nil
			}
			deleted++
			log.Info("删除多余 strm", "path", p)
		}
		return nil
	})
	if err != nil {
		return deleted, err
	}
	cleanEmptyDirs(targetDir, log)
	return deleted, nil
}

// cleanEmptyDirs 自底向上删除空目录（保留根目录）。
func cleanEmptyDirs(root string, log *slog.Logger) {
	var dirs []string
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() && p != root {
			dirs = append(dirs, p)
		}
		return nil
	})
	// 深目录先删。
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Remove(dirs[i]); err == nil {
			log.Debug("清理空目录", "dir", dirs[i])
		}
	}
}
