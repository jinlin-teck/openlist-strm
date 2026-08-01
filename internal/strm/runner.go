// Package strm 实现从 OpenList 目录扫描并生成 .strm 文件的核心逻辑。
package strm

import (
	"context"
	"fmt"
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
	Scanned         int `json:"scanned"`          // 扫描到的视频文件数
	Created         int `json:"created"`          // 新生成/覆盖的 strm 数
	Skipped         int `json:"skipped"`          // 已存在且未覆盖而跳过的 strm 数
	Deleted         int `json:"deleted"`          // 同步删除的本地 strm 数
	Downloaded      int `json:"downloaded"`       // 下载的伴生文件数
	Failed          int `json:"failed"`           // 处理失败的文件数
	BaselineSkipped int `json:"baseline_skipped"` // only_new 任务被基线过滤跳过的文件数
}

// Runner 执行 STRM 生成任务。
type Runner struct {
	client *alist.Client
	hc     *http.Client
}

func New(client *alist.Client) *Runner {
	return &Runner{client: client, hc: &http.Client{Timeout: 10 * time.Minute}}
}

// 跳过扫描的系统文件/目录（键全小写，比较时对文件名做 ToLower）。
var skipNames = map[string]bool{
	"@eadir":    true,
	"thumbs.db": true,
	".ds_store": true,
}

// Run 执行一次全量任务，返回统计与本次扫描到的完整快照条目（"路径:大小"，已排序）。
// baseline 非空时启用 only_new 基线过滤：基线内且本地文件不存在的远端文件直接跳过；
// 基线内但本地已有产物的文件照常处理（计入 generated，保护 sync_delete 不误删）。
// ctx 取消时尽快退出。
func (r *Runner) Run(ctx context.Context, task config.TaskConfig, baseline []string, log *slog.Logger) (*Stats, []string, error) {
	stats := &Stats{}

	// path_replace / alist_url 模式与伴生文件下载都需要用户 base_path 来拼下载地址。
	basePath := ""
	if task.Mode == config.ModeAlistURL || task.Mode == config.ModePathReplace || task.Download.Enable {
		bp, err := r.client.Me(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("获取用户信息失败: %w", err)
		}
		basePath = bp
	}

	if err := os.MkdirAll(task.TargetDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("创建目标目录失败: %w", err)
	}

	exts := map[string]bool{}
	for _, e := range task.Exts() {
		exts[e] = true
	}
	downloadExts := task.DownloadExts()

	// only_new 基线集合；nil 表示不过滤。
	var baselineSet map[string]bool
	if baseline != nil {
		baselineSet = make(map[string]bool, len(baseline))
		for _, e := range baseline {
			baselineSet[e] = true
		}
	}

	sem := make(chan struct{}, task.Concurrency)
	dlSem := make(chan struct{}, max(task.Download.Concurrency, 1))
	var wg sync.WaitGroup
	var mu sync.Mutex
	// 本次扫描生成的全部 strm 的本地绝对路径，用于同步删除。
	generated := map[string]bool{}
	// 与 strm 生成相关的失败数（列目录失败、生成失败；不含伴生下载失败）。
	// 非零时跳过同步删除，防止部分失败被误判为「远端已删除」而误清空本地。
	var scanFailed int
	// 本次扫描到的全部受管文件条目（"路径:大小"），供调用方更新树快照。
	var entries []string

	// path_replace 的 url_prefix 若与下载基址（{base_url}/d）没有任何前缀关系，
	// 则每个文件都必然失败，扫描前直接报错，避免配置错误被放大成逐文件失败。
	if task.Mode == config.ModePathReplace {
		dlBase := r.client.BaseURL() + "/d"
		if !strings.HasPrefix(task.URLPrefix, dlBase) && !strings.HasPrefix(dlBase, task.URLPrefix) {
			return nil, nil, fmt.Errorf("url_prefix %q 与下载基址 %q 不匹配，请检查配置", task.URLPrefix, dlBase)
		}
	}

	// 显式栈 DFS 遍历目录，避免深递归。
	stack := []string{task.SourceDir}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return stats, nil, err
		}
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// refresh=true：手动/cron 触发的全量运行必须看到上游最新状态，
		// 否则 OpenList 缓存未过期时新增文件扫不出来（only_new 任务尤其致命，
		// 它要处理的正是基线之后新增的文件）；监控增量路径不受影响（探测已刷新）。
		items, err := r.client.List(ctx, dir, true)
		if err != nil {
			log.Warn("列目录失败，跳过", "dir", dir, "err", err)
			mu.Lock()
			stats.Failed++
			scanFailed++
			mu.Unlock()
			continue
		}
		for _, it := range items {
			if skipNames[strings.ToLower(it.Name)] {
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
					entry := fmt.Sprintf("%s:%d", full, it.Size)
					mu.Lock()
					entries = append(entries, entry)
					mu.Unlock()
					// only_new：基线内且本地不存在的伴生文件不下载。
					if baselineSet != nil && baselineSet[entry] {
						if local, err := relLocalPath(task, full); err != nil || !fileExists(local) {
							mu.Lock()
							stats.BaselineSkipped++
							mu.Unlock()
							continue
						}
					}
					wg.Add(1)
					select {
					case dlSem <- struct{}{}:
					case <-ctx.Done():
						wg.Done()
						continue
					}
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
			entry := fmt.Sprintf("%s:%d", full, it.Size)
			mu.Lock()
			entries = append(entries, entry)
			mu.Unlock()
			// only_new：基线内且本地 strm 不存在的存量文件跳过；
			// 本地已有 strm 的照常处理（计入 generated，防止 sync_delete 误删）。
			if baselineSet != nil && baselineSet[entry] {
				if local, err := localStrmPath(task, full); err != nil || !fileExists(local) {
					mu.Lock()
					stats.BaselineSkipped++
					mu.Unlock()
					continue
				}
			}
			mu.Lock()
			stats.Scanned++
			mu.Unlock()

			wg.Add(1)
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Done()
				continue
			}
			go func(item alist.FsItem, remotePath string) {
				defer wg.Done()
				defer func() { <-sem }()
				local, ok, err := r.processOne(ctx, task, basePath, item, remotePath, log)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err != nil:
					stats.Failed++
					scanFailed++
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

	// 存在任何与生成相关的失败（列目录失败/文件处理失败）时跳过同步删除：
	// 失败文件不在 generated 集合中，照旧删除会把「远端仍存在」的本地 strm 误清掉。
	if task.SyncDelete {
		if scanFailed > 0 {
			log.Warn("本次运行存在失败项，已跳过同步删除以保护数据", "failed", scanFailed)
		} else {
			deleted, err := syncDelete(task.TargetDir, generated, stats.Scanned, log)
			if err != nil {
				log.Warn("同步删除失败", "err", err)
			} else {
				stats.Deleted = deleted
			}
		}
	}
	sort.Strings(entries)
	return stats, entries, nil
}

// processOne 处理单个视频文件。返回本地 strm 路径与是否新写入。
func (r *Runner) processOne(ctx context.Context, task config.TaskConfig, basePath string, item alist.FsItem, remotePath string, log *slog.Logger) (string, bool, error) {
	local, err := localStrmPath(task, remotePath)
	if err != nil {
		return "", false, err
	}

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
	// 原子写：先写临时文件再改名，避免进程中断留下截断的 strm 被 overwrite=false 永久跳过。
	if err := writeFileAtomic(local, []byte(content), 0o644); err != nil {
		return local, false, err
	}
	log.Debug("生成 strm", "local", local, "content", stripQuery(content))
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
		u := r.downloadURL(task, basePath, item, remotePath, true, true)
		// 配置了 public_url 时，把直链域名替换为公网地址（签名与路径绑定，换域名仍有效）。
		if task.PublicURL != "" {
			u = strings.Replace(u, r.client.BaseURL(), task.PublicURL, 1)
		}
		return u, nil
	case config.ModePathReplace:
		raw := r.downloadURL(task, basePath, item, remotePath, task.WithSign, task.EncodeEnabled())
		if !strings.HasPrefix(raw, task.URLPrefix) {
			return "", fmt.Errorf("URL %q 不包含前缀 %q", stripQuery(raw), task.URLPrefix)
		}
		return task.PrefixTo + strings.TrimPrefix(raw, task.URLPrefix), nil
	}
	return "", fmt.Errorf("未知 mode %q", task.Mode)
}

// Snapshot 递归扫描任务源目录，返回全部受管文件（视频 + 伴生）的
// 排序 "路径:大小" 条目列表，作为 tree_diff 监控的变更对比基准与 only_new 任务的基线。
// 使用 refresh 绕过服务端缓存。
func (r *Runner) Snapshot(ctx context.Context, task config.TaskConfig) ([]string, error) {
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
			return nil, err
		}
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		items, err := r.client.List(ctx, dir, true)
		if err != nil {
			return nil, fmt.Errorf("列目录 %s 失败: %w", dir, err)
		}
		for _, it := range items {
			if skipNames[strings.ToLower(it.Name)] {
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
	return entries, nil
}

// DiffSnapshots 对比两个有序快照（"路径:大小" 条目），返回新增与消失的条目。
// 同路径大小变化会同时出现在 removed 与 added 中（视为先消失后新增，会重新生成）。
func DiffSnapshots(old, new []string) (added, removed []string) {
	i, j := 0, 0
	for i < len(old) && j < len(new) {
		switch {
		case old[i] == new[j]:
			i++
			j++
		case old[i] < new[j]:
			removed = append(removed, old[i])
			i++
		default:
			added = append(added, new[j])
			j++
		}
	}
	removed = append(removed, old[i:]...)
	added = append(added, new[j:]...)
	return added, removed
}

// downloadOne 下载伴生文件到目标目录同名位置。返回是否真正写入。
func (r *Runner) downloadOne(ctx context.Context, task config.TaskConfig, basePath string, item alist.FsItem, remotePath string, log *slog.Logger) (bool, error) {
	local, err := relLocalPath(task, remotePath)
	if err != nil {
		return false, err
	}

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
	// 网盘（如 115）的下载签名与 UA 绑定，取链与跳转后的下载必须带一致的 UA，
	// 且 Go 默认 UA 会被 115 拒绝，必须使用配置的用户代理。
	if ua := r.client.UserAgent(); ua != "" {
		req.Header.Set("User-Agent", ua)
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
	// 写临时文件成功后再改名，失败不留半截文件；限制最大 512MB 防异常响应写爆磁盘。
	tmp := local + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return false, err
	}
	n, err := io.Copy(f, io.LimitReader(resp.Body, 512<<20))
	if err == nil && item.Size > 0 && n != item.Size {
		err = fmt.Errorf("下载大小不符: 期望 %d 字节，实际 %d 字节", item.Size, n)
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	if err := os.Rename(tmp, local); err != nil {
		_ = os.Remove(tmp)
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

// isPathTraversal 判断 filepath.Rel 算出的相对路径是否越出根目录。
// 精确匹配 ".." 或 "../" 前缀，避免误伤 "..foo" 之类的合法文件名。
func isPathTraversal(rel string) bool {
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// relLocalPath 计算远端文件在目标目录下的本地路径（与远端相对结构一致）。
func relLocalPath(task config.TaskConfig, remotePath string) (string, error) {
	rel, err := filepath.Rel(filepath.FromSlash(task.SourceDir), filepath.FromSlash(remotePath))
	if err != nil || isPathTraversal(rel) {
		return "", fmt.Errorf("非法相对路径 %q", remotePath)
	}
	return filepath.Join(task.TargetDir, rel), nil
}

// localStrmPath 计算远端视频文件对应的本地 .strm 路径。
func localStrmPath(task config.TaskConfig, remotePath string) (string, error) {
	local, err := relLocalPath(task, remotePath)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(local, filepath.Ext(local)) + ".strm", nil
}

// fileExists 判断本地文件是否存在。
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// entryPath 从 "路径:大小" 快照条目中提取路径（按最后一个冒号分隔，兼容含冒号的文件名）。
func entryPath(entry string) string {
	if i := strings.LastIndex(entry, ":"); i >= 0 {
		return entry[:i]
	}
	return entry
}

// writeFileAtomic 先写临时文件再改名，避免进程中断留下截断文件。
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// stripQuery 去掉 URL 查询串（含 ?sign= 临时签名），避免签名进入日志。
func stripQuery(s string) string {
	if i := strings.IndexByte(s, '?'); i >= 0 {
		return s[:i] + "?..."
	}
	return s
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

// RunIncremental 依据 tree_diff 的 diff 结果增量执行任务：
// added 中的新增/变化文件生成 strm（或下载伴生），removed 中消失的视频文件
// 在 sync_delete 开启时删除本地 strm 并清理空目录（伴生文件不删除，与全量行为一致）。
// oldTotal 为旧快照条目总数，用于「全部消失」护栏：added 为空且 removed 覆盖全部
// 旧条目时判定为远端异常，拒绝删除。
func (r *Runner) RunIncremental(ctx context.Context, task config.TaskConfig, added, removed []string, oldTotal int, log *slog.Logger) (*Stats, error) {
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

	// 解析 added 条目，按父目录分组，每组一次 List 拿到 FsItem（含签名）。
	// 探测刚用 refresh=true 拉取过，这里 refresh=false 命中服务端缓存。
	byDir := map[string][]string{}
	for _, e := range added {
		full := entryPath(e)
		dir, name := path.Split(full)
		dir = strings.TrimRight(dir, "/")
		if dir == "" {
			dir = "/"
		}
		byDir[dir] = append(byDir[dir], name)
	}

	sem := make(chan struct{}, task.Concurrency)
	dlSem := make(chan struct{}, max(task.Download.Concurrency, 1))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for dir, names := range byDir {
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return stats, err
		}
		items, err := r.client.List(ctx, dir, false)
		if err != nil {
			log.Warn("列目录失败，跳过该目录的新增文件", "dir", dir, "err", err)
			mu.Lock()
			stats.Failed += len(names)
			mu.Unlock()
			continue
		}
		found := map[string]alist.FsItem{}
		for _, it := range items {
			if !it.IsDir {
				found[it.Name] = it
			}
		}
		for _, name := range names {
			it, ok := found[name]
			if !ok {
				// 服务端缓存未刷新时可能暂时取不到，记失败；快照不更新，下轮探测重试。
				log.Warn("新增文件在目录列表中未找到，将在下次探测时重试", "dir", dir, "name", name)
				mu.Lock()
				stats.Failed++
				mu.Unlock()
				continue
			}
			full := joinPath(dir, name)
			ext := strings.ToLower(path.Ext(name))
			switch {
			case exts[ext]:
				mu.Lock()
				stats.Scanned++
				mu.Unlock()
				wg.Add(1)
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					wg.Done()
					continue
				}
				go func(item alist.FsItem, remotePath string) {
					defer wg.Done()
					defer func() { <-sem }()
					_, ok, err := r.processOne(ctx, task, basePath, item, remotePath, log)
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err != nil:
						stats.Failed++
						log.Warn("生成 strm 失败", "path", remotePath, "err", err)
					case ok:
						stats.Created++
					default:
						stats.Skipped++
					}
				}(it, full)
			case downloadExts[ext]:
				wg.Add(1)
				select {
				case dlSem <- struct{}{}:
				case <-ctx.Done():
					wg.Done()
					continue
				}
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
			default:
				log.Debug("忽略不在受管后缀内的新增文件", "path", full)
			}
		}
	}
	wg.Wait()

	// removed：sync_delete 开启时删除本地对应 strm；关闭时仅记录。
	// 存在任何失败项（列目录/生成/下载失败）时跳过删除：
	// 与全量路径的 scanFailed 护栏同理，「同名替换」（removed 旧 + added 新）场景下
	// 新文件生成失败时照旧删除会把用户仍需要的旧 strm 误清掉。
	if len(removed) > 0 {
		switch {
		case !task.SyncDelete:
			log.Info("检测到远端文件消失（sync_delete 关闭，仅记录）", "removed", len(removed))
		case stats.Failed > 0:
			log.Warn("本次运行存在失败项，已跳过同步删除以保护数据", "failed", stats.Failed)
		case len(added) == 0 && oldTotal > 0 && len(removed) >= oldTotal:
			log.Warn("远端文件全部消失，疑似远端异常，已跳过同步删除以保护数据", "removed", len(removed))
		default:
			for _, e := range removed {
				full := entryPath(e)
				if !exts[strings.ToLower(path.Ext(full))] {
					continue // 伴生文件不删除（与全量 sync_delete 行为一致）
				}
				local, err := localStrmPath(task, full)
				if err != nil {
					log.Warn("路径映射失败，跳过删除", "path", full, "err", err)
					mu.Lock()
					stats.Failed++
					mu.Unlock()
					continue
				}
				if err := os.Remove(local); err == nil {
					stats.Deleted++
					log.Info("删除多余 strm", "path", local)
				} else if !os.IsNotExist(err) {
					log.Warn("删除失败", "path", local, "err", err)
					mu.Lock()
					stats.Failed++
					mu.Unlock()
				}
			}
			cleanEmptyDirs(task.TargetDir, log)
		}
	}
	return stats, nil
}
