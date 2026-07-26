package config

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// Config 是应用的顶层配置。
type Config struct {
	Server ServerConfig `yaml:"server" json:"server"`
	Alist  AlistConfig  `yaml:"alist" json:"alist"`
	Tasks  []TaskConfig `yaml:"tasks" json:"tasks"`
}

type ServerConfig struct {
	Listen string `yaml:"listen" json:"listen"` // HTTP 监听地址，默认 :8080
}

// AlistConfig 描述 OpenList/Alist 服务器连接，仅支持令牌认证。
type AlistConfig struct {
	BaseURL   string `yaml:"base_url" json:"base_url"`     // 服务器地址，如 https://alist.example.com
	Token     string `yaml:"token" json:"token"`           // 永久令牌
	WaitTime  int    `yaml:"wait_time" json:"wait_time"`   // API 请求最小间隔，毫秒；0 表示不限速
	UserAgent string `yaml:"user_agent" json:"user_agent"` // HTTP User-Agent；115 等网盘会按 UA 校验下载签名，留空用默认
}

// DefaultUserAgent 默认 UA。Go 默认 UA (Go-http-client) 会被 115 网盘的
// 下载签名校验拒绝（403 invalid signature），因此默认使用浏览器 UA。
const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

// STRM 内容模式。
const (
	ModeAlistURL    = "alist_url"    // Alist 下载直链 {base_url}/d/...?sign=...
	ModeRawURL      = "raw_url"      // 上游存储真实直链（调用 /api/fs/get）
	ModeAlistPath   = "alist_path"   // Alist 内部路径
	ModePathReplace = "path_replace" // 替换 URL 前缀（可选 URL 编码），得到 Linux 路径；放在最后，供特殊需求使用
)

var validModes = map[string]bool{
	ModeAlistURL:    true,
	ModeRawURL:      true,
	ModeAlistPath:   true,
	ModePathReplace: true,
}

// cronParser 与 app 层调度器一致（6 段带秒），用于保存配置前预校验 cron 表达式，
// 避免非法表达式保存成功但任务静默不被注册。
var cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// 监控方式。
const (
	WatchFingerprint = "fingerprint" // 递归扫描全部文件算指纹，能检出深层文件变化，适合本地存储
	WatchDirCount    = "dir_count"   // 仅对比源目录直属子项数量，每次 1 次 API 调用，适合网盘存储
)

// TaskConfig 描述一个 STRM 生成任务。
type TaskConfig struct {
	ID            string         `yaml:"id" json:"id"`
	Name          string         `yaml:"name" json:"name"`
	Enabled       *bool          `yaml:"enabled" json:"enabled"`               // 是否启用，默认 true；禁用时定时/监控/手动均不运行
	Cron          string         `yaml:"cron" json:"cron"`                     // 6 段 cron（带秒），留空表示仅手动触发
	WatchInterval int            `yaml:"watch_interval" json:"watch_interval"` // 变动监控间隔（秒），0 关闭；检测到远端变化后自动生成
	WatchMode     string         `yaml:"watch_mode" json:"watch_mode"`         // 监控方式：fingerprint（递归指纹）/ dir_count（目录计数）
	SourceDir     string         `yaml:"source_dir" json:"source_dir"`
	TargetDir     string         `yaml:"target_dir" json:"target_dir"`
	Mode          string         `yaml:"mode" json:"mode"`
	PublicURL     string         `yaml:"public_url" json:"public_url"` // alist_url 模式：把直链域名替换为公网地址（内网 API 取链、公网播放）
	URLPrefix     string         `yaml:"url_prefix" json:"url_prefix"` // path_replace 模式：被替换的 URL 前缀
	PrefixTo      string         `yaml:"prefix_to" json:"prefix_to"`   // path_replace 模式：替换为（留空即仅去除前缀）
	URLEncode     *bool          `yaml:"url_encode" json:"url_encode"` // path_replace 模式：路径是否 URL 编码，默认 true
	WithSign      bool           `yaml:"with_sign" json:"with_sign"`   // path_replace 模式：URL 末尾是否附加签名（?sign=），默认 false；prefix_to 仍是 http(s) URL 时才应开启
	Overwrite     bool           `yaml:"overwrite" json:"overwrite"`
	Concurrency   int            `yaml:"concurrency" json:"concurrency"`
	VideoExts     []string       `yaml:"video_exts" json:"video_exts"`   // 留空使用默认视频后缀
	SyncDelete    bool           `yaml:"sync_delete" json:"sync_delete"` // 删除远端已不存在的本地 strm
	Download      DownloadConfig `yaml:"download" json:"download"`       // 伴生文件下载
}

// DownloadConfig 描述伴生文件（字幕/图片/NFO 等）下载。
type DownloadConfig struct {
	Enable      bool     `yaml:"enable" json:"enable"`
	Subtitle    bool     `yaml:"subtitle" json:"subtitle"`   // .ass .srt .ssa .sub
	Image       bool     `yaml:"image" json:"image"`         // .png .jpg .jpeg
	Nfo         bool     `yaml:"nfo" json:"nfo"`             // .nfo
	OtherExt    []string `yaml:"other_ext" json:"other_ext"` // 自定义后缀
	Concurrency int      `yaml:"concurrency" json:"concurrency"`
}

// DefaultVideoExts 是默认的视频文件后缀。
var DefaultVideoExts = []string{
	".mp4", ".mkv", ".flv", ".avi", ".wmv",
	".ts", ".rmvb", ".webm", ".mpg", ".m2ts", ".mov",
}

// 伴生文件默认后缀。
var (
	SubtitleExts = []string{".ass", ".srt", ".ssa", ".sub"}
	ImageExts    = []string{".png", ".jpg", ".jpeg"}
	NfoExts      = []string{".nfo"}
)

// IsEnabled 返回任务是否启用（默认启用）。
func (t *TaskConfig) IsEnabled() bool {
	return t.Enabled == nil || *t.Enabled
}

// EncodeEnabled 返回 path_replace 模式拼路径时是否做 URL 编码（默认开启，与原项目行为一致）。
func (t *TaskConfig) EncodeEnabled() bool {
	return t.URLEncode == nil || *t.URLEncode
}

// Exts 返回任务生效的视频后缀列表（统一小写、补点号）。
func (t *TaskConfig) Exts() []string {
	exts := t.VideoExts
	if len(exts) == 0 {
		exts = DefaultVideoExts
	}
	return normalizeExts(exts)
}

// DownloadExts 返回需要下载的伴生文件后缀集合。
func (t *TaskConfig) DownloadExts() map[string]bool {
	out := map[string]bool{}
	if !t.Download.Enable {
		return out
	}
	add := func(exts []string) {
		for _, e := range normalizeExts(exts) {
			out[e] = true
		}
	}
	if t.Download.Subtitle {
		add(SubtitleExts)
	}
	if t.Download.Image {
		add(ImageExts)
	}
	if t.Download.Nfo {
		add(NfoExts)
	}
	add(t.Download.OtherExt)
	return out
}

func normalizeExts(exts []string) []string {
	out := make([]string, 0, len(exts))
	for _, e := range exts {
		e = strings.ToLower(strings.TrimSpace(e))
		if e == "" {
			continue
		}
		if !strings.HasPrefix(e, ".") {
			e = "." + e
		}
		out = append(out, e)
	}
	return out
}

// pathOverlap 判断两个目录是否相同或互为祖先（用于 sync_delete 的 target_dir 冲突检测）。
func pathOverlap(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	if a == b {
		return true
	}
	sep := string(filepath.Separator)
	return strings.HasPrefix(a, b+sep) || strings.HasPrefix(b, a+sep)
}

// Validate 校验配置，返回第一个错误。
func (c *Config) Validate() error {
	if _, err := net.ResolveTCPAddr("tcp", c.Server.Listen); err != nil {
		return fmt.Errorf("server.listen 非法: %w", err)
	}
	if strings.TrimSpace(c.Alist.BaseURL) == "" {
		return fmt.Errorf("alist.base_url 不能为空")
	}
	if strings.TrimSpace(c.Alist.Token) == "" {
		return fmt.Errorf("alist.token 不能为空（仅支持令牌认证）")
	}
	seen := map[string]bool{}
	for i, t := range c.Tasks {
		if strings.TrimSpace(t.ID) == "" {
			return fmt.Errorf("tasks[%d]: id 不能为空", i)
		}
		if seen[t.ID] {
			return fmt.Errorf("tasks[%d]: id %q 重复", i, t.ID)
		}
		seen[t.ID] = true
		if strings.TrimSpace(t.SourceDir) == "" || strings.TrimSpace(t.TargetDir) == "" {
			return fmt.Errorf("task %q: source_dir / target_dir 不能为空", t.ID)
		}
		mode := t.Mode
		if mode == "" {
			mode = ModeAlistURL
		}
		if !validModes[mode] {
			return fmt.Errorf("task %q: 非法 mode %q", t.ID, t.Mode)
		}
		if mode == ModePathReplace && strings.TrimSpace(t.URLPrefix) == "" {
			return fmt.Errorf("task %q: path_replace 模式必须配置 url_prefix", t.ID)
		}
		if t.WithSign && mode != ModePathReplace {
			return fmt.Errorf("task %q: with_sign 仅 path_replace 模式可用", t.ID)
		}
		if t.PublicURL != "" {
			if mode != ModeAlistURL {
				return fmt.Errorf("task %q: public_url 仅 alist_url 模式可用", t.ID)
			}
			if !strings.HasPrefix(t.PublicURL, "http://") && !strings.HasPrefix(t.PublicURL, "https://") {
				return fmt.Errorf("task %q: public_url 必须以 http:// 或 https:// 开头", t.ID)
			}
		}
		if t.WatchMode != "" && t.WatchMode != WatchFingerprint && t.WatchMode != WatchDirCount {
			return fmt.Errorf("task %q: 非法 watch_mode %q", t.ID, t.WatchMode)
		}
		if t.Cron != "" {
			if _, err := cronParser.Parse(t.Cron); err != nil {
				return fmt.Errorf("task %q: cron 表达式非法: %w", t.ID, err)
			}
		}
	}
	// 开启 sync_delete 的任务会删除 target_dir 下不属于自己的 strm，
	// 目标目录互相重叠时会误删其他任务的产物，必须禁止。
	for i := range c.Tasks {
		if !c.Tasks[i].SyncDelete {
			continue
		}
		for j := i + 1; j < len(c.Tasks); j++ {
			if !c.Tasks[j].SyncDelete {
				continue
			}
			if pathOverlap(c.Tasks[i].TargetDir, c.Tasks[j].TargetDir) {
				return fmt.Errorf("task %q 与 %q 均开启 sync_delete 且 target_dir 重叠（%s 与 %s），会互相误删",
					c.Tasks[i].ID, c.Tasks[j].ID, c.Tasks[i].TargetDir, c.Tasks[j].TargetDir)
			}
		}
	}
	return nil
}

// Normalize 填充默认值。
func (c *Config) Normalize() {
	if c.Server.Listen == "" {
		c.Server.Listen = ":8080"
	}
	if c.Alist.UserAgent == "" {
		c.Alist.UserAgent = DefaultUserAgent
	}
	if c.Alist.WaitTime < 0 {
		c.Alist.WaitTime = 0
	}
	for i := range c.Tasks {
		t := &c.Tasks[i]
		if t.Name == "" {
			t.Name = t.ID
		}
		if t.Mode == "" {
			t.Mode = ModeAlistURL
		}
		if t.Mode == "local_path" { // 兼容旧配置
			t.Mode = ModePathReplace
		}
		t.PublicURL = strings.TrimRight(strings.TrimSpace(t.PublicURL), "/")
		if t.Concurrency <= 0 {
			t.Concurrency = 50
		}
		if t.WatchInterval < 0 {
			t.WatchInterval = 0
		}
		if t.WatchInterval > 0 && t.WatchInterval < 10 {
			t.WatchInterval = 10 // 防止过密轮询打爆服务器
		}
		if t.WatchMode == "" {
			t.WatchMode = WatchFingerprint
		}
		if t.Download.Enable && t.Download.Concurrency <= 0 {
			t.Download.Concurrency = 5
		}
	}
}

// Task 按 ID 查找任务。
func (c *Config) Task(id string) *TaskConfig {
	for i := range c.Tasks {
		if c.Tasks[i].ID == id {
			return &c.Tasks[i]
		}
	}
	return nil
}

// Load 从文件加载配置并校验。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // 拒绝未知字段，配置项拼写错误时直接报错而非静默忽略
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	// 配置文件含 API 令牌，确保仅属主可读写。
	_ = os.Chmod(path, 0o600)
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Save 将配置写回文件。
func Save(path string, cfg *Config) error {
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return err
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	// 原子写（临时文件 + 改名），避免写盘中途崩溃留下截断配置；配置文件含令牌，仅属主可读写。
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
