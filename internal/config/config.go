package config

import (
	"fmt"
	"os"
	"strings"

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
	ModePathReplace = "path_replace" // 替换 URL 前缀（可选 URL 解码），得到 Linux 路径
)

var validModes = map[string]bool{
	ModeAlistURL:    true,
	ModeRawURL:      true,
	ModeAlistPath:   true,
	ModePathReplace: true,
}

// 监控方式。
const (
	WatchFingerprint = "fingerprint" // 递归扫描全部文件算指纹，能检出深层文件变化，适合本地存储
	WatchDirCount    = "dir_count"   // 仅对比源目录直属子项数量，每次 1 次 API 调用，适合网盘存储
)

// TaskConfig 描述一个 STRM 生成任务。
type TaskConfig struct {
	ID            string         `yaml:"id" json:"id"`
	Name          string         `yaml:"name" json:"name"`
	Cron          string         `yaml:"cron" json:"cron"`                     // 6 段 cron（带秒），留空表示仅手动触发
	WatchInterval int            `yaml:"watch_interval" json:"watch_interval"` // 变动监控间隔（秒），0 关闭；检测到远端变化后自动生成
	WatchMode     string         `yaml:"watch_mode" json:"watch_mode"`         // 监控方式：fingerprint（递归指纹）/ dir_count（目录计数）
	SourceDir     string         `yaml:"source_dir" json:"source_dir"`
	TargetDir     string         `yaml:"target_dir" json:"target_dir"`
	Mode          string         `yaml:"mode" json:"mode"`
	URLPrefix     string         `yaml:"url_prefix" json:"url_prefix"` // path_replace 模式：被替换的 URL 前缀
	PrefixTo      string         `yaml:"prefix_to" json:"prefix_to"`   // path_replace 模式：替换为（留空即仅去除前缀）
	URLEncode     *bool          `yaml:"url_encode" json:"url_encode"` // path_replace 模式：路径是否 URL 编码，默认 true
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

// Validate 校验配置，返回第一个错误。
func (c *Config) Validate() error {
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
		if t.WatchMode != "" && t.WatchMode != WatchFingerprint && t.WatchMode != WatchDirCount {
			return fmt.Errorf("task %q: 非法 watch_mode %q", t.ID, t.WatchMode)
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
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
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
	return os.WriteFile(path, data, 0o644)
}
