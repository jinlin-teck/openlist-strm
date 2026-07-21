// Package alist 是 OpenList/Alist API 的极简客户端，仅支持令牌认证。
package alist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client 调用 OpenList/Alist 的 HTTP API。
type Client struct {
	baseURL   string
	token     string
	userAgent string
	hc        *http.Client

	mu      sync.Mutex
	wait    time.Duration
	lastReq time.Time
}

// New 创建客户端。baseURL 未写协议时默认补 https://。
func New(baseURL, token string, waitMs int, userAgent string) *Client {
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "https://" + baseURL
	}
	return &Client{
		baseURL:   strings.TrimRight(baseURL, "/"),
		token:     token,
		userAgent: userAgent,
		hc:        &http.Client{Timeout: 60 * time.Second},
		wait:      time.Duration(waitMs) * time.Millisecond,
	}
}

// BaseURL 返回规范化后的服务器地址。
func (c *Client) BaseURL() string { return c.baseURL }

// UserAgent 返回客户端使用的 User-Agent。
func (c *Client) UserAgent() string { return c.userAgent }

// envelope 是 OpenList API 的统一响应信封。
type envelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// do 发送一次 API 请求并解析统一信封，按需限速。
func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	c.throttle()

	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.token)
	req.Header.Set("Content-Type", "application/json")
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("%s %s: 响应解析失败(HTTP %d): %s", method, path, resp.StatusCode, truncate(raw, 200))
	}
	if env.Code != 200 {
		return fmt.Errorf("%s %s: %s (code %d)", method, path, env.Message, env.Code)
	}
	if out != nil && len(env.Data) > 0 {
		if err := json.Unmarshal(env.Data, out); err != nil {
			return fmt.Errorf("%s %s: data 解析失败: %w", method, path, err)
		}
	}
	return nil
}

// throttle 保证两次请求之间有最小间隔。
func (c *Client) throttle() {
	if c.wait <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if d := time.Since(c.lastReq); d < c.wait {
		time.Sleep(c.wait - d)
	}
	c.lastReq = time.Now()
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

// FsItem 是 /api/fs/list 返回的单个对象。
type FsItem struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	IsDir    bool   `json:"is_dir"`
	Modified string `json:"modified"`
	Sign     string `json:"sign"`
}

type fsListReq struct {
	Path     string `json:"path"`
	Password string `json:"password"`
	Refresh  bool   `json:"refresh"`
	Page     int    `json:"page"`
	PerPage  int    `json:"per_page"`
}

type fsListData struct {
	Content []FsItem `json:"content"`
	Total   int      `json:"total"`
}

// List 列出目录全部内容（per_page=0 表示不分页）。
// refresh 为 true 时强制绕过服务端缓存重新拉取存储列表。
func (c *Client) List(ctx context.Context, path string, refresh bool) ([]FsItem, error) {
	var data fsListData
	err := c.do(ctx, http.MethodPost, "/api/fs/list", fsListReq{
		Path: path, Refresh: refresh, Page: 1, PerPage: 0,
	}, &data)
	if err != nil {
		return nil, err
	}
	return data.Content, nil
}

// Total 返回目录直属子项数量（含文件和子目录），用于轻量变更检测。
func (c *Client) Total(ctx context.Context, path string, refresh bool) (int, error) {
	var data fsListData
	err := c.do(ctx, http.MethodPost, "/api/fs/list", fsListReq{
		Path: path, Refresh: refresh, Page: 1, PerPage: 1,
	}, &data)
	if err != nil {
		return 0, err
	}
	return data.Total, nil
}

type fsGetReq struct {
	Path     string `json:"path"`
	Password string `json:"password"`
}

type fsGetData struct {
	RawURL string `json:"raw_url"`
}

// RawURL 获取文件的上游存储真实直链。
func (c *Client) RawURL(ctx context.Context, path string) (string, error) {
	var data fsGetData
	if err := c.do(ctx, http.MethodPost, "/api/fs/get", fsGetReq{Path: path}, &data); err != nil {
		return "", err
	}
	if data.RawURL == "" {
		return "", fmt.Errorf("%s: raw_url 为空", path)
	}
	return data.RawURL, nil
}

type meData struct {
	BasePath string `json:"base_path"`
}

// Me 返回当前令牌对应用户的 base_path。
func (c *Client) Me(ctx context.Context) (string, error) {
	var data meData
	if err := c.do(ctx, http.MethodGet, "/api/me", nil, &data); err != nil {
		return "", err
	}
	return data.BasePath, nil
}
