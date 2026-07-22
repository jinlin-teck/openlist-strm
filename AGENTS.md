# AGENTS.md

本文件面向 AI 编码代理，介绍本项目的架构、构建方式与开发约定。假定读者对项目一无所知。

## 项目概览

**openlist-strm** 是一个用 Go 编写的轻量服务：扫描 OpenList/Alist 服务器上的目录，为其中的视频文件在本地生成 `.strm` 文件（供 Emby/Jellyfin 等媒体服务器使用），并可下载伴生文件（字幕/图片/NFO）。服务带一个内嵌的 WebUI，所有配置可在 WebUI 中修改并热加载，无需重启。

项目基于 [AutoFilm](https://github.com/AkimioJR/AutoFilm) 重构，仅保留 Alist→STRM 功能。

- 语言：Go 1.24（`go.mod` 中声明，无 CGO）
- 依赖极少，仅两个第三方库：
  - `github.com/robfig/cron/v3` — 6 段 cron（带秒）定时调度
  - `gopkg.in/yaml.v3` — 配置文件解析
- HTTP 服务、路由（Go 1.22+ 模式路由）、WebUI 嵌入均使用标准库
- 仅支持 OpenList API 令牌认证，不支持用户名密码

## 构建与运行

```bash
go build -o openlist-strm .          # 构建（Go 1.24+）
go vet ./...                         # 静态检查
gofmt -l .                           # 格式检查
./openlist-strm --config config.yaml # 运行（默认读 config.yaml）
./openlist-strm --debug              # 输出调试日志
```

交叉编译示例：`GOOS=linux GOARCH=arm64 go build -o openlist-strm-linux-arm64 .`

Docker：根目录 `Dockerfile` 为两阶段构建（golang:1.24-alpine → alpine:3.21），`docker-compose.yml` 可直接 `docker compose up -d`。容器内 `tasks[].target_dir` 必须是已挂载路径；`TZ` 决定 cron 时区。

## 测试

**项目目前没有测试文件、没有 CI 流水线。** 验证改动的方式是 `go build` + `go vet` 通过后，用真实 OpenList 服务器手动运行任务确认行为。如新增测试，按 Go 惯例放在被测包旁的 `_test.go` 中，用 `go test ./...` 运行。

## 代码结构

```
main.go                    入口：解析 --config/--debug，初始化 slog，启动 app 与 HTTP 服务
internal/
  config/config.go         配置结构、校验（Validate）、默认值填充（Normalize）、YAML 加载/保存
  alist/client.go          OpenList/Alist HTTP API 极简客户端：/api/fs/list、/api/fs/get、/api/me
  strm/runner.go           核心逻辑：递归扫描、按模式生成 .strm、伴生文件下载、同步删除、变更指纹
  app/app.go               组装层：持有配置与 Runner，管理 cron 调度、变动监控 goroutine、任务运行时状态
  api/server.go            HTTP 接口（net/http ServeMux 模式路由）+ embed 内嵌 WebUI
  api/web/index.html       WebUI 单页（原生 HTML/JS，无前端构建工具）
config.example.yaml        配置模板（含全部字段注释）；config.yaml 为实际配置，已 gitignore
```

运行时架构（`app.App` 为中心）：

- `App` 持有当前 `config.Config` 与一个 `strm.Runner`；配置更新（PUT /api/config）时校验、落盘并整体热加载——重建 Alist 客户端与 cron 条目；监控 goroutine 按 `watchKey`（Alist 连接 + 任务配置的指纹）做 diff，未变更的任务保留原 goroutine，避免热加载导致全部任务重跑
- 任务有三种触发方式：cron 定时（robfig/cron，6 段带秒）、手动（POST /api/tasks/{id}/run，异步执行）、变动监控（每任务一个 goroutine 按 `watch_interval` 轮询指纹，变化才触发；监控启动后立即全量执行一次）
- 监控的上次指纹持久化在配置文件旁的 `watch-state.json`（`internal/app/state.go`，按 `watchKey` 校验有效性，配置变更即作废），重启后远端无变化不会重扫；删除任务或关闭监控时会清掉对应状态
- 任务可用 `enabled` 字段禁用（默认启用，`TaskConfig.IsEnabled()`）；禁用的任务不注册 cron 与监控，`RunTask` 与手动触发接口均拒绝执行
- 同一任务的并发执行通过 `TaskStatus.Running` 标志拒绝（非锁等待）
- 变动监控两种方式：`fingerprint`（递归列目录，对受管文件「路径:大小」算 FNV-1a，N 次 API 请求）与 `dir_count`（仅取源目录子项总数，1 次请求）

四种 STRM 内容模式（`internal/strm/runner.go` 的 `strmContent`）：

- `path_replace`：拼接 `{base_url}/d{base_path}{path}` 后替换掉 `url_prefix`（替换为 `prefix_to`，可留空），`url_encode` 控制是否 URL 编码——可生成本地明文路径或 rclone 挂载路径
- `alist_url`：OpenList 下载直链（带 `?sign=`）
- `raw_url`：调 `/api/fs/get` 取上游真实直链（每文件多一次 API 调用）
- `alist_path`：OpenList 内部路径

## 代码风格与约定

- **注释、日志、错误信息、配置注释全部使用中文**，与现有代码保持一致（用户可见的报错用中文，格式如 `任务 %q 不存在`）
- 标准 Go 风格：gofmt；包级注释描述包的职责；导出的标识符有中文 doc 注释
- 日志用 `log/slog`，键值对形式（如 `log.Info("任务完成", "scanned", ...)）
- 配置新增字段时同步更新：`config.go` 结构体（yaml+json 双 tag）、`Validate`/`Normalize`、`config.example.yaml` 注释、README 配置表、WebUI（`internal/api/web/index.html`）
- `mode` 等枚举值用 `internal/config` 中的常量（`ModeAlistURL` 等）；`Normalize` 中有旧配置兼容逻辑（如 `local_path` → `path_replace`），勿随意删除
- 并发模型：目录用显式栈 DFS（避免深递归），文件处理用 `chan struct{}` 信号量 + `sync.WaitGroup`，统计用互斥锁保护
- 远端路径用 `/` 分隔（`joinPath`/`ensureSlash`），本地路径用 `filepath`；`path_replace` 的 URL 编码按段 `url.PathEscape` 并保留 `/`

## 安全与数据保护注意事项

- **令牌保护**：`config.yaml` 含 API 令牌，已在 `.gitignore` 中，切勿提交；勿在日志中打印 token
- **同步删除保护**：`syncDelete` 在本次扫描到 0 个视频文件时拒绝删除，防止 OpenList 异常导致误清空本地 strm——修改相关逻辑时不得移除此保护
- **路径安全**：生成/下载文件前校验相对路径不含 `..`（`processOne`/`downloadOne`），防止远端文件名穿越到目标目录外
- **User-Agent**：默认使用浏览器 UA（`DefaultUserAgent`），因为 Go/curl UA 会被 115 等网盘的下载签名校验拒绝（403）；取链与跳转下载必须带一致的 UA（见 `downloadOne`）
- API 请求支持 `wait_time` 限速（客户端内 `throttle`）；指纹扫描用 `refresh=true` 会强制回源，网盘存储应调大 `watch_interval`（≥10 秒，Normalize 强制下限）
- HTTP 响应体读取有 32MB 上限（`io.LimitReader`）

## API 一览

| 方法 | 路径                  | 说明                                       |
| ---- | --------------------- | ------------------------------------------ |
| GET  | `/api/config`         | 读取当前配置                               |
| PUT  | `/api/config`         | 保存配置并热加载                           |
| GET  | `/api/tasks`          | 任务列表（含运行状态、统计、下次触发时间） |
| POST | `/api/tasks/{id}/run` | 手动触发指定任务（异步）                   |
| POST | `/api/alist/test`     | 测试连接 `{base_url, token}`               |

WebUI 静态文件经 `//go:embed web` 嵌入二进制，`GET /` 直接由 `http.FileServerFS` 提供。

## 其他

- 更详细的用户文档见 `README.md`（中文）
- `dist/` 是预编译产物目录（不提交进版本控制时无需理会）
- 无 AGENTS.md 子目录约定；本文件即唯一约定文件
