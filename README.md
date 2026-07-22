# openlist-strm

从 OpenList/Alist 目录生成 `.strm` 文件的轻量服务，带 WebUI。
基于 [AutoFilm](https://github.com/AkimioJR/AutoFilm) 重构，仅保留 Alist→STRM 功能。

## 特性

- **四种 STRM 内容模式**
  - `path_replace`：替换下载链接的 URL 前缀（可留空即仅去除），可选 URL 编码——既能生成本地明文路径，也能生成 rclone 等网盘挂载路径
  - `alist_url`：Alist 下载直链（`{base_url}/d/...?sign=...`）
  - `raw_url`：上游存储真实直链
  - `alist_path`：Alist 内部路径
- **伴生文件下载**：可选下载字幕（.ass/.srt/.ssa/.sub）、图片（.png/.jpg/.jpeg）、NFO 及自定义后缀文件
- **仅令牌认证**：只使用 OpenList API Token，不支持用户名密码
- **三种触发方式**
  - 定时：6 段 cron（秒 分 时 日 月 星期）
  - 手动：WebUI 按钮或 API 触发指定任务
  - 变动监控：按 `watch_interval` 轮询，检测到变化才自动生成（见下文「变动监控」）
- **WebUI**：修改全部配置（热加载，无需重启）、查看运行状态与统计、手动运行任务、启用/禁用任务（禁用后任何方式都不会运行）
- 并发扫描、断点跳过（`overwrite: false` 时跳过已存在文件）、可选同步删除（带 0 扫描保护）
- 变动监控指纹持久化（`watch-state.json`），服务/容器重启或热加载后不会重复全量扫描

## 快速开始

### 方式一：下载预编译二进制

从 [Releases](https://github.com/jinlin-teck/openlist-strm/releases) 下载对应平台的二进制，然后：

```bash
chmod +x openlist-strm
cp config.example.yaml config.yaml   # 编辑填入 OpenList 地址与令牌
./openlist-strm --config config.yaml
```

### 方式二：从源码构建

需要 Go 1.24+：

```bash
git clone https://github.com/jinlin-teck/openlist-strm.git
cd openlist-strm
go build -o openlist-strm .
cp config.example.yaml config.yaml   # 编辑填入 OpenList 地址与令牌
./openlist-strm --config config.yaml
```

为其他平台交叉编译（示例：ARM 架构 NAS）：

```bash
GOOS=linux GOARCH=arm64 go build -o openlist-strm-linux-arm64 .
```

### 方式三：Docker 运行

```bash
# 构建镜像
docker build -t openlist-strm:latest .

# 准备配置
mkdir -p ./config && cp config.example.yaml ./config/config.yaml  # 编辑修改

# 启动（strm 输出目录按实际情况挂载，路径需与 config.yaml 中 target_dir 一致）
docker run -d --name openlist-strm \
  -p 8080:8080 \
  -e TZ=Asia/Shanghai \
  -v ./config:/app/config \
  -v /opt/appdata/emby/strm:/opt/appdata/emby/strm \
  openlist-strm:latest
```

或直接 `docker compose up -d`（见仓库根目录 `docker-compose.yml`）。

注意：容器内只能看到挂载进去的目录，`tasks[].target_dir` 必须是已挂载的路径；`TZ` 决定 cron 定时执行的时区。

### 后台运行

```bash
# nohup 简易方式
nohup ./openlist-strm --config config.yaml > openlist-strm.log 2>&1 &

# 或 systemd 服务（/etc/systemd/system/openlist-strm.service）
[Unit]
Description=openlist-strm
After=network-online.target

[Service]
WorkingDirectory=/opt/openlist-strm
ExecStart=/opt/openlist-strm/openlist-strm --config /opt/openlist-strm/config.yaml
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

启动后打开 `http://<主机IP>:8080` 进入 WebUI，之后所有配置均可在 WebUI 中修改（热加载，无需重启）。

命令行参数：`--config` 指定配置文件路径（默认 `config.yaml`）；`--debug` 输出调试日志。

## 配置说明

见 `config.example.yaml`。关键字段：

| 字段                                        | 说明                                                                         |
| ------------------------------------------- | ---------------------------------------------------------------------------- |
| `server.listen`                             | WebUI/API 监听地址，默认 `:8080`                                             |
| `alist.base_url` / `alist.token`            | OpenList 地址与令牌                                                          |
| `alist.wait_time`                           | API 请求最小间隔（毫秒），0 不限速                                           |
| `alist.user_agent`                          | HTTP User-Agent，默认浏览器 UA；115 等网盘按 UA 校验下载签名，勿填 Go/curl UA |
| `tasks[].source_dir` / `tasks[].target_dir` | OpenList 源目录 / 本地 strm 输出目录                                         |
| `tasks[].mode`                              | `path_replace` / `alist_url` / `raw_url` / `alist_path`                      |
| `tasks[].url_prefix`                        | `path_replace` 模式下被替换的 URL 前缀，如 `https://alist.example.com/d/nas` |
| `tasks[].prefix_to`                         | 前缀替换为，留空即仅去除；可填 `/mnt/rclone/nas` 等挂载路径                  |
| `tasks[].url_encode`                        | 路径是否 URL 编码，默认 `true`；生成本地明文路径时设为 `false`               |
| `tasks[].cron`                              | 6 段 cron，留空则仅手动触发                                                  |
| `tasks[].enabled`                           | 是否启用任务，默认 `true`；禁用后定时/监控/手动触发均不会运行                |
| `tasks[].watch_interval`                    | 变动监控间隔（秒），0 关闭，最小 10；检测到变化才触发生成                    |
| `tasks[].watch_mode`                        | `fingerprint`（默认）/ `dir_count`，见下文「变动监控」                       |
| `tasks[].overwrite`                         | 覆盖已存在的 strm / 伴生文件，默认 `false`（存在且大小一致则跳过）           |
| `tasks[].concurrency`                       | 任务并发处理数，默认 50                                                      |
| `tasks[].video_exts`                        | 视频后缀列表，留空用默认（mp4/mkv/flv/avi/wmv/ts/rmvb/webm/mpg/m2ts/mov）    |
| `tasks[].sync_delete`                       | 删除远端已不存在的本地 strm（本次扫描为 0 时自动跳过删除）                   |
| `tasks[].download`                          | 伴生文件下载：`enable`/`subtitle`/`image`/`nfo`/`other_ext`/`concurrency`    |

## 变动监控

OpenList/Alist 没有文件变更通知 API（webhook 仍在[讨论阶段](https://github.com/orgs/OpenListTeam/discussions/1066)），只能通过轮询检测。本项目提供两种监控方式，按任务选择：

|                                  | `fingerprint`（默认）                         | `dir_count`        |
| -------------------------------- | --------------------------------------------- | ------------------ |
| 检测信号                         | 递归全部目录，对受管文件的「路径+大小」算指纹 | 源目录直属子项数量 |
| 每轮 API 请求                    | N 次（N = 目录数）                            | 1 次               |
| 目录内部新增文件（如电视剧更剧） | 能检出                                        | **不能检出**       |
| 同名文件替换（大小变化）         | 能检出                                        | 不能检出           |
| 适用                             | 本地 NAS 存储                                 | 网盘存储（防风控） |

说明：

- 指纹扫描使用 `refresh=true` 绕过服务端缓存；网盘存储会强制回源列表，请把 `watch_interval` 调大（如 1800s）
- 监控触发与定时/手动触发共用任务锁，同一任务不会并发执行
- 启动 watcher 后会立即全量执行一次任务；热加载配置时只重启配置有变化的 watcher，未变更的任务不会重跑
- 各任务的上次监控指纹持久化在配置文件旁的 `watch-state.json`，服务/容器重启后若远端无变化不会重复扫描；任务配置变化后旧状态自动作废

## STRM 内容模式

| 模式           | strm 内容                   | 适用场景                                                |
| -------------- | --------------------------- | ------------------------------------------------------- |
| `path_replace` | 前缀替换后的路径            | Emby 直接读本地/挂载路径（推荐）                        |
| `alist_url`    | OpenList 下载直链（带签名） | 经 OpenList 播放                                        |
| `raw_url`      | 上游存储真实直链            | 绕过 OpenList 播放（每次运行多一次 `/api/fs/get` 调用） |
| `alist_path`   | OpenList 内部路径           | 配合 MediaWarp 等 302 重定向方案                        |

`path_replace` 模式示例：

```
下载链接: https://alist.example.com/d/nas/mnt/.../%E8%8B%B1%E9%9B%84%E6%9C%AC%E8%89%B22%20%281987%29/...1080p.mkv
url_prefix: https://alist.example.com/d/nas
prefix_to: （留空）, url_encode: false
strm 内容: /mnt/.../英雄本色2 (1987)/英雄本色2 (1987) - 1080p.mkv

# rclone 挂载场景：
prefix_to: /mnt/rclone/nas, url_encode: false
strm 内容: /mnt/rclone/nas/mnt/.../英雄本色2 (1987)/英雄本色2 (1987) - 1080p.mkv
```

## API

| 方法 | 路径                  | 说明                                       |
| ---- | --------------------- | ------------------------------------------ |
| GET  | `/api/config`         | 读取当前配置                               |
| PUT  | `/api/config`         | 保存配置并热加载                           |
| GET  | `/api/tasks`          | 任务列表（含运行状态、统计、下次触发时间） |
| POST | `/api/tasks/{id}/run` | 手动触发指定任务                           |
| POST | `/api/alist/test`     | 测试连接 `{base_url, token}`               |
