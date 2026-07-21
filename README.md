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
- **定时 + 手动**：6 段 cron（秒 分 时 日 月 星期）定时执行；也可通过 WebUI 或 API 手动触发
- **WebUI**：修改全部配置（热加载，无需重启）、查看运行状态与统计、手动运行任务
- 并发扫描、断点跳过（`overwrite: false` 时跳过已存在文件）、可选同步删除（带 0 扫描保护）

## 快速开始

```bash
go build -o openlist-strm .
cp config.example.yaml config.yaml   # 编辑填入 base_url 与 token
./openlist-strm --config config.yaml
```

打开 `http://localhost:8080` 进入 WebUI。

## 配置说明

见 `config.example.yaml`。关键字段：

| 字段 | 说明 |
|---|---|
| `alist.base_url` / `alist.token` | OpenList 地址与令牌 |
| `tasks[].mode` | `path_replace` / `alist_url` / `raw_url` / `alist_path` |
| `tasks[].url_prefix` | `path_replace` 模式下被替换的 URL 前缀，如 `https://alist.example.com/d/nas` |
| `tasks[].prefix_to` | 前缀替换为，留空即仅去除；可填 `/mnt/rclone/nas` 等挂载路径 |
| `tasks[].url_encode` | 路径是否 URL 编码，默认 `true`；生成本地明文路径时设为 `false` |
| `tasks[].cron` | 6 段 cron，留空则仅手动触发 |
| `tasks[].sync_delete` | 删除远端已不存在的本地 strm（本次扫描为 0 时自动跳过删除） |
| `tasks[].download` | 伴生文件下载：`enable`/`subtitle`/`image`/`nfo`/`other_ext`/`concurrency` |

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

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/config` | 读取当前配置 |
| PUT | `/api/config` | 保存配置并热加载 |
| GET | `/api/tasks` | 任务列表（含运行状态、统计、下次触发时间） |
| POST | `/api/tasks/{id}/run` | 手动触发指定任务 |
| POST | `/api/alist/test` | 测试连接 `{base_url, token}` |
