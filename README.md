# TG Media Save · TG 媒体中心 Pro

把 Telegram 频道的图片和视频归档到自己的网盘，并提供一套 Web 控制台来管理频道、存储和采集任务。

基于上游 [tangyoha/telegram_media_downloader](https://github.com/tangyoha/telegram_media_downloader) 的下载引擎，
外面包了一层 FastAPI 面板，解决上游在「多网盘 / 频道级路由 / 可视化管理」上的空白。

---

## 功能

**多网盘 + 频道级路由**
每个频道可以单独绑定一个存储目标和子目录。引擎按「存储 + 子目录」自动分组，逐组生成独立配置串行运行，
所以不同频道能落到不同网盘、不同路径下，不需要改引擎本身。

支持的存储类型：

| 类型 | 说明 |
|---|---|
| `webdav` | 坚果云、Nextcloud、pCloud、群晖 WebDAV 等 |
| `smb` | 局域网 SMB 共享 |
| `tgfs` | 见下方「TGFS 集成」，把频道本身当网盘 |

**预览库**
直接读网盘，不读容器本地（上传成功后本地文件会被删掉）。按存储方案分类，
图片走缩略图 + 落盘缓存，视频用 `rclone serve http` 原生支持 Range，可以拖进度条。

**重启后自动补采增量**
容器 / NAS 重启后自动补一轮，只采「上次水位线之后」的新消息，采过的不重扫。
水位线为 0（从没采过）的频道会被跳过，避免重启时意外全量拉历史。

**安全**
动态口令（TOTP）登录，支持 30 天免验证；密钥可换绑并持久化，不依赖环境变量。

---

## 快速开始

```bash
git clone <本仓库>
cd tg-media-save

# 1. 准备密钥（必填，否则 compose 会拒绝启动）
cp .env.example .env
python3 -c "import secrets; print('JWT_SECRET=' + secrets.token_urlsafe(48))" >> .env
python3 -c "import pyotp; print('OTP_SECRET=' + pyotp.random_base32())" >> .env
chmod 600 .env

# 2. 启动
docker compose up -d --build
```

打开 `http://<你的主机>:8082`，用验证器 App 里 `OTP_SECRET` 对应的 6 位口令登录。

首次使用顺序：

1. **登录与频道** → 填 api_id / api_hash（[my.telegram.org](https://my.telegram.org) 获取）→ 手机号登录
2. **网盘与存储** → 添加存储（WebDAV / SMB / TGFS），填远程目录
3. **登录与频道** → 选频道、绑定存储目标、加入监控队列
4. 点顶部「▶ 启动」跑第一轮；之后在「通知与日志」里打开「重启后自动补采」即可

---

## 环境变量

见 `.env.example`。要点：

| 变量 | 必填 | 说明 |
|---|---|---|
| `JWT_SECRET` | 是 | 登录态签名密钥。**换掉会让所有已登录设备失效** |
| `OTP_SECRET` | 是 | 动态口令种子（Base32）。首次启动后可在面板「系统设置」换绑，之后以面板里的为准 |
| `BUILD_HTTP_PROXY` / `BUILD_HTTPS_PROXY` | 否 | 构建镜像时的代理，境内 pip 慢时用 |
| `TGFS_IMAGE` | 否 | TGFS 容器基础镜像，默认走国内镜像源 |

`.env` 已在 `.gitignore` 中排除，不要提交。

---

## TGFS 集成

[TGFS](https://github.com/TheodoreKrypton/tgfs) 把 Telegram 私有频道当成网盘，对外提供 WebDAV。
本项目可以直接在面板里配置并代管它的容器，不需要单独部署。

在「网盘与存储 → 🗄️ TGFS 服务」填表保存，面板会：生成 `config.yaml` → 导出账号会话 →
重启 tgfs 容器 → 自动把它的 WebDAV 挂成一条存储。

几个绕不开的前提（都踩过）：

**1. Bot 必须是频道管理员**
Telegram 不允许 bot 作为普通成员待在频道里，必须直接设为管理员（需要发消息权限）。
如果报 `CHANNEL_INVALID`，基本就是这一步没做，或者填错了 bot（名字差一个下划线就是另一个 bot）。

**2. 存储频道要填纯数字或完整 ID**
底层解析是 `int(f"-100{channel_id}")`，要的是 ID 的纯数字部分。面板会自动剥掉 `-100` 前缀，
所以填 `-1001234567890`、`1234567890` 或 `@username` 都可以。

**3. 必须有用户账号会话**
TGFS 用频道的**置顶消息**存文件索引，而 Bot API 读不了置顶消息。
所以面板会把当前登录的 Telegram 账号会话导出给 TGFS（只用来读索引，上传下载仍走 Bot）。
这意味着 **TGFS 依赖面板的登录态**，退出登录会导致它起不来。

**换 bot 是安全的**：session 按 bot_id 分文件存，文件索引用的是 message_id 而不是
file_id（file_id 会绑定到具体 bot）。换完只要新 bot 在频道里当管理员即可。

---

## 目录结构

```
main.py                     面板全部后端逻辑（FastAPI，单文件）
templates/index.html        前端（Vue 3 in-DOM 单文件，无需构建）
templates/vendor/           本地化的 vue / tailwind / qrcode，不依赖 CDN
patch/                      对上游引擎两个文件的覆盖（见各文件头注释）
tgfs_patch/                 TGFS 容器补丁：sitecustomize.py 注入代理
docker-compose.yml          面板 + tgfs 两个服务
```

运行时卷：

```
config/     面板配置（config.yaml、缩略图缓存、OTP 密钥）
sessions/   Telegram 登录态
downloads/  临时下载目录（上传成功后会清空）
rclone/     rclone 配置
tgfs_data/  TGFS 的 config.yaml 与 session
```

---

## 几个设计取舍

**为什么引擎不是常驻服务**
上游引擎是「跑一轮就退出」的批处理，不是守护进程。面板把它按存储分组串行拉起，
跑完就结束。这样频道级路由才能实现（每组一份独立配置），代价是不能实时监听新消息——
需要靠定时或重启后自动补采。

**为什么上传后删本地**
归档场景下本地只是中转站，长期堆积会撑爆磁盘。如果不想要这个行为，可以在网盘设置里关掉。

**为什么预览库读网盘而不读本地**
本地文件已经被删了。所有预览都通过 rclone 从远端拉流，缩略图落盘缓存以减少往返。

---

## 注意

- 面板挂载了 `/var/run/docker.sock`（用于启停 tgfs 容器）。这等于给了容器 root 权限，
  **不要把这个面板暴露到公网**，也不要给不可信的人使用。
- `config.yaml` 里以明文保存 api_hash、bot token、网盘密码、代理密码。请确保宿主机权限可控。
- 上游引擎版本升级前，要比对 `patch/` 下两个文件头记录的上游 md5，否则补丁可能失效。
