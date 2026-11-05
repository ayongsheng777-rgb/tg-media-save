import os, re, io, json, shutil, subprocess, threading, time, mimetypes, configparser, hmac, hashlib, base64, uuid, ipaddress, secrets, socket, asyncio, copy, tempfile
from collections import deque
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlsplit, quote
import pyotp, jwt, httpx
from ruamel.yaml import YAML
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import SessionPasswordNeeded

# 缩略图依赖：镜像里没有 Pillow 时退化为「直接转发原图」，功能不中断
try:
    from PIL import Image, ImageOps
except Exception:                                   # pragma: no cover
    Image = ImageOps = None

app = FastAPI(title="TG Media Downloader Pro Panel")
yaml = YAML()
yaml.preserve_quotes = True

# ruamel 的 YAML 实例**不是线程安全的**：多个线程同时 load 同一个实例会随机抛
# 「string index out of range」「expected <block end>」这类假解析错误。
# load_config 于是回退到旧快照、甚至空 dict —— 预览库预热、并发请求都会踩到，
# 历史上一度表现为「配置莫名其妙变空」。所有解析/序列化一律过这把锁。
_yaml_lock = threading.RLock()


def _yaml_load(stream):
    with _yaml_lock:
        return yaml.load(stream)


def _yaml_dump(data, stream):
    with _yaml_lock:
        yaml.dump(data, stream)


CONFIG_PATH = Path("/app/config_store/config.yaml")
ENGINE_CONFIG_LINK = Path("/app/config.yaml")
# 引擎空闲时的配置副本：绝不让引擎的写盘碰到 CONFIG_PATH 本身
ENGINE_IDLE_CONFIG = Path("/app/config_store/_engine_idle.yaml")
SESSION_STORE_DIR = Path("/app/sessions")
SESSION_NAME = "media_downloader"
RCLONE_CONF_PATH = Path("/root/.config/rclone/rclone.conf")
RCLONE_BIN = "/app/rclone/rclone"

JWT_SECRET = os.getenv("JWT_SECRET") or secrets.token_urlsafe(48)

# 动态口令：优先使用「设置 → 动态口令换绑」写入并持久化的密钥；
# 未换绑时回落到环境变量 OTP_SECRET（向后兼容现有部署，不会把用户锁在门外）。
ENV_OTP_SECRET = os.getenv("OTP_SECRET") or pyotp.random_base32()
OTP_SECRET_FILE = Path("/app/config_store/otp_secret")
OTP_ISSUER = "TG Media Downloader Pro"
OTP_ACCOUNT = "admin"
OTP_PENDING_TTL = 300  # 换绑会话 5 分钟有效
_otp_lock = threading.Lock()
_otp_pending = {"secret": "", "created_at": 0.0}

ALLOWED_STORAGE_TYPES = {"webdav", "smb", "tgfs"}

def normalize_storage_type(stype: str) -> str:
    """把历史配置里的存储类型收敛到当前支持集合。

    InfiniCLOUD 专项已移除（免费额度太小、WebDAV 需付费，没有保留价值）。
    老配置里的 type=infinicloud 一律按普通 WebDAV 处理：它的 url 当年就被
    归一化成 https://<节点>.teracloud.jp/dav/ 了，直接可用，不会丢配置。
    """
    stype = (stype or "webdav").strip().lower()
    if stype == "infinicloud":
        return "webdav"
    return stype if stype in ALLOWED_STORAGE_TYPES else "webdav"

def _read_custom_otp_secret():
    try:
        raw = OTP_SECRET_FILE.read_text(encoding="utf-8").strip().upper()
    except OSError:
        return None
    return raw if re.fullmatch(r"[A-Z2-7]{16,64}", raw) else None

def current_otp_secret() -> str:
    """每次请求实时读取，换绑确认后立刻生效；文件丢失则回落到环境变量。"""
    return _read_custom_otp_secret() or ENV_OTP_SECRET

def otp_source() -> str:
    return "custom" if _read_custom_otp_secret() else "env"

def otp_provisioning_uri(secret: str) -> str:
    return pyotp.TOTP(secret).provisioning_uri(name=OTP_ACCOUNT, issuer_name=OTP_ISSUER)

# 引擎分组运行状态
downloader_proc: Optional[subprocess.Popen] = None
engine_thread: Optional[threading.Thread] = None
stop_event = threading.Event()
pending_auth = {"client": None, "phone": None, "phone_code_hash": None}

_config_cache = {}   # 配置解析成功后的快照，用于读失败时兜底
engine_logs = deque(maxlen=500)
session_stats = {"downloaded_count": 0, "last_file": "", "start_time": 0}

# ==================== 引擎日志过滤 ====================
# 引擎上传走 `rclone copy --progress`（module/cloud_drive.py 里拼的命令），
# 而 rclone 每 0.5 秒就往 stdout 打一段统计：
#     Transferred:   4.496 MiB / 14.111 MiB, 32%, 728 B/s, ETA 3h50m43s
#     Transferring:
#      *   5 - IMG_2468.MOV: 31% /14.111Mi, 776/s, 3h36m15s
#     Elapsed time:      1m49.5s
# 引擎把每一行都 print 出来，面板原先原样 append 进 engine_logs。
# 问题是 engine_logs 是 maxlen=500 的环形缓冲，而引擎会**并发**上传 4~6 个文件，
# 每个文件每 0.5 秒刷 4 行 —— 十几秒就把整个缓冲冲干净，真正的
# 「下载成功 / upload file ... success」全被挤掉，用户看到的「采集反馈」只剩百分比。
# 这里只做两件事：丢掉纯进度行；同一个文件每跨 25% 才留一条。
_RCLONE_NOISE_PREFIX = (
    "transferred:", "transferring:", "elapsed time:", "checks:", "errors:",
    "deleted:", "renamed:", "server side copies:", "server side moves:",
    "waiting for checks to finish", "cancelling",
)
# 形如 ` *   5 - IMG_2468.MOV: 31% /14.111Mi, 776/s, 3h36m15s`
_RCLONE_PROGRESS = re.compile(r"^\*\s+(?P<name>.+?):\s+(?P<pct>\d+)%")
# rclone 用 \r 原地刷新进度，非 TTY 下这些段会粘在同一个 readline 结果里
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def filter_engine_output(raw: str, progress: dict) -> list:
    """把引擎一行原始输出拆成若干条值得展示的日志；返回空列表表示整行丢弃。

    progress 是本次分组运行共享的 `{文件名: 已记录到的百分比档位}`，
    用来给同一个文件限流 —— 否则 25/26/27/28% 这种细微变化照样刷屏。
    """
    kept = []
    for seg in _ANSI_ESCAPE.sub("", raw).replace("\r", "\n").split("\n"):
        seg = seg.strip()
        # 空行，以及 rclone 刷新时留下的纯 "*"、"-"、空白
        if not seg or set(seg) <= {"*", "-", " "}:
            continue
        m = _RCLONE_PROGRESS.match(seg)
        if m:
            name = m.group("name").strip().split("/")[-1]
            pct = int(m.group("pct"))
            bucket = pct // 25          # 0 / 25 / 50 / 75 / 100 五档
            if progress.get(name) == bucket:
                continue
            progress[name] = bucket
            kept.append(f"⬆ {name} {pct}%")
            continue
        # 剩余以 "*" 开头的都是「未知大小的文件正在传」，没有信息量
        if seg.startswith("*"):
            continue
        if seg.lower().startswith(_RCLONE_NOISE_PREFIX):
            continue
        kept.append(seg[:300])          # 极端长的合并行截断，别撑爆前端
    return kept


# ==================== 初始化 ====================

def sync_session_files():
    SESSION_STORE_DIR.mkdir(parents=True, exist_ok=True)
    app_sess = Path(f"/app/{SESSION_NAME}.session")
    store_sess = SESSION_STORE_DIR / f"{SESSION_NAME}.session"
    if store_sess.exists() and not app_sess.exists():
        app_sess.symlink_to(store_sess)
    elif app_sess.exists() and not app_sess.is_symlink():
        shutil.move(str(app_sess), str(store_sess))
        app_sess.symlink_to(store_sess)

def init_env():
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    RCLONE_CONF_PATH.parent.mkdir(parents=True, exist_ok=True)
    sync_session_files()
    if not CONFIG_PATH.exists():
        default_cfg = {
            "api_id": 123456,
            "api_hash": "",
            "media_types": ["photo", "video"],
            "file_formats": {"video": ["all"], "audio": ["all"], "document": ["all"]},
            "save_path": "/app/save",
            "file_path_prefix": ["chat_title", "media_datetime"],
            "language": "ZH",
            "web_host": "0.0.0.0",
            "web_port": 5000,
            "chat": []
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            _yaml_dump(default_cfg, f)
    # /app/config.yaml 绝不能指向主配置本身。
    # 引擎退出时用 open(config_file, "w") 回写水位线，这个写法会**穿透符号链接**，
    # 一旦链接指向 config_store/config.yaml，_ui_*（网盘、登录态、频道标题、
    # 飞书通知…）就会被整段冲掉、水位线还会被打回 0。
    # 这里只指向一份「去掉 _ui_* 的副本」；此处是模块导入期，link_engine_idle()
    # 还没定义，所以内联同样的逻辑。
    try:
        if not ENGINE_IDLE_CONFIG.exists():
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                _boot_cfg = _yaml_load(f) or {}
            with open(ENGINE_IDLE_CONFIG, "w", encoding="utf-8") as f:
                _yaml_dump({k: v for k, v in _boot_cfg.items()
                           if not str(k).startswith("_ui_")}, f)
    except Exception as _exc:
        print(f"[引擎配置] 初始化空闲副本失败: {_exc}")
    if ENGINE_CONFIG_LINK.exists() or ENGINE_CONFIG_LINK.is_symlink():
        ENGINE_CONFIG_LINK.unlink()
    ENGINE_CONFIG_LINK.symlink_to(ENGINE_IDLE_CONFIG)

init_env()
# 安全：不在容器日志中输出 OTP 密钥 / provisioning URI（见设计文档 §2）
print("\n========== [安全中心] ==========")
print("动态口令来源：" + ("自定义密钥（设置页换绑，已持久化）" if otp_source() == "custom" else "环境变量 OTP_SECRET"))
print("可在 WebUI「系统设置」中换绑；日志不回显任何密钥")
print("================================\n")

# 前端依赖本地化：避免 CDN 不可达导致 Vue 未挂载、页面只剩背景色
VENDOR_DIR = Path("/app/templates/vendor")
if VENDOR_DIR.exists():
    app.mount("/vendor", StaticFiles(directory=str(VENDOR_DIR)), name="vendor")

# ==================== 配置读写 ====================

def load_config():
    """读取配置。

    解析失败通常是并发写入读到半截 YAML 的瞬时情况：重试一次，仍失败则
    回退到上次成功解析的快照，避免整个面板直接 500。
    """
    global _config_cache
    for attempt in (1, 2):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = _yaml_load(f) or {}
            _config_cache = cfg
            return cfg
        except Exception as e:
            if attempt == 1:
                time.sleep(0.25)
                continue
            print(f"[配置] 读取失败（{e}），回退到上次成功快照")
            try:
                return copy.deepcopy(_config_cache) if _config_cache else {}
            except Exception:
                return {}

def dump_config(cfg):
    """原子写配置：先写临时文件再 os.replace。

    之前是「open(w) 直接写」，写入期间并发读会拿到半截 YAML，
    ruamel 解析直接抛 IndexError，表现为接口 500。

    另外加一道保险：盘上配置带 _ui_*（网盘/登录态/通知…）而待写的完全不带时，
    说明拿错了对象（引擎格式的配置），拒绝覆盖 —— 这类覆盖会静默丢掉整段设置。
    """
    if cfg and not any(k.startswith("_ui_") for k in cfg):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                on_disk = _yaml_load(f) or {}
            if any(k.startswith("_ui_") for k in on_disk):
                print("[配置] 拒绝写入：待写配置不含 _ui_*，会覆盖掉网盘/登录态/通知设置")
                return
        except Exception:
            pass
    tmp = CONFIG_PATH.with_suffix(".yaml.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        _yaml_dump(cfg, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CONFIG_PATH)

def link_engine_config(target: Path):
    """把 /app/config.yaml 指向指定配置文件（引擎按相对路径读取此文件）"""
    if ENGINE_CONFIG_LINK.exists() or ENGINE_CONFIG_LINK.is_symlink():
        ENGINE_CONFIG_LINK.unlink()
    ENGINE_CONFIG_LINK.symlink_to(target)


def link_engine_idle():
    """引擎空闲时把 /app/config.yaml 指向一份主配置副本，而不是主配置本身。

    引擎退出时用 open(config_file, "w") 回写水位线，这个写法会**穿透符号链接**。
    只要链接曾经指向 config_store/config.yaml，引擎的回写就会把 _ui_*（网盘、
    登录态、频道标题、飞书通知…）整段冲掉，水位线还会被打回 0。
    所以链接一律只指向副本或分组配置，主配置永远不经引擎之手。
    """
    try:
        data = strip_ui_keys(load_config())
    except Exception as exc:
        print(f"[引擎配置] 生成空闲副本失败: {exc}")
        return
    try:
        itmp = ENGINE_IDLE_CONFIG.with_suffix(".yaml.tmp")
        with open(itmp, "w", encoding="utf-8") as f:
            _yaml_dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(itmp, ENGINE_IDLE_CONFIG)
    except Exception as exc:
        print(f"[引擎配置] 写空闲副本失败: {exc}")
        return
    link_engine_config(ENGINE_IDLE_CONFIG)

# ==================== 代理配置 ====================

ALLOWED_PROXY_SCHEMES = {"socks5", "socks4", "http"}

class ProxyPayload(BaseModel):
    enable: bool = False
    scheme: str = "socks5"
    host: str = "127.0.0.1"
    port: int = 7890
    user: str = ""
    password: str = ""  # 留空表示沿用已保存的密码（前端不回显明文）

def build_proxy_conf(payload: ProxyPayload, existing: Optional[dict] = None) -> Optional[dict]:
    """校验并生成 Pyrogram 使用的 proxy dict；关闭代理时返回 None。"""
    if not payload.enable:
        return None
    scheme = (payload.scheme or "socks5").strip().lower()
    if scheme not in ALLOWED_PROXY_SCHEMES:
        raise ValueError("不支持的代理协议，仅支持 socks5 / socks4 / http")
    host = (payload.host or "").strip()
    if not host:
        raise ValueError("请填写代理主机地址")
    try:
        port = int(payload.port)
    except (TypeError, ValueError):
        raise ValueError("代理端口必须是数字")
    if not (1 <= port <= 65535):
        raise ValueError("代理端口需在 1–65535 之间")

    user = (payload.user or "").strip()
    password = (payload.password or "").strip()
    old = existing or {}
    if user and not password:
        # 密码不回显到前端：留空表示沿用旧密码，但换用户名时必须重新填
        if old.get("password") and old.get("username") == user:
            password = old["password"]
        elif old.get("password"):
            raise ValueError("代理用户名已变更，请重新填写代理密码")
    if password and not user:
        raise ValueError("填写了代理密码就必须同时填写用户名")

    conf = {"scheme": scheme, "hostname": host, "port": port}
    if user:
        conf["username"] = user
        conf["password"] = password
    return conf

def apply_proxy(cfg, payload: ProxyPayload) -> Optional[dict]:
    conf = build_proxy_conf(payload, cfg.get("proxy"))
    if conf:
        cfg["proxy"] = conf
    elif "proxy" in cfg:
        del cfg["proxy"]
    return conf

def proxy_to_url(conf: dict) -> str:
    """httpx 用的代理 URL。用户名/密码必须百分号编码——sing-box 密码常含 @ : 等字符，
    直接 f-string 拼接会被解析成 host 的一部分，导致代理握手失败。"""
    auth = ""
    if conf.get("username"):
        auth = f"{quote(conf['username'], safe='')}:{quote(conf.get('password', ''), safe='')}@"
    return f"{conf.get('scheme', 'socks5')}://{auth}{conf['hostname']}:{conf['port']}"

# ==================== 多网盘管理 ====================

def normalize_tgfs_webdav_url(raw: str) -> str:
    """TGFS：仅接受显式提供的受控 WebDAV URL，不静默猜测公网域名。

    遵循设计文档 §5.2：必须完整 http(s)://host:port/path；URL 内不得夹带
    账户/密码/查询串/片段；明文 HTTP 仅允许内网地址或名为 tgfs 的内部服务。
    """
    url = (raw or "").strip().rstrip("/")
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("TGFS 需要完整的 http(s)://host:port/webdav 地址")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("URL 不得包含账户、密码、查询串或片段")

    def _is_private_host(host: str) -> bool:
        if host in {"localhost", "tgfs", "127.0.0.1", "::1"}:
            return True
        try:
            return ipaddress.ip_address(host).is_private
        except ValueError:
            return host.endswith(".local") or host.endswith(".internal")

    if parsed.scheme == "http" and not _is_private_host(parsed.hostname):
        raise ValueError("公网地址必须使用 HTTPS；HTTP 仅限内网 TGFS 服务")
    return url


def normalize_storage_url(stype: str, host_val: str) -> str:
    host_val = (host_val or "").strip().rstrip("/")
    if not host_val:
        return ""
    if stype == "tgfs":
        return normalize_tgfs_webdav_url(host_val)
    if stype == "webdav":
        if not host_val.startswith("http"):
            host_val = f"https://{host_val}"
        if not host_val.endswith("/"):
            host_val += "/"
    return host_val

def encrypt_rclone_pass(plain: str) -> str:
    if not plain:
        return ""
    try:
        res = subprocess.run([RCLONE_BIN, "obscure", plain], capture_output=True, text=True)
        return res.stdout.strip()
    except Exception as e:
        print(f"[rclone obscure error] {e}")
        return ""

def get_existing_rclone_passes() -> dict:
    if not RCLONE_CONF_PATH.exists():
        return {}
    cp = configparser.ConfigParser()
    try:
        cp.read(RCLONE_CONF_PATH, encoding="utf-8")
    except Exception:
        return {}
    return {sec: cp[sec]["pass"] for sec in cp.sections() if cp.has_option(sec, "pass")}

def remote_name_of(sid: str) -> str:
    return f"remote_{sid}" if sid else "tg_remote"

def write_rclone_conf(storages: list):
    """根据多网盘配置重建 rclone.conf（每个网盘一个独立 remote）"""
    lines = []
    existing_passes = get_existing_rclone_passes()
    for s in storages:
        sid = s.get("id", "")
        rname = remote_name_of(sid)
        stype = normalize_storage_type(s.get("type"))
        url = s.get("url", "")
        user = s.get("user", "")
        plain_pass = s.get("_plain_pass", "") or s.get("pass", "")
        obs_pass = encrypt_rclone_pass(plain_pass) if plain_pass else existing_passes.get(rname, "")
        # TGFS 仍走 Rclone 的 webdav 后端（不存在 tgfs 驱动），且凭据必须齐全
        if stype == "tgfs":
            if not url or not user.strip() or not obs_pass.strip():
                raise ValueError(f"TGFS 网盘「{s.get('name', sid)}」需填写完整的 WebDAV 地址、用户名和密码")
            if "\n" in user or "\r" in user:
                raise ValueError("TGFS WebDAV 用户名非法")
        lines.append(f"[{rname}]")
        if stype in ("webdav", "tgfs"):
            lines.append("type = webdav")
            lines.append(f"url = {url}")
            lines.append("vendor = other")
        elif stype == "smb":
            lines.append("type = smb")
            lines.append(f"host = {url}")
        else:
            lines.append("type = webdav")
            lines.append(f"url = {url}")
            lines.append("vendor = other")
        lines.append(f"user = {user}")
        lines.append(f"pass = {obs_pass}")
        lines.append("")
    # 原子写：rclone 可能在别处并发读取，截断写会读到半截配置
    rtmp = RCLONE_CONF_PATH.with_suffix(".conf.tmp")
    rtmp.write_text("\n".join(lines), encoding="utf-8")
    try:
        os.chmod(rtmp, 0o600)
    except OSError:
        pass
    os.replace(rtmp, RCLONE_CONF_PATH)

def get_all_rclone_quotas() -> list:
    if not RCLONE_CONF_PATH.exists():
        return []
    cp = configparser.ConfigParser()
    try:
        cp.read(RCLONE_CONF_PATH, encoding="utf-8")
    except Exception:
        return []
    quotas = []
    tgfs_remotes = {
        remote_name_of(s.get("id", ""))
        for s in load_config().get("_ui_storages", [])
        if s.get("type") == "tgfs"
    }
    for sec in cp.sections():
        # TGFS 的 WebDAV 桥接不一定实现标准容量接口，按文档要求明示「未知」
        if sec in tgfs_remotes:
            quotas.append({"remote": sec, "info": "未知（TGFS 无标准容量接口）"})
            continue
        try:
            res = subprocess.run([RCLONE_BIN, "about", f"{sec}:", "--timeout", "8s"], capture_output=True, text=True)
            info = res.stdout.strip().replace("\n", " | ") if res.returncode == 0 and res.stdout.strip() else "未知"
        except Exception:
            info = "未知"
        quotas.append({"remote": sec, "info": info})
    return quotas

def get_rclone_quota_summary() -> str:
    quotas = get_all_rclone_quotas()
    if not quotas:
        return "未配置网盘"
    return " | ".join(f"{q['remote']}: {q['info']}" for q in quotas)

# ==================== 引擎分组运行（多网盘路由核心） ====================
# 关键事实：telegram_media_downloader 引擎只在启动时从全局 config.yaml 读取
# 一次 upload_drive（module/app.py -> self.cloud_drive_config 单例），不支持
# 频道级覆盖。因此这里按「频道 -> 网盘」分组，串行生成每组的临时 config，
# 逐组启动引擎；每组结束后把更新后的水位线回写主配置。

def clean_sub_path(raw: str) -> str:
    """清洗频道级子目录：去掉空段、. 与 ..，防止路径穿越。"""
    parts = []
    for seg in (raw or "").replace("\\", "/").split("/"):
        seg = seg.strip().strip(".")
        if seg in ("", ".", ".."):
            continue
        parts.append(seg)
    return "/".join(parts)


def describe_channel_target(c, title_map, storages_by_id, base_save_path) -> str:
    """给通知用的一行：频道 → 存储目标 → 完整落盘路径。

    与引擎的路径拼接保持一致：save_path / chat_title / media_datetime，
    media_datetime 的格式来自引擎 date_format（%Y_%m，即 YYYY_MM）。
    """
    cid = str(c.get("chat_id"))
    title = title_map.get(cid, cid)
    sub = clean_sub_path(c.get("_path", ""))
    storage = storages_by_id.get(c.get("_storage_id", ""))
    if storage:
        base = (storage.get("remote_dir", "tg_backup") or "tg_backup").strip("/")
        target = storage.get("name", storage.get("id", ""))
        rel = "/".join(x for x in [base, sub, title, "YYYY_MM"] if x)
        return f"• {title} → {target}：`{rel}/`"
    rel = "/".join(x for x in [base_save_path, sub, title, "YYYY_MM"] if x)
    return f"• {title} → 本地存储：`{rel}/`"


def flush_group_watermark(group_cfg: Path) -> int:
    """把引擎写进分组配置的水位线同步回主配置；返回本次推进了几个频道。

    为什么需要这一步：引擎只认它自己读到的那份配置（/app/config.yaml 这个符号
    链接指向本组临时文件），而面板真正保存的是 config_store/config.yaml。
    引擎跑完会把 last_read_message_id 写进**链接指向的那份**，必须搬回主配置，
    否则下一轮又从头开始。

    注意引擎是「整轮结束才写一次」：patch/media_downloader.py 第 718 行在 finally
    里调 app.update_config()，而 update_config 内部还有 `if value.finish_task` 的
    门槛。所以运行途中分组配置里一直是旧值 —— 这也意味着**中途重启会丢掉本轮
    进度**，那个频道下次要从头扫（已上传的文件有 rclone --ignore-existing 挡着，
    不会重复上传，但会白下一遍）。这是上游引擎的行为，面板侧改不了。

    以「链接此刻实际指向哪里」为准：引擎可能就地改写它读到的那份配置，也可能
    把符号链接换成了普通文件，只认传进来的路径会读到旧值（水位线被打回 0）。
    """
    src = group_cfg
    try:
        resolved = ENGINE_CONFIG_LINK.resolve()
        if resolved.exists():
            src = resolved
    except Exception:
        pass
    with open(src, "r", encoding="utf-8") as f:
        updated = _yaml_load(f) or {}
    id_map = {str(u.get("chat_id")): int(u.get("last_read_message_id", 0) or 0)
              for u in updated.get("chat", [])}
    if not id_map:
        return 0
    with _yaml_lock:
        main_cfg = load_config()
        moved = 0
        for c in main_cfg.get("chat", []):
            key = str(c.get("chat_id"))
            if key in id_map and id_map[key] > int(c.get("last_read_message_id", 0) or 0):
                c["last_read_message_id"] = id_map[key]
                moved += 1
        if moved:
            dump_config(main_cfg)
    return moved


def build_groups(cfg):
    """按 (存储目标, 频道子目录) 分组。

    上游引擎的 save_path 与 upload_drive.remote_dir 都是「每组一份」的，
    所以只要按子目录把频道拆成更细的组，就能让每个频道落到自己的路径，
    无需改动引擎本身。
    """
    storages = {s["id"]: s for s in cfg.get("_ui_storages", [])}
    groups = {}
    for c in cfg.get("chat", []):
        sid = c.get("_storage_id", "")
        if sid not in storages:
            sid = ""
        # 双保险：未经显式授权的 TGFS 绝不参与自动上传，降级为本地存储
        if sid and storages[sid].get("type") == "tgfs" and not storages[sid].get("upload_authorized"):
            engine_logs.append(
                f"[{time.strftime('%H:%M:%S')}] ⚠ 频道 {c.get('chat_id')} 指向未授权的 TGFS，本轮改存本地"
            )
            sid = ""
        sub = clean_sub_path(c.get("_path", ""))
        groups.setdefault((sid, sub), []).append(c)
    return storages, groups

def make_upload_drive(storage, delete_after: bool, sub_path: str = "") -> dict:
    if not storage:
        return {"enable_upload_file": False}
    rname = remote_name_of(storage.get("id", ""))
    base = (storage.get("remote_dir", "tg_backup") or "tg_backup").strip("/") or "tg_backup"
    remote = f"{rname}:{base}"
    if sub_path:
        remote += "/" + sub_path.strip("/")
    return {
        "enable_upload_file": True,
        "remote_dir": remote,
        "upload_adapter": "rclone",
        "rclone_path": RCLONE_BIN,
        "before_upload_file_zip": False,
        "after_upload_file_delete": delete_after,
    }

def clean_chat_for_engine(c) -> dict:
    return {
        "chat_id": c.get("chat_id"),
        "last_read_message_id": int(c.get("last_read_message_id", 0)),
    }

def strip_ui_keys(cfg) -> dict:
    return {k: v for k, v in cfg.items() if not k.startswith("_ui_")}

def run_all_groups(auto: bool = False):
    """按网盘分组串行运行下载引擎，实现真正的频道级存储路由。

    auto=True 表示这轮是「容器启动后自动补一轮」，语义与手动点「▶ 启动」不同：
    手动启动用户心里有数，自动启动不该替人做决定，所以水位线为 0（从没采过）
    的频道会被跳过 —— 那种频道跑起来等于全量拉历史，可能几百 GB。
    """
    global downloader_proc
    started_at = time.time()
    auto_skipped = 0
    # 采集真正结束的时刻。不能用 finally 里的 time.time()：那之后还要发飞书/TG 通知，
    # 一次通知可能耗十几秒，会把「耗时」算成两倍（实测 12.5s 采集被记成 25.8s）。
    collect_finished_at = None
    try:
        cfg = load_config()
        storages, groups = build_groups(cfg)
        if not groups:
            engine_logs.append(f"[{time.strftime('%H:%M:%S')}] ⚠ 未配置任何频道，任务跳过")
            return

        delete_after = cfg.get("_ui_delete_after_upload", False)
        base_save_path = cfg.get("save_path", "/app/save")
        total = len(groups)
        engine_logs.append(f"[{time.strftime('%H:%M:%S')}] 📋 共 {total} 个存储分组，将依次归档")

        for idx, ((sid, sub_path), chats) in enumerate(groups.items()):
            if stop_event.is_set():
                engine_logs.append(f"[{time.strftime('%H:%M:%S')}] ⏹ 收到停止信号，中止后续分组")
                break

            if auto:
                # 自动启动只补增量：水位线 > 0 才说明这个频道采过，
                # 引擎会用 offset_id=last_read_message_id 从它之后往新处取。
                kept = [c for c in chats if int(c.get("last_read_message_id", 0) or 0) > 0]
                if len(kept) != len(chats):
                    auto_skipped += len(chats) - len(kept)
                chats = kept
                if not chats:
                    continue

            label = storages[sid].get("name", sid) if sid else "本地存储"
            if sub_path:
                label += f" · {sub_path}"
            engine_logs.append(f"[{time.strftime('%H:%M:%S')}] ▶ [{idx+1}/{total}] {label} · {len(chats)} 个频道")

            # 每组生成独立 config，避免私有字段污染引擎
            run_cfg = strip_ui_keys(load_config())
            run_cfg["chat"] = [clean_chat_for_engine(c) for c in chats]
            run_cfg["upload_drive"] = make_upload_drive(storages.get(sid), delete_after, sub_path)
            run_cfg.pop("_ui_delete_after_upload", None)
            # 频道级子目录：本地根目录也下沉一层，使本地结构与远端 remote_dir 对齐
            run_cfg["save_path"] = f"{base_save_path}/{sub_path}" if sub_path else base_save_path

            tmp = Path(f"/app/config_store/_run_group_{idx}.yaml")
            # 先写 .tmp 再 replace：引擎随时可能读到这份配置，避免半截 YAML
            gtmp = tmp.with_suffix(".yaml.tmp")
            with open(gtmp, "w", encoding="utf-8") as f:
                _yaml_dump(run_cfg, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(gtmp, tmp)
            link_engine_config(tmp)

            proc = subprocess.Popen(
                ["python3", "-u", "media_downloader.py"], cwd="/app",
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
            )
            downloader_proc = proc
            # 每个分组一份进度档位表：上一组的同名文件不该把这一组的进度压掉
            progress = {}
            for raw_line in iter(proc.stdout.readline, ""):
                for line in filter_engine_output(raw_line, progress):
                    engine_logs.append(f"[{time.strftime('%H:%M:%S')}] {line}")
                    if any(k in line.lower() for k in ["successfully downloaded", "下载成功", "成功下载", "upload file"]):
                        session_stats["downloaded_count"] += 1
            proc.wait()

            # 组结束后回写水位线。
            # 引擎只在整轮 finally 里调一次 app.update_config()（patch/media_downloader.py
            # 第 718 行），而且 update_config 内部还有 `if value.finish_task` 的门槛 ——
            # 所以运行途中这份分组配置里一直是旧值，中途同步没有意义，只能等这里。
            try:
                flush_group_watermark(tmp)
            except Exception as e:
                engine_logs.append(f"[{time.strftime('%H:%M:%S')}] ⚠ 水位线回写失败: {e}")
            finally:
                downloader_proc = None
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass

            engine_logs.append(f"[{time.strftime('%H:%M:%S')}] ✅ [{idx+1}/{total}] {label} 完成")

        # 恢复引擎 config 指向主配置
        link_engine_idle()

        elapsed = round(time.time() - session_stats["start_time"], 1)
        engine_logs.append(f"[{time.strftime('%H:%M:%S')}] 🏁 全部任务结束 (耗时 {elapsed}s)")
        collect_finished_at = time.time()

        # 任务完成通知
        notify_cfg = load_config().get("_ui_notify", {})
        if notify_cfg.get("notify_on_task_finish", True) and elapsed > 2:
            latest = load_config()
            title_map = latest.get("_ui_chat_titles", {})
            storages_by_id = {s["id"]: s for s in latest.get("_ui_storages", [])}
            chat_lines = []
            for c in latest.get("chat", []):
                chat_lines.append(describe_channel_target(c, title_map, storages_by_id, base_save_path))
            send_notification(
                "TG 媒体归档与同步完成",
                [
                    f"**本轮处理文件数**：`{session_stats['downloaded_count']}` 项",
                    f"**总耗时**：`{elapsed}` 秒",
                    "**归档结构**：`子目录 / 频道名称 / YYYY_MM`（子目录为空时省略；月份为媒体发布月份）",
                    "\n".join(chat_lines) if chat_lines else "**频道**：无",
                    f"**网盘状态**：`{get_rclone_quota_summary()}`"
                ]
            )
    finally:
        downloader_proc = None
        link_engine_idle()
        # 落盘「上次采集记录」。
        # engine_logs 是内存 list，容器一重启就清空 —— 用户重启后既看不到刚才跑到
        # 哪一组，也不知道为什么停了。把这一轮的起止时间、模式、结果写进 _ui_last_run，
        # 面板重启后还能显示，这比只留一个水位线 ID 直观得多。
        # read-modify-write 全程持 _yaml_lock，避免与其它请求的 dump_config 交叉覆盖。
        try:
            with _yaml_lock:
                _cfg = load_config()
                _end = collect_finished_at or time.time()
                _cfg["_ui_last_run"] = {
                    "at": int(started_at),
                    "ended_at": int(_end),
                    "elapsed": round(_end - float(session_stats.get("start_time") or started_at), 1),
                    "downloaded": int(session_stats.get("downloaded_count", 0)),
                    "mode": "auto" if auto else "manual",
                    "skipped_no_watermark": auto_skipped,
                    "ok": not stop_event.is_set(),
                }
                dump_config(_cfg)
        except Exception as _exc:
            print(f"[采集记录] 写入失败: {_exc}")

# ==================== 通知 ====================

def send_notification(title: str, content_lines: List[str]) -> dict:
    cfg = load_config()
    notify = cfg.get("_ui_notify", {})
    results = {}

    if notify.get("feishu_enable") and notify.get("feishu_webhook"):
        webhook = notify["feishu_webhook"].strip()
        secret = notify.get("feishu_secret", "").strip()
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {"title": {"tag": "plain_text", "content": f"🚀 {title}"}, "template": "blue"},
                "elements": [
                    {"tag": "markdown", "content": "\n".join(content_lines)},
                    {"tag": "hr"},
                    {"tag": "note", "elements": [{"tag": "plain_text", "content": f"推送时间: {time.strftime('%Y-%m-%d %H:%M:%S')}"}]}
                ]
            }
        }
        if secret:
            ts = str(int(time.time()))
            sig_str = f"{ts}\n{secret}"
            hmac_code = hmac.new(sig_str.encode("utf-8"), digestmod=hashlib.sha256).digest()
            payload["timestamp"] = ts
            payload["sign"] = base64.b64encode(hmac_code).decode("utf-8")
        try:
            r = httpx.post(webhook, json=payload, timeout=10.0)
            results["feishu"] = r.json()
        except Exception as e:
            results["feishu"] = str(e)

    if notify.get("tgbot_enable") and notify.get("tgbot_token") and notify.get("tgbot_chat_id"):
        token = notify["tgbot_token"].strip()
        chat_id = notify["tgbot_chat_id"].strip()
        proxy_cfg = cfg.get("proxy")
        proxy_url = proxy_to_url(proxy_cfg) if proxy_cfg and proxy_cfg.get("hostname") and proxy_cfg.get("port") else None
        text_msg = f"🚀 *{title}*\n\n" + "\n".join(content_lines)
        try:
            with httpx.Client(proxy=proxy_url, timeout=12.0) as client:
                r = client.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": text_msg, "parse_mode": "Markdown"})
                results["tgbot"] = r.json()
        except Exception as e:
            results["tgbot"] = str(e)
    return results

# ==================== 鉴权 ====================

def verify_auth(request: Request) -> dict:
    token = request.cookies.get("tg_device_token")
    if not token:
        raise HTTPException(status_code=401, detail="未登录")
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401, detail="凭证无效")

class LoginReq(BaseModel):
    otp_code: str
    remember_30d: bool = True

@app.post("/api/login")
def login(req: LoginReq, response: Response):
    if not pyotp.TOTP(current_otp_secret()).verify(req.otp_code, valid_window=1):
        raise HTTPException(status_code=403, detail="OTP 错误")
    now = time.time()
    days = 30 if req.remember_30d else 1
    ttl = days * 86400
    exp = now + ttl
    token = jwt.encode(
        {"sub": "admin", "iat": now, "exp": exp, "days": days},
        JWT_SECRET, algorithm="HS256"
    )
    # max_age 必须与 JWT 的 exp 一致：过去固定 30 天，选「仅 1 天」时浏览器会
    # 继续携带一个早已失效的 cookie，表现成反复要求登录却清不掉。
    response.set_cookie(
        key="tg_device_token", value=token,
        max_age=ttl, httponly=True, samesite="lax", path="/",
    )
    return {"status": "ok", "days": days, "expires_at": exp}

@app.post("/api/logout")
def logout(response: Response):
    response.delete_cookie("tg_device_token", path="/")
    return {"status": "ok"}

@app.get("/api/security/session")
def session_info(claims: dict = Depends(verify_auth)):
    """当前设备的登录有效期。兼容早期签发、未带 days/iat 的旧 token。"""
    exp = float(claims.get("exp", 0))
    remain = max(0.0, exp - time.time())
    if "days" in claims:
        days = int(claims["days"])
    else:
        iat = float(claims.get("iat", 0))
        days = round((exp - iat) / 86400) if iat else (30 if remain > 2 * 86400 else 1)
    return {
        "days": days,
        "long_lived": days >= 30,
        "expires_at": exp,
        "remain_seconds": int(remain),
        "remain_days": round(remain / 86400, 1),
    }

# ==================== 系统设置：动态口令换绑 ====================
# 两阶段确认：先用当前口令证明身份 → 生成待确认密钥 → 用新口令确认后才落盘。
# 确认前旧口令一直有效，pending 只在内存中且 5 分钟过期，避免把自己锁在门外。

class OtpCurrentReq(BaseModel):
    current_code: str

class OtpCodeReq(BaseModel):
    code: str

@app.get("/api/security/otp", dependencies=[Depends(verify_auth)])
def otp_status():
    with _otp_lock:
        pending = bool(_otp_pending["secret"] and time.time() - _otp_pending["created_at"] < OTP_PENDING_TTL)
    return {
        "source": otp_source(),
        "issuer": OTP_ISSUER,
        "account": OTP_ACCOUNT,
        "has_env_secret": bool(os.getenv("OTP_SECRET")),
        "pending": pending,
    }

@app.post("/api/security/otp/begin", dependencies=[Depends(verify_auth)])
def otp_begin(req: OtpCurrentReq):
    if not pyotp.TOTP(current_otp_secret()).verify(req.current_code.strip(), valid_window=1):
        raise HTTPException(status_code=403, detail="当前动态口令不正确")
    secret = pyotp.random_base32()
    with _otp_lock:
        _otp_pending.update({"secret": secret, "created_at": time.time()})
    return {"secret": secret, "uri": otp_provisioning_uri(secret), "expires_in": OTP_PENDING_TTL}

@app.post("/api/security/otp/confirm", dependencies=[Depends(verify_auth)])
def otp_confirm(req: OtpCodeReq):
    with _otp_lock:
        secret, created = _otp_pending["secret"], _otp_pending["created_at"]
        if not secret or time.time() - created >= OTP_PENDING_TTL:
            _otp_pending.update({"secret": "", "created_at": 0.0})
            raise HTTPException(status_code=400, detail="换绑会话已过期，请重新生成")
        if not pyotp.TOTP(secret).verify(req.code.strip(), valid_window=1):
            raise HTTPException(status_code=403, detail="新动态口令不正确，请确认已用新验证器扫码或手动录入")
        OTP_SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = OTP_SECRET_FILE.with_suffix(".tmp")
        tmp.write_text(secret + "\n", encoding="utf-8")
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        os.replace(tmp, OTP_SECRET_FILE)
        _otp_pending.update({"secret": "", "created_at": 0.0})
    return {"status": "ok", "source": "custom", "message": "换绑成功，下次登录请使用新的动态口令"}

@app.post("/api/security/otp/cancel", dependencies=[Depends(verify_auth)])
def otp_cancel():
    with _otp_lock:
        _otp_pending.update({"secret": "", "created_at": 0.0})
    return {"status": "ok"}

@app.post("/api/security/otp/reset", dependencies=[Depends(verify_auth)])
def otp_reset(req: OtpCodeReq):
    """清除自定义密钥，恢复使用环境变量 OTP_SECRET（需验证当前生效的口令）。"""
    if not pyotp.TOTP(current_otp_secret()).verify(req.code.strip(), valid_window=1):
        raise HTTPException(status_code=403, detail="动态口令不正确")
    try:
        OTP_SECRET_FILE.unlink(missing_ok=True)
    except OSError:
        raise HTTPException(status_code=500, detail="无法清除自定义密钥")
    with _otp_lock:
        _otp_pending.update({"secret": "", "created_at": 0.0})
    return {"status": "ok", "source": "env", "message": "已恢复为环境变量密钥，请重新绑定原来的验证器"}

# ==================== TG 客户端 ====================

def build_pyrogram_client() -> Client:
    cfg = load_config()
    return Client(name=SESSION_NAME, api_id=int(cfg["api_id"]), api_hash=str(cfg["api_hash"]), workdir=str(SESSION_STORE_DIR), proxy=cfg.get("proxy"))

def get_login_state() -> dict:
    """TG 账号登录态（手机号 / 是否已授权 / 昵称），持久化在 config.yaml 的 _ui_login。

    放在配置里而不是内存里：容器重建、面板重启后登录状态提示不会退化成「未知」。
    """
    state = load_config().get("_ui_login") or {}
    return state if isinstance(state, dict) else {}

def update_login_state(**kwargs) -> dict:
    """合并写入登录态并原子落盘。"""
    cfg = load_config()
    state = dict(cfg.get("_ui_login") or {})
    state.update(kwargs)
    cfg["_ui_login"] = state
    dump_config(cfg)
    link_engine_idle()
    return state

_tg_status_cache = {"at": 0.0, "data": None}
TG_STATUS_TTL = 30  # 秒：连 Telegram DC 不便宜，短时间内不重复探测

@app.get("/api/tg/status", dependencies=[Depends(verify_auth)])
async def tg_status(force: bool = False):
    """TG 账号登录状态，供前端显示「已登录 / 未登录」提示。

    引擎运行中不再另起连接：Pyrogram 的 session 文件是 SQLite，
    与引擎同时打开会撞库锁，此时直接按「已登录 + 引擎占用」返回。
    """
    now = time.time()
    if not force and _tg_status_cache["data"] and now - _tg_status_cache["at"] < TG_STATUS_TTL:
        return _tg_status_cache["data"]

    state = get_login_state()
    phone = state.get("phone", "")
    session_exists = (SESSION_STORE_DIR / f"{SESSION_NAME}.session").exists()
    base = {
        "logged_in": bool(state.get("authorized")) or session_exists,
        "authorized": bool(state.get("authorized")),
        "phone": phone,
        "engine_busy": False,
        "user": state.get("user") or None,
        "login_at": state.get("at", 0),
    }
    if not session_exists:
        base["logged_in"] = False
        base["authorized"] = False
        _tg_status_cache.update({"at": now, "data": base})
        return base
    if engine_thread and engine_thread.is_alive():
        base["engine_busy"] = True
        _tg_status_cache.update({"at": now, "data": base})
        return base

    client = build_pyrogram_client()
    try:
        await client.connect()
        me = await client.get_me()
        base["logged_in"] = True
        base["authorized"] = True
        base["user"] = {
            "name": f"{me.first_name} {me.last_name or ''}".strip(),
            "id": me.id,
            "username": me.username or "",
        }
    except Exception:
        # 会话文件在但连不上：多半是登录态已失效
        base["logged_in"] = False
        base["authorized"] = False
        base["user"] = None
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
    _tg_status_cache.update({"at": now, "data": base})
    return base

class SendCodeReq(BaseModel):
    api_id: int
    api_hash: str
    phone: str
    # 代理随请求一起提交并立即落盘：用户无需先点「保存全部配置」再发验证码
    proxy: Optional[ProxyPayload] = None

@app.post("/api/tg/send_code", dependencies=[Depends(verify_auth)])
async def tg_send_code(req: SendCodeReq):
    phone = (req.phone or "").strip()
    api_hash = (req.api_hash or "").strip()
    if not phone:
        raise HTTPException(status_code=422, detail="请填写 Telegram 手机号（含国家代码，如 +8613800000000）")
    if not api_hash:
        raise HTTPException(status_code=422, detail="请先填写 API Hash")

    cfg = load_config()
    if req.proxy is not None:
        try:
            apply_proxy(cfg, req.proxy)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    cfg["api_id"], cfg["api_hash"] = req.api_id, api_hash
    # 手机号随「发送验证码」一并持久化：用户不必再点一次「保存全部配置」
    login_state = dict(cfg.get("_ui_login") or {})
    login_state["phone"] = phone
    cfg["_ui_login"] = login_state
    dump_config(cfg)
    link_engine_idle()

    # 清理上一次未完成的登录会话，避免残留连接占用代理
    old = pending_auth.get("client")
    if old:
        try:
            await old.disconnect()
        except Exception:
            pass
    pending_auth.update({"client": None, "phone": None, "phone_code_hash": None})

    client = build_pyrogram_client()
    try:
        await client.connect()
        sent = await client.send_code(phone)
    except Exception as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        # Pyrogram 连不上 DC 时抛内置 ConnectionError(OSError)，过去会变成 500
        # 且前端只弹「发送失败」，用户完全看不到原因——这里给出可操作的提示。
        if isinstance(e, (ConnectionError, OSError, TimeoutError)):
            detail = ("连不上 Telegram，通常是代理未配置或不可达。"
                      "请在「⚙️ 系统设置 → 🌐 代理设置」中填写代理并先点「测试连接」；"
                      "容器内 127.0.0.1 指向容器自己，需填宿主机 IP 或 host.docker.internal。"
                      f"（底层错误：{type(e).__name__}）")
        else:
            detail = f"Telegram 返回：{e}"
        raise HTTPException(status_code=400, detail=detail) from e

    pending_auth.update({"client": client, "phone": phone, "phone_code_hash": sent.phone_code_hash})
    return {"ok": True, "msg": "验证码已发送，请在下方输入"}

@app.post("/api/tg/proxy/test", dependencies=[Depends(verify_auth)])
async def test_proxy(req: ProxyPayload):
    """用与真实登录完全相同的 Pyrogram 链路验证代理，不碰账号、不发验证码、不触发风控。

    不用 httpx + 外部 URL 探测：那样测的是「代理能否访问某个网站」，与 Telegram DC
    链路并不等价，容易出现「测试说失败但 send_code 其实能用」的误判。
    """
    cfg = load_config()
    try:
        conf = build_proxy_conf(req, cfg.get("proxy"))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if not conf:
        return {"ok": True, "direct": True, "msg": "未启用代理，将直连 Telegram（境内通常连不上）"}

    def tcp_probe():
        s = socket.create_connection((conf["hostname"], conf["port"]), timeout=5)
        s.close()

    try:
        await asyncio.to_thread(tcp_probe)
    except OSError as e:
        raise HTTPException(
            status_code=400,
            detail=f"代理端口不可达 {conf['hostname']}:{conf['port']}：{e}。容器内请填宿主机 IP，不要填 127.0.0.1"
        )

    probe = Client(
        name="_proxy_probe",
        api_id=int(cfg.get("api_id") or 123456),
        api_hash=str(cfg.get("api_hash") or ""),
        workdir="/tmp",
        in_memory=True,   # 不写 session 文件，避免污染登录态
        proxy=conf,
    )
    try:
        await asyncio.wait_for(probe.connect(), timeout=20)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"代理端口通，但连不上 Telegram：{type(e).__name__}: {e}。请检查协议、账号密码与分流规则"
        )
    finally:
        try:
            await probe.disconnect()
        except Exception:
            pass
    return {"ok": True, "msg": f"代理可用，已成功连上 Telegram DC（{conf['scheme']}://{conf['hostname']}:{conf['port']}）"}

@app.post("/api/tg/auth/cancel", dependencies=[Depends(verify_auth)])
async def tg_auth_cancel():
    """放弃当前登录会话，允许重新发送验证码。"""
    client = pending_auth.get("client")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    pending_auth.update({"client": None, "phone": None, "phone_code_hash": None})
    return {"status": "ok"}

class VerifyCodeReq(BaseModel):
    code: str = ""
    password: str = ""

@app.post("/api/tg/verify_code", dependencies=[Depends(verify_auth)])
async def tg_verify_code(req: VerifyCodeReq):
    client = pending_auth.get("client")
    if not client:
        raise HTTPException(status_code=400, detail="会话超时")
    try:
        if req.code:
            try:
                await client.sign_in(pending_auth["phone"], pending_auth["phone_code_hash"], req.code.strip())
            except SessionPasswordNeeded:
                if not req.password:
                    return {"ok": False, "need_2fa": True, "msg": "请输入两步验证密码"}
                await client.check_password(req.password)
        elif req.password:
            await client.check_password(req.password)
        me = await client.get_me()
        await client.disconnect()
        pending_auth["client"] = None
        sync_session_files()
        # 记录登录态：账号昵称与手机号落盘，供「登录状态提示」直接读取
        update_login_state(
            authorized=True,
            phone=pending_auth.get("phone") or get_login_state().get("phone", ""),
            user={"name": (me.first_name or "").strip(), "id": me.id, "username": me.username or ""},
            at=time.time(),
        )
        _tg_status_cache.update({"at": 0.0, "data": None})
        return {"ok": True, "user": {"name": me.first_name}}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

async def fetch_dialog_list() -> List[dict]:
    """拉取 TG 侧全部频道/群组，按 chat_id 去重（同一 chat 只保留一条）。

    Pyrogram 的 get_dialogs 默认只返回 100 条，这里放开上限，
    否则账号里频道一多就采不全。replies=0 避免拉进群组回复消息。
    """
    client = build_pyrogram_client()
    dialogs, seen = [], set()
    try:
        await client.connect()
        async for d in client.get_dialogs(limit=1000):
            if d.chat.type not in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP):
                continue
            cid = d.chat.id
            if cid in seen:
                continue
            seen.add(cid)
            dialogs.append({
                "chat_id": cid,
                "title": d.chat.title or "未命名",
                "username": d.chat.username or "",
                "type": "频道" if d.chat.type == ChatType.CHANNEL else "群组",
            })
        return dialogs
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

@app.get("/api/tg/dialogs", dependencies=[Depends(verify_auth)])
async def tg_list_dialogs():
    try:
        dialogs = await fetch_dialog_list()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"拉取频道列表失败：{e}")
    cfg = load_config()
    existing = {str(c.get("chat_id")): c for c in cfg.get("chat", [])}
    for d in dialogs:
        c = existing.get(str(d["chat_id"]))
        d["added"] = c is not None
        d["last_read_message_id"] = int(c.get("last_read_message_id", 0)) if c else 0
        d["storage_id"] = c.get("_storage_id", "") if c else ""
    return {"ok": True, "dialogs": dialogs, "total": len(dialogs),
            "added_count": sum(1 for d in dialogs if d["added"])}

class CollectReq(BaseModel):
    chat_ids: List[int] = []
    storage_id: str = ""
    storage_path: str = ""
    start_id: int = 0      # 0 = 从最旧开始，补齐该频道全部历史
    start_engine: bool = False

@app.post("/api/tg/collect", dependencies=[Depends(verify_auth)])
async def tg_collect(req: CollectReq):
    """一键采集：把选中的频道批量加入监控队列。

    - 已存在于队列的频道自动跳过（重复信息不重复添加）
    - 落库前对整个 chat 列表按 chat_id 再去重一次
    - start_id=0 表示起始水位线归零，引擎会从最旧的历史消息一路补到最新
    """
    try:
        dialogs = await fetch_dialog_list()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"拉取频道列表失败：{e}")

    wanted = {str(int(x)) for x in (req.chat_ids or [])}
    cfg = load_config()
    existing = {str(c.get("chat_id")) for c in cfg.get("chat", [])}
    titles = dict(cfg.get("_ui_chat_titles", {}))
    added, skipped, not_found = [], [], []

    for d in dialogs:
        key = str(d["chat_id"])
        if key not in wanted:
            continue
        if key in existing:
            skipped.append({"chat_id": d["chat_id"], "title": d["title"], "reason": "已在队列中，已自动跳过"})
            continue
        entry = {"chat_id": d["chat_id"], "last_read_message_id": int(req.start_id)}
        if req.storage_id:
            entry["_storage_id"] = req.storage_id
        sub = clean_sub_path(req.storage_path)
        if sub:
            entry["_path"] = sub
        cfg.setdefault("chat", []).append(entry)
        titles[key] = d["title"]
        existing.add(key)
        added.append({"chat_id": d["chat_id"], "title": d["title"], "type": d["type"]})

    for x in wanted - {str(d["chat_id"]) for d in dialogs}:
        not_found.append(int(x))

    # 落库前统一去重：同一个 chat_id 只保留第一条
    seen, deduped = set(), []
    for c in cfg.get("chat", []):
        k = str(c.get("chat_id"))
        if k in seen:
            continue
        seen.add(k)
        deduped.append(c)
    removed = len(cfg.get("chat", [])) - len(deduped)
    cfg["chat"] = deduped
    cfg["_ui_chat_titles"] = titles
    dump_config(cfg)
    link_engine_idle()

    if req.start_engine:
        control_engine("start")

    return {
        "ok": True,
        "added": added,
        "skipped": skipped,
        "not_found": not_found,
        "dedup_removed": removed,
        "total": len(deduped),
        "start_id": int(req.start_id),
    }

# ==================== 一键采集历史（已绑定网盘的频道）====================
# 语义：把「监控队列里已绑定网盘」的频道的 last_read_message_id 归零，让引擎的
# offset_id 退化为 1，从该频道最旧的消息一路采到最新，并按频道绑定的网盘上传。
# 与「从账号批量添加频道」是两件事：前者采历史并上传，后者只是把频道建进队列。

class CollectHistoryReq(BaseModel):
    chat_ids: List[int] = []        # 空 = 全部已绑定网盘的频道
    reset_watermark: bool = True    # True: 归零重采全部历史；False: 只补水位线之后的增量
    start_engine: bool = True
    dry_run: bool = False

def _describe_bound_channels(cfg) -> dict:
    """把队列里的频道分成「已绑定网盘」和「未绑定」两类，并算出各自的落盘位置。"""
    storages_by_id = {s["id"]: s for s in cfg.get("_ui_storages", [])}
    title_map = cfg.get("_ui_chat_titles", {})
    base = cfg.get("save_path", "/app/save")
    bound, unbound = [], []
    for c in cfg.get("chat", []):
        cid = c.get("chat_id")
        title = title_map.get(str(cid), str(cid))
        sid = c.get("_storage_id", "")
        storage = storages_by_id.get(sid)
        sub = clean_sub_path(c.get("_path", ""))
        wm = int(c.get("last_read_message_id", 0) or 0)
        item = {
            "chat_id": cid,
            "title": title,
            "storage_id": sid,
            "storage_name": storage.get("name", sid) if storage else "",
            "sub_path": sub,
            "last_read_message_id": wm,
            "collected": wm > 0,
        }
        if storage:
            rbase = (storage.get("remote_dir", "tg_backup") or "tg_backup").strip("/") or "tg_backup"
            rel = "/".join(x for x in [rbase, sub, title, "YYYY_MM"] if x)
            item["remote_dir"] = f"{remote_name_of(sid)}:{rel}"
            item["remote_path"] = rel
            item["local_path"] = "/".join(x for x in [base, sub, title, "YYYY_MM"] if x)
            bound.append(item)
        else:
            item["reason"] = "未绑定网盘：采集后只留在本地，不会上传"
            unbound.append(item)
    return {"bound": bound, "unbound": unbound}

@app.get("/api/collect/history/preview", dependencies=[Depends(verify_auth)])
def collect_history_preview():
    """只读预览：哪些频道已绑定网盘、各自采到哪、会传到哪个路径。"""
    data = _describe_bound_channels(load_config())
    return {
        "bound": data["bound"],
        "unbound": data["unbound"],
        "bound_count": len(data["bound"]),
        "unbound_count": len(data["unbound"]),
        "pending": sum(1 for x in data["bound"] if not x["collected"]),
        "running": bool(engine_thread and engine_thread.is_alive()),
    }

@app.post("/api/collect/history", dependencies=[Depends(verify_auth)])
def collect_history(req: CollectHistoryReq):
    cfg = load_config()
    storage_ids = {s["id"] for s in cfg.get("_ui_storages", [])}
    title_map = cfg.get("_ui_chat_titles", {})
    wanted = {int(x) for x in req.chat_ids} if req.chat_ids else None
    touched, skipped = [], []
    for c in cfg.get("chat", []):
        cid = c.get("chat_id")
        if wanted is not None and cid not in wanted:
            continue
        title = title_map.get(str(cid), str(cid))
        sid = c.get("_storage_id", "")
        if not sid or sid not in storage_ids:
            skipped.append({"chat_id": cid, "title": title, "reason": "未绑定网盘，已跳过"})
            continue
        old = int(c.get("last_read_message_id", 0) or 0)
        new = 0 if req.reset_watermark else old
        c["last_read_message_id"] = new
        touched.append({"chat_id": cid, "title": title,
                        "old_watermark": old, "new_watermark": new})

    if not touched:
        raise HTTPException(status_code=422,
                            detail="没有可采集的频道：请先在「监控队列」里给频道绑定网盘")

    if req.dry_run:
        return {"ok": True, "dry_run": True, "channels": touched,
                "skipped": skipped, "engine_started": False, "total": len(touched)}

    # 引擎每组跑完会把组内水位线回写主配置；若此时直接归零，会被旧值覆盖 → 先停再改
    was_running = bool(engine_thread and engine_thread.is_alive())
    if was_running:
        control_engine("stop")
        if engine_thread:
            engine_thread.join(timeout=20)

    dump_config(cfg)
    link_engine_idle()

    started = False
    if req.start_engine:
        control_engine("start")
        started = True

    mode = "重采全部历史" if req.reset_watermark else "只补增量"
    engine_logs.append(
        f"[{time.strftime('%H:%M:%S')}] 🧲 一键采集历史：{len(touched)} 个频道（{mode}）"
        + ("，引擎已启动" if started else "")
    )
    return {
        "ok": True,
        "channels": touched,
        "skipped": skipped,
        "mode": mode,
        "engine_started": started,
        "was_running": was_running,
        "total": len(touched),
    }

# ==================== 配置 API ====================

class ConfigPayload(BaseModel):
    api_id: int
    api_hash: str
    save_path: str
    file_path_prefix: List[str] = ["chat_title", "media_datetime"]
    media_types: List[str] = ["photo", "video"]
    proxy_enable: bool = False
    proxy_scheme: str = "socks5"
    proxy_host: str = "127.0.0.1"
    proxy_port: int = 7890
    proxy_user: str = ""
    proxy_pass: str = ""
    login_phone: str = ""   # 登录设置里的手机号，持久化保存
    chats: List[dict] = []
    storages: List[dict] = []
    delete_after_upload: bool = False
    feishu_enable: bool = False
    feishu_webhook: str = ""
    feishu_secret: str = ""
    tgbot_enable: bool = False
    tgbot_token: str = ""
    tgbot_chat_id: str = ""
    notify_on_task_finish: bool = True
    notify_on_each_file: bool = False

@app.get("/api/config", dependencies=[Depends(verify_auth)])
def get_config():
    cfg = load_config()
    proxy, notify = cfg.get("proxy", {}), cfg.get("_ui_notify", {})
    title_map = cfg.get("_ui_chat_titles", {})
    existing_passes = get_existing_rclone_passes()

    chats_out = [{
        "chat_id": c.get("chat_id"),
        "title": title_map.get(str(c.get("chat_id")), ""),
        "last_read_message_id": c.get("last_read_message_id", 0),
        "storage_id": c.get("_storage_id", ""),
        "storage_path": c.get("_path", ""),
    } for c in cfg.get("chat", [])]

    storages_out = [{
        "id": s.get("id", ""),
        "name": s.get("name", ""),
        "type": normalize_storage_type(s.get("type")),
        "url": s.get("url", ""),
        "user": s.get("user", ""),
        "has_pass": bool(existing_passes.get(remote_name_of(s.get("id", "")))),
        "remote_dir": s.get("remote_dir", "tg_backup"),
        "upload_authorized": bool(s.get("upload_authorized", False)) if s.get("type") == "tgfs" else True,
    } for s in cfg.get("_ui_storages", [])]

    return {
        "api_id": cfg.get("api_id", 123456), "api_hash": cfg.get("api_hash", ""),
        "save_path": cfg.get("save_path", "/app/save"),
        "file_path_prefix": cfg.get("file_path_prefix", ["chat_title", "media_datetime"]),
        "media_types": cfg.get("media_types", ["photo", "video"]),
        # 登录设置：手机号持久化回显
        "login_phone": get_login_state().get("phone", ""),
        "proxy_enable": bool(proxy), "proxy_scheme": proxy.get("scheme", "socks5"),
        "proxy_host": proxy.get("hostname", "127.0.0.1"), "proxy_port": proxy.get("port", 7890),
        "proxy_user": proxy.get("username", ""),
        # 代理密码不回显到浏览器；留空提交表示沿用已保存的密码
        "proxy_pass": "", "proxy_has_pass": bool(proxy.get("password")),
        "chats": chats_out, "storages": storages_out,
        "delete_after_upload": cfg.get("_ui_delete_after_upload", False),
        "feishu_enable": notify.get("feishu_enable", False), "feishu_webhook": notify.get("feishu_webhook", ""),
        "feishu_secret": notify.get("feishu_secret", ""), "tgbot_enable": notify.get("tgbot_enable", False),
        "tgbot_token": notify.get("tgbot_token", ""), "tgbot_chat_id": notify.get("tgbot_chat_id", ""),
        "notify_on_task_finish": notify.get("notify_on_task_finish", True),
        "notify_on_each_file": notify.get("notify_on_each_file", False),
        "running": bool(engine_thread and engine_thread.is_alive()),
        # 采集记录：容器重启后 engine_logs 会清空，这两项是跨重启保留的
        "engine_autostart": bool(cfg.get("_ui_engine_autostart")),
        "last_run": cfg.get("_ui_last_run") or {},
    }

@app.post("/api/config", dependencies=[Depends(verify_auth)])
def save_config(payload: ConfigPayload):
    cfg = load_config()
    cfg["api_id"], cfg["api_hash"] = payload.api_id, payload.api_hash.strip()
    cfg["save_path"], cfg["media_types"] = payload.save_path, payload.media_types
    cfg["file_path_prefix"] = ["chat_title", "media_datetime"]

    # --- 登录设置：手机号持久化（与其它 _ui_ 状态同处一个键，避免散落）---
    login_state = dict(cfg.get("_ui_login") or {})
    login_state["phone"] = (payload.login_phone or "").strip()
    cfg["_ui_login"] = login_state

    # --- 多网盘（含 TGFS）---
    clean_storages = []
    for s in payload.storages:
        sid = s.get("id", "") or str(uuid.uuid4())[:8]
        stype = normalize_storage_type(s.get("type"))
        if stype not in ALLOWED_STORAGE_TYPES:
            raise HTTPException(status_code=422, detail=f"不支持的存储类型: {stype}")
        try:
            url = normalize_storage_url(stype, s.get("url", ""))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=f"网盘「{s.get('name') or sid}」：{exc}") from exc
        clean_storages.append({
            "id": sid,
            "name": s.get("name", "").strip() or f"网盘_{sid[:4]}",
            "type": stype,
            "url": url,
            "user": s.get("user", ""),
            "remote_dir": (s.get("remote_dir", "") or "tg_backup").strip("/") or "tg_backup",
            # TGFS 默认关闭自动上传，必须由用户显式勾选授权（设计文档 §5.3）
            "upload_authorized": bool(s.get("upload_authorized", False)) if stype == "tgfs" else True,
            "_plain_pass": s.get("pass", ""),
        })
    try:
        write_rclone_conf(clean_storages)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    # rclone.conf 变了，常驻的预览 serve 还握着旧凭据，必须重启并清掉目录缓存
    stop_lib_servers()
    cfg["_ui_storages"] = [{k: v for k, v in s.items() if k != "_plain_pass"} for s in clean_storages]
    cfg["_ui_delete_after_upload"] = payload.delete_after_upload

    # --- 频道 + 存储路由 ---
    clean_chats, title_map = [], {}
    storage_by_id = {s["id"]: s for s in clean_storages}
    seen_chat_ids = set()  # 同一个 chat_id 只保留一条，自动去掉重复
    for c in payload.chats:
        cid_raw = str(c.get("chat_id", "")).strip()
        if cid_raw == "":
            continue
        cid = int(cid_raw) if cid_raw.lstrip("-").isdigit() else cid_raw
        if cid in seen_chat_ids:
            continue
        seen_chat_ids.add(cid)
        sid = c.get("storage_id", "")
        if sid and sid not in storage_by_id:
            sid = ""
        if sid:
            target = storage_by_id[sid]
            if target["type"] == "tgfs" and not target["upload_authorized"]:
                raise HTTPException(
                    status_code=422,
                    detail=f"频道 {c.get('title') or cid} 指向未授权的 TGFS「{target['name']}」。"
                           "请先在网盘设置中勾选上传授权，或改选其它存储目标。"
                )
        entry = {"chat_id": cid, "last_read_message_id": int(c.get("last_read_message_id", 0))}
        if sid:
            entry["_storage_id"] = sid
        sub_path = clean_sub_path(c.get("storage_path", ""))
        if sub_path:
            entry["_path"] = sub_path
        clean_chats.append(entry)
        if c.get("title"):
            title_map[str(cid)] = c["title"]
    cfg["chat"], cfg["_ui_chat_titles"] = clean_chats, title_map

    # --- 代理（统一走 build_proxy_conf：校验协议/端口、密码留空保持不变）---
    try:
        apply_proxy(cfg, ProxyPayload(
            enable=payload.proxy_enable, scheme=payload.proxy_scheme,
            host=payload.proxy_host, port=payload.proxy_port,
            user=payload.proxy_user, password=payload.proxy_pass,
        ))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # 引擎运行时的 upload_drive 由分组逻辑按组动态生成，这里只留一个静态兜底
    cfg["upload_drive"] = {"enable_upload_file": False}

    cfg["_ui_notify"] = {
        "feishu_enable": payload.feishu_enable, "feishu_webhook": payload.feishu_webhook,
        "feishu_secret": payload.feishu_secret, "tgbot_enable": payload.tgbot_enable,
        "tgbot_token": payload.tgbot_token, "tgbot_chat_id": payload.tgbot_chat_id,
        "notify_on_task_finish": payload.notify_on_task_finish,
        "notify_on_each_file": payload.notify_on_each_file,
    }
    dump_config(cfg)
    link_engine_idle()
    return {"status": "saved", "storages_count": len(clean_storages)}

@app.get("/api/route_preview", dependencies=[Depends(verify_auth)])
def route_preview():
    """只读预览：按分组展示每个频道最终会存到哪里（不启动引擎）。"""
    cfg = load_config()
    storages, groups = build_groups(cfg)
    storages_by_id = {s["id"]: s for s in cfg.get("_ui_storages", [])}
    title_map = cfg.get("_ui_chat_titles", {})
    base = cfg.get("save_path", "/app/save")
    delete_after = cfg.get("_ui_delete_after_upload", False)
    out = []
    for (sid, sub), chats in groups.items():
        storage = storages.get(sid)
        upload = make_upload_drive(storage, delete_after, sub)
        out.append({
            "storage_id": sid,
            "storage_name": storage.get("name", sid) if storage else "本地存储",
            "sub_path": sub,
            "save_path": f"{base}/{sub}" if sub else base,
            "remote_dir": upload.get("remote_dir", ""),
            "chats": [{"chat_id": c.get("chat_id"), "title": title_map.get(str(c.get("chat_id")), "")} for c in chats],
            "lines": [describe_channel_target(c, title_map, storages_by_id, base) for c in chats],
        })
    return {"groups": out, "base_save_path": base}


# ==================== 网盘测试 ====================

class TestStorageReq(BaseModel):
    storage_id: str = ""

@app.post("/api/test_storage", dependencies=[Depends(verify_auth)])
def test_storage(req: TestStorageReq):
    sid = req.storage_id
    if not sid:
        return {"ok": False, "msg": "请先保存配置后再测试该网盘"}
    rname = remote_name_of(sid)
    remote_dir = "tg_backup"
    for s in load_config().get("_ui_storages", []):
        if s.get("id") == sid:
            remote_dir = s.get("remote_dir", "tg_backup")
            break
    cfg = load_config()
    stype = normalize_storage_type(
        next((s.get("type") for s in cfg.get("_ui_storages", []) if s.get("id") == sid), "webdav")
    )

    try:
        if stype == "tgfs":
            # TGFS 分支：只读列目录探测，绝不 mkdir 写入（设计文档 §5.4）
            res = subprocess.run([RCLONE_BIN, "lsd", f"{rname}:", "--timeout", "15s"],
                                 capture_output=True, text=True, timeout=20)
            if res.returncode != 0:
                # 不回显可能包含内部主机名/认证细节的 stderr
                return {"ok": False, "msg": "TGFS WebDAV 只读连接失败，请检查服务日志"}
            return {"ok": True, "msg": "TGFS WebDAV 只读连接测试成功（容量：未知）"}

        test_dir = f"{rname}:{remote_dir}"
        res = subprocess.run([RCLONE_BIN, "mkdir", test_dir, "--timeout", "15s"], capture_output=True, text=True)
        if res.returncode != 0:
            return {"ok": False, "msg": f"连接失败: {res.stderr.strip()}"}
        try:
            res2 = subprocess.run([RCLONE_BIN, "about", f"{rname}:", "--timeout", "8s"], capture_output=True, text=True)
            quota = res2.stdout.strip().replace("\n", " | ") if res2.returncode == 0 and res2.stdout.strip() else "未知"
        except Exception:
            quota = "未知"
        return {"ok": True, "msg": f"连接成功！({quota})"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "msg": "连接测试超时，请检查服务可用性"}
    except Exception:
        return {"ok": False, "msg": "连接测试异常，请检查服务日志"}

class TestTGFSReq(BaseModel):
    storage_id: str = ""


@app.post("/api/tgfs/test", dependencies=[Depends(verify_auth)])
def test_tgfs_endpoint(req: Optional[TestTGFSReq] = None):
    """设计文档 §5.4 兼容端点：对已配置的 TGFS 网盘做只读列目录探测。

    body 可省略（默认测第一个 TGFS），也可传 storage_id 指定某一个 TGFS。
    不向浏览器回显可能含内部主机名/凭据细节的 stderr。
    """
    tgfs = [s for s in load_config().get("_ui_storages", []) if s.get("type") == "tgfs"]
    if not tgfs:
        raise HTTPException(status_code=400, detail="当前未配置任何 TGFS 存储")
    sid = (req.storage_id if req and req.storage_id else "") or tgfs[0].get("id", "")
    if sid not in {s.get("id") for s in tgfs}:
        raise HTTPException(status_code=400, detail="指定的存储不是 TGFS 类型")
    result = test_storage(TestStorageReq(storage_id=sid))
    if not result.get("ok"):
        raise HTTPException(status_code=503, detail=result.get("msg") or "TGFS WebDAV 连接失败，请检查服务日志")
    return {"ok": True, "message": result["msg"]}


# ==================== TGFS 集成（面板内配置 + 代管 TGFS 容器）====================
#
# 官方 TGFS（github.com/TheodoreKrypton/tgfs）把 Telegram 私有频道当网盘，
# 对外提供 WebDAV。以前面板只支持「你自己去别处部署好，再把地址填进来」；
# 现在面板直接代管：生成它的 config.yaml、通过 Docker API 启停 tgfs 容器、
# 并自动把它的 WebDAV 挂成一个存储目标，用户不用再碰命令行。
#
# 几个绕不开的事实（都是实测/读源码确认的，改动前先看这里）：
#   * WebDAV 根下按 metadata.name 分一级目录，所以 rclone 的 remote_dir 是 `tgfs`，
#     完整路径是 /webdav/tgfs/...
#   * `private_file_channel` 必须是**列表**，`metadata` 必须是「channel_id -> 配置」
#     的映射；README 里的 demo-config.yaml 是旧格式，照抄会 KeyError。
#   * `metadata.type` 用 pinned_message 即可，不需要 GitHub 仓库和 token。
#   * 官方代码的 Pyrogram Client 不支持 proxy，境内直连 Telegram 必超时 ——
#     tgfs_patch/sitecustomize.py 会在容器里自动注入代理，不改它的源码。

TGFS_LOCAL_DIR = Path("/app/tgfs_data")          # 与 tgfs 容器的 /data 是同一个卷
TGFS_CONF_PATH = TGFS_LOCAL_DIR / "config.yaml"
TGFS_ACCOUNT_SESSION = TGFS_LOCAL_DIR / "account.session"   # 存 session_string 的纯文本
TGFS_CONTAINER = "tgfs"                          # 唯一允许操作的容器
TGFS_DEFAULT_PORT = 1900
TGFS_WEBDAV_PATH = "/webdav"
TGFS_META_NAME = "tgfs"                          # WebDAV 根下的一级目录 = metadata.name
TGFS_HOST_ALIAS = "host.docker.internal"         # tgfs 跑 host 网络，面板经此访问
DOCKER_SOCK = "/var/run/docker.sock"


def _tgfs_docker(action: str, timeout: float = 40.0):
    """对 tgfs 容器执行 start / stop / restart。

    面板一旦挂上 docker.sock 就等于有 root 权限，所以这里**不接受任何外部输入
    拼进 URL**：action 只认三个字面量，容器名硬编码。
    """
    if action not in ("start", "stop", "restart"):
        raise HTTPException(status_code=500, detail="内部错误：非法的容器操作")
    if not os.path.exists(DOCKER_SOCK):
        raise HTTPException(
            status_code=503,
            detail="面板没有挂载 docker.sock，无法直接控制 TGFS 容器。"
                   "请在 docker-compose.yml 里挂载 /var/run/docker.sock 后重建面板容器。",
        )
    try:
        transport = httpx.HTTPTransport(uds=DOCKER_SOCK)
        with httpx.Client(transport=transport, base_url="http://docker",
                          timeout=httpx.Timeout(timeout)) as c:
            return c.post(f"/containers/{TGFS_CONTAINER}/{action}",
                          params={"t": 15} if action in ("stop", "restart") else None)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Docker API 调用失败：{exc}") from exc


def _tgfs_container_state() -> dict:
    if not os.path.exists(DOCKER_SOCK):
        return {"exists": False, "running": False, "status": "no-docker-sock"}
    try:
        transport = httpx.HTTPTransport(uds=DOCKER_SOCK)
        with httpx.Client(transport=transport, base_url="http://docker",
                          timeout=httpx.Timeout(10.0)) as c:
            r = c.get(f"/containers/{TGFS_CONTAINER}/json")
    except httpx.HTTPError as exc:
        return {"exists": False, "running": False, "status": f"docker-error: {exc}"}
    if r.status_code == 404:
        return {"exists": False, "running": False, "status": "not-created"}
    if r.status_code != 200:
        return {"exists": False, "running": False, "status": f"error {r.status_code}"}
    state = (r.json() or {}).get("State") or {}
    return {
        "exists": True,
        "running": bool(state.get("Running")),
        "status": state.get("Status") or "unknown",
        "exit_code": state.get("ExitCode"),
        "started_at": (state.get("StartedAt") or "")[:19],
    }


def _tgfs_webdav_probe(port: int, user: str, password: str) -> dict:
    """对 WebDAV 根发一次 PROPFIND，确认它真的能用（而不只是端口活着）。

    用 host.docker.internal 而不是 127.0.0.1：tgfs 在 host 网络，面板在桥接网络，
    面板里的 127.0.0.1 是它自己。
    """
    url = f"http://{TGFS_HOST_ALIAS}:{port}{TGFS_WEBDAV_PATH}/"
    try:
        with httpx.Client(timeout=httpx.Timeout(8.0), trust_env=False) as c:
            r = c.request("PROPFIND", url, headers={"Depth": "0"},
                          auth=(user or "", password or ""))
    except httpx.HTTPError as exc:
        return {"ok": False, "status": 0, "msg": f"连不上：{type(exc).__name__}"}
    if r.status_code == 207:
        return {"ok": True, "status": 207, "msg": "WebDAV 可用"}
    if r.status_code == 401:
        return {"ok": False, "status": 401, "msg": "WebDAV 在，但用户名或密码不对"}
    return {"ok": False, "status": r.status_code, "msg": f"WebDAV 返回 HTTP {r.status_code}"}


def _suggest_tgfs_proxy() -> str:
    """把面板已配的代理翻译成 TGFS 能用的地址。

    TGFS 跑在 host 网络，所以容器网关那种 172.x.0.1 要换回 127.0.0.1。
    """
    conf = load_config().get("proxy") or {}
    if not conf.get("hostname"):
        return ""
    host = conf["hostname"]
    if host.startswith("172.") and host.endswith(".0.1"):
        host = "127.0.0.1"
    scheme = conf.get("scheme") or "socks5"
    auth = ""
    if conf.get("username"):
        # 代理密码常含 @ : 等字符，必须百分号编码，否则会被当成 host 的一部分
        auth = f"{quote(conf['username'], safe='')}:{quote(conf.get('password', ''), safe='')}@"
    return f"{scheme}://{auth}{host}:{conf.get('port') or 1080}"


def normalize_tgfs_channel(raw) -> str:
    """把用户填的频道标识归一成 TGFS 要的形状。

    TGFS 的 resolve_channel_id 是这么写的：
        try:
            return int(f"-100{channel_id}")
        except ValueError:
            resolve_peer(f"@{channel_id}")

    也就是说 `private_file_channel` 要的是频道 ID 的**纯数字部分**：
      "3807758162"      -> int("-1003807758162") = -1003807758162   ✓
      "-1003807758162"  -> int("-100-1003807758162") 抛 ValueError
                        -> 退化去 resolve_peer("@-1003807758162") -> PeerIdInvalid ✗

    面板的输入框写的是「ID 或 @username」，用户几乎必然填完整 ID，
    所以这里必须把 `-100` 前缀剥掉；填 username 的去掉 @ 走另一条分支。
    """
    s = str(raw or "").strip()
    if s.startswith("-100"):
        s = s[4:]
    elif s.startswith("@"):
        s = s[1:]
    return s


def export_tgfs_account_session() -> str:
    """把面板已登录的用户账号导出成 session_string，供 TGFS 读置顶消息。

    为什么必须要这一步：TGFS 的 `metadata.type = pinned_message` 是拿频道的
    **置顶消息**存文件索引的，而 Bot API 读不了置顶消息 ——
    `tgfs/core/api/message/__init__.py` 里只要 `tdlib_api.account is None`
    就直接抛 PinnedMessageNotSupported。所以必须配 `telegram.account`。

    而 `telegram.account.session_file` 要的东西和直觉不一样：它不是 Pyrogram 的
    `.session` SQLite 文件，而是 `Session.get()` 直接 `read().strip()` 的
    **纯文本 session_string**。所以这里导出的是一行字符串。

    导出时复制一份 session 副本再连：直接开原文件会和正在跑的引擎抢 SQLite 锁。
    """
    src = SESSION_STORE_DIR / f"{SESSION_NAME}.session"
    if not src.exists():
        raise RuntimeError("面板还没有登录 Telegram 账号，TGFS 无法读写频道置顶消息")

    cfg = load_config()
    api_id = int(cfg.get("api_id") or 0)
    api_hash = str(cfg.get("api_hash") or "")
    if not api_id or not api_hash:
        raise RuntimeError("缺少 api_id / api_hash，请先在「登录与频道」里配好")
    proxy = cfg.get("proxy") or None

    tmpdir = tempfile.mkdtemp(prefix="tgfs_sess_")
    try:
        shutil.copy2(src, os.path.join(tmpdir, f"{SESSION_NAME}.session"))

        async def _export() -> str:
            c = Client(name=SESSION_NAME, api_id=api_id, api_hash=api_hash,
                       workdir=tmpdir, proxy=proxy)
            await c.start()
            try:
                return await c.export_session_string()
            finally:
                await c.stop()

        sess = asyncio.run(_export())
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    if not sess:
        raise RuntimeError("导出 session_string 为空，请重新登录 Telegram 账号")
    TGFS_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    TGFS_ACCOUNT_SESSION.write_text(sess, encoding="utf-8")
    # tgfs 容器里是非 root 用户，600 会 Permission denied
    os.chmod(TGFS_ACCOUNT_SESSION, 0o644)
    return sess


def build_tgfs_config(t: dict) -> dict:
    """把面板里的字段翻译成 TGFS 的 config.yaml 结构。"""
    channel = normalize_tgfs_channel(t.get("channel"))
    cfg = {
        "telegram": {
            "api_id": int(t.get("api_id") or 0),
            "api_hash": str(t.get("api_hash") or "").strip(),
            "bot": {
                "session_file": "bot.session",
                "tokens": [str(t.get("bot_token") or "").strip()],
            },
            "private_file_channel": [channel],
            "lib": "pyrogram",
            # pinned_message 模式必须有用户账号（bot 读不了置顶消息）。
            # session_file 用相对路径，TGFS 的 expand_path 会拼成 /data/account.session；
            # 两个 used_to_* 都关掉 —— 上传下载仍走 bot，账号只用来读索引。
            "account": {
                "session_file": "account.session",
                "used_to_upload": False,
                "used_to_download": False,
            },
        },
        "tgfs": {
            "users": {
                (str(t.get("username") or "").strip() or "admin"): {
                    "password": t.get("password") or "",
                    "readonly": bool(t.get("readonly")),
                }
            },
            "download": {"chunk_size_kb": 1024},
            "jwt": {
                "secret": t.get("jwt_secret") or "",
                "algorithm": "HS256",
                "life": 604800,
            },
            "metadata": {
                channel: {"name": TGFS_META_NAME, "type": "pinned_message"}
            },
            "server": {
                "host": "0.0.0.0",
                "port": int(t.get("port") or TGFS_DEFAULT_PORT),
            },
        },
    }
    proxy = (t.get("proxy") or "").strip()
    if proxy:
        cfg["telegram"]["proxy"] = proxy
    return cfg


def write_tgfs_config(t: dict):
    TGFS_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    tmp = TGFS_CONF_PATH.with_suffix(".yaml.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        _yaml_dump(build_tgfs_config(t), f)
        f.flush()
        os.fsync(f.fileno())
    # 644 而不是 600：tgfs 容器里是非 root 用户（uid 1000 的 tgfs）在读这份文件，
    # root:root 600 它会直接 Permission denied。目录本身在宿主私有目录下。
    try:
        os.chmod(tmp, 0o644)
    except OSError:
        pass
    os.replace(tmp, TGFS_CONF_PATH)


def sync_tgfs_storage(cfg: dict, t: dict) -> str:
    """把面板代管的 TGFS 同步成一条 storage，返回它的 sid。

    用 _ui_tgfs.storage_id 记住自己那条，避免每次保存都新建一条。
    """
    port = int(t.get("port") or TGFS_DEFAULT_PORT)
    sid = (t.get("storage_id") or "").strip() or str(uuid.uuid4())[:8]
    storages = list(cfg.get("_ui_storages") or [])
    entry = next((s for s in storages if s.get("id") == sid), None)
    if entry is None:
        entry = {"id": sid}
        storages.append(entry)
    entry.update({
        "id": sid,
        "name": t.get("storage_name") or "TGFS",
        "type": "tgfs",
        "url": f"http://{TGFS_HOST_ALIAS}:{port}{TGFS_WEBDAV_PATH}",
        "user": str(t.get("username") or "").strip() or "admin",
        "remote_dir": TGFS_META_NAME,
        # 用户是在面板里主动配的 TGFS，视为已显式授权自动上传
        "upload_authorized": True,
        "_managed_by": "tgfs",
    })
    cfg["_ui_storages"] = storages
    return sid


class TGFSConfigReq(BaseModel):
    api_id: int
    api_hash: str
    bot_token: str = ""
    channel: str
    username: str = "admin"
    password: str = ""
    readonly: bool = False
    port: int = TGFS_DEFAULT_PORT
    proxy: str = ""
    storage_name: str = "TGFS"
    restart: bool = True


@app.get("/api/tgfs/status", dependencies=[Depends(verify_auth)])
def tgfs_status():
    cfg = load_config()
    t = cfg.get("_ui_tgfs") or {}
    port = int(t.get("port") or TGFS_DEFAULT_PORT)
    user = str(t.get("username") or "").strip() or "admin"
    conf_exists = TGFS_CONF_PATH.exists()
    state = _tgfs_container_state()
    webdav = {"ok": False, "status": 0, "msg": "未运行"}
    if state.get("running"):
        webdav = _tgfs_webdav_probe(port, user, t.get("password") or "")
    return {
        "configured": bool(t.get("bot_token") and t.get("channel") and t.get("api_hash")),
        "api_id": int(t.get("api_id") or cfg.get("api_id") or 0),
        "api_hash": t.get("api_hash") or cfg.get("api_hash") or "",
        "has_bot_token": bool(t.get("bot_token")),
        "channel": t.get("channel") or "",
        "username": user,
        "has_password": bool(t.get("password")),
        "readonly": bool(t.get("readonly")),
        "port": port,
        "proxy": t.get("proxy") or "",
        "suggest_proxy": _suggest_tgfs_proxy(),
        "storage_id": t.get("storage_id") or "",
        "storage_name": t.get("storage_name") or "TGFS",
        "conf_exists": conf_exists,
        "conf_path": str(TGFS_CONF_PATH),
        "docker_ok": os.path.exists(DOCKER_SOCK),
        "container": state,
        "webdav": webdav,
        "url": f"http://{TGFS_HOST_ALIAS}:{port}{TGFS_WEBDAV_PATH}",
    }


@app.post("/api/tgfs/config", dependencies=[Depends(verify_auth)])
def tgfs_save_config(req: TGFSConfigReq):
    """保存 TGFS 配置：写 config.yaml → 同步存储目标 → 重启容器。"""
    cfg = load_config()
    old = dict(cfg.get("_ui_tgfs") or {})
    # Token / 密码框留空都表示沿用已保存的值（前端不回显这两项）
    bot_token = (req.bot_token or "").strip() or (old.get("bot_token") or "")
    api_hash = (req.api_hash or "").strip() or (old.get("api_hash") or "")
    channel = str(req.channel or "").strip()
    username = (req.username or "").strip() or "admin"
    password = req.password or old.get("password", "")

    if not req.api_id or not api_hash:
        raise HTTPException(status_code=422, detail="api_id / api_hash 不能为空（可用「登录与频道」里已填的那组）")
    if not channel:
        raise HTTPException(status_code=422, detail="请填写用作存储的 Telegram 频道 ID 或 @username")
    if ":" not in bot_token:
        raise HTTPException(status_code=422,
                            detail="请填写 Bot Token，格式为「数字:字母串」，形如 123456789:AAxxxx")
    if not password:
        raise HTTPException(status_code=422, detail="请设置 WebDAV 密码，否则 TGFS 会拒绝所有连接")

    t = {
        "api_id": int(req.api_id),
        "api_hash": api_hash,
        "bot_token": bot_token,
        "password": password,
        "channel": channel,
        "username": username,
        "readonly": bool(req.readonly),
        "port": int(req.port or TGFS_DEFAULT_PORT),
        # 空值沿用已保存的代理，和 bot_token / password 同样的语义。
        # 之前是直接取 req.proxy，前端一旦没预填就会把代理清空 ——
        # TGFS 在境内没有代理根本连不上 Telegram，表现是满屏 Connection timed out。
        "proxy": (req.proxy or "").strip() or (old.get("proxy") or ""),
        "storage_name": (req.storage_name or "TGFS").strip() or "TGFS",
        # jwt secret 必须持久化，否则每次重启已签发的 token 全失效
        "jwt_secret": old.get("jwt_secret") or secrets.token_urlsafe(32),
        "storage_id": old.get("storage_id") or "",
    }

    # 先导出账号会话：TGFS 的 pinned_message 模式缺了它起不来（bot 读不了置顶消息）。
    # 放在写配置之前，失败就直接报错，避免留下「配置写了但容器起不来」的中间态。
    try:
        export_tgfs_account_session()
    except Exception as exc:                              # noqa: BLE001
        raise HTTPException(status_code=422,
                            detail=f"导出 Telegram 账号会话失败：{exc}") from exc

    try:
        write_tgfs_config(t)
    except Exception as exc:                              # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"写入 TGFS 配置失败：{exc}") from exc

    sid = sync_tgfs_storage(cfg, t)
    t["storage_id"] = sid
    cfg["_ui_tgfs"] = t

    # 同步 rclone.conf：把这条 TGFS 也写成一个 remote，预览库和上传都能直接用
    storages = [{**s, "_plain_pass": t["password"] if s.get("id") == sid else ""}
                for s in cfg.get("_ui_storages", [])]
    try:
        write_rclone_conf(storages)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    stop_lib_servers()
    dump_config(cfg)

    result = {"ok": True, "storage_id": sid, "config_path": str(TGFS_CONF_PATH),
              "url": f"http://{TGFS_HOST_ALIAS}:{t['port']}{TGFS_WEBDAV_PATH}"}
    if not req.restart:
        result["msg"] = "配置已保存，未重启容器"
        return result
    try:
        _tgfs_docker("restart")
        result["msg"] = "配置已保存，TGFS 正在重启（首次登录 Telegram 需要 10~40 秒）"
    except HTTPException as exc:
        # 容器还没创建过（首次 docker compose up 前）就退化成 start
        try:
            _tgfs_docker("start")
            result["msg"] = "配置已保存，TGFS 已启动（首次登录 Telegram 需要 10~40 秒）"
        except HTTPException:
            raise HTTPException(status_code=503,
                                detail=f"配置已保存，但没能重启 TGFS 容器：{exc.detail}") from exc
    return result


@app.post("/api/tgfs/restart", dependencies=[Depends(verify_auth)])
def tgfs_restart():
    # 这里**不再**因为「没有配置」就拒绝启动。
    # 容器在 compose 里带了一个等待循环（`while [ ! -f /data/config.yaml ]`），
    # 没配置时启动只是停在循环里空等，无害；但如果拒绝，就会出现死胡同：
    # 未配置时「停止」能点成功，「重启」却被 400 挡住，用户只能去命令行
    # `docker start tgfs` 才能把状态收回来。
    conf_ok = TGFS_CONF_PATH.exists()
    try:
        _tgfs_docker("restart")
    except HTTPException as exc:
        try:
            _tgfs_docker("start")
        except HTTPException:
            raise exc from None
    if not conf_ok:
        return {"ok": True,
                "msg": "TGFS 已启动，但还没生成配置，容器正等待你保存配置"}
    return {"ok": True, "msg": "TGFS 重启中"}


@app.post("/api/tgfs/stop", dependencies=[Depends(verify_auth)])
def tgfs_stop():
    _tgfs_docker("stop")
    # Docker 的 unless-stopped 语义：手动 stop 过的容器，宿主机重启后不会自动拉起
    return {"ok": True, "msg": "TGFS 已停止（重启 NAS 后不会自动恢复，需点「重启 TGFS」）"}


@app.get("/api/tgfs/logs", dependencies=[Depends(verify_auth)])
def tgfs_logs(tail: int = 120):
    """取 TGFS 容器最近日志，方便在面板里直接看它连不上 Telegram 之类的错误。"""
    if not os.path.exists(DOCKER_SOCK):
        raise HTTPException(status_code=503, detail="面板没有挂载 docker.sock")
    try:
        transport = httpx.HTTPTransport(uds=DOCKER_SOCK)
        with httpx.Client(transport=transport, base_url="http://docker",
                          timeout=httpx.Timeout(20.0)) as c:
            r = c.get(f"/containers/{TGFS_CONTAINER}/logs",
                      params={"stdout": 1, "stderr": 1, "tail": max(10, min(int(tail), 500))})
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Docker API 调用失败：{exc}") from exc
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="TGFS 容器还没创建")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"读取日志失败：HTTP {r.status_code}")
    # Docker 的日志流是多路复用格式：8 字节头（流类型 + 3 字节填充 + 4 字节长度）+ 正文
    raw, out, i = r.content, [], 0
    while i + 8 <= len(raw):
        size = int.from_bytes(raw[i + 4:i + 8], "big")
        body = raw[i + 8:i + 8 + size]
        out.append(body.decode("utf-8", "replace"))
        i += 8 + size
    text = "".join(out) if out else raw.decode("utf-8", "replace")
    return {"ok": True, "logs": text}



@app.post("/api/test_notify", dependencies=[Depends(verify_auth)])
def test_notify_api():
    res = send_notification("测试通知", ["✅ **消息通道正常！**", f"网盘状态: `{get_rclone_quota_summary()}`"])
    return {"ok": True, "msg": f"推送结果: {res}"}

# ==================== 引擎控制 ====================

def _start_engine(auto: bool = False) -> bool:
    """启动一轮采集；已在跑就返回 False。

    抽成函数是为了让「容器启动时自动补一轮」能复用同一套逻辑 ——
    走的是完全相同的路径，自动模式不会绕开任何安全检查。
    """
    global engine_thread
    if engine_thread is not None and engine_thread.is_alive():
        return False
    stop_event.clear()
    engine_logs.clear()
    session_stats["downloaded_count"] = 0
    session_stats["start_time"] = time.time()
    link_engine_idle()
    engine_thread = threading.Thread(target=run_all_groups, args=(auto,), daemon=True)
    engine_thread.start()
    return True


@app.post("/api/control/{action}", dependencies=[Depends(verify_auth)])
def control_engine(action: str):
    global engine_thread
    sync_session_files()
    if action == "start":
        _start_engine(auto=False)
        return {"running": True}
    elif action == "stop":
        stop_event.set()
        if downloader_proc and downloader_proc.poll() is None:
            downloader_proc.terminate()
        return {"running": False}
    elif action == "restart":
        stop_event.set()
        if downloader_proc and downloader_proc.poll() is None:
            downloader_proc.terminate()
        if engine_thread and engine_thread.is_alive():
            engine_thread.join(timeout=8)
        # 旧线程若在 join 超时后仍活着，_start_engine 会拒绝再起一个 ——
        # 原先这里是无条件新建线程，极端情况下会有两个 run_all_groups 并行跑。
        _start_engine(auto=False)
        return {"running": bool(engine_thread and engine_thread.is_alive())}
    raise HTTPException(status_code=400, detail="未知操作")

@app.get("/api/logs", dependencies=[Depends(verify_auth)])
def get_logs():
    # 只读一次配置：这个接口会被前端每几秒轮询一次，重复读盘没必要
    cfg = load_config()
    return {
        "running": bool(engine_thread and engine_thread.is_alive()),
        "logs": list(engine_logs), "stats": session_stats,
        "last_run": cfg.get("_ui_last_run") or {},
        "autostart": bool(cfg.get("_ui_engine_autostart")),
    }


@app.post("/api/engine/autostart", dependencies=[Depends(verify_auth)])
def set_engine_autostart(enabled: bool = False):
    """开关「容器启动后自动补一轮增量」。"""
    with _yaml_lock:
        cfg = load_config()
        cfg["_ui_engine_autostart"] = bool(enabled)
        dump_config(cfg)
    return {"ok": True, "autostart": bool(enabled)}


@app.on_event("startup")
def _autostart_engine_on_boot():
    """容器 / NAS 重启后自动补一轮**增量**采集。

    为什么是增量而不是重扫：引擎取消息用的是
    `offset_id=last_read_message_id` + `reverse=True`，即从上次记录的水位线
    往**更新**的方向取，之前采过的一律不再碰。水位线就是「采集记录」本身。
    唯一会把水位线归零的是「一键采集历史」按钮，那是用户显式点的特殊操作，
    自动启动走不到那条路。

    水位线为 0（从没采过）的频道会被 run_all_groups(auto=True) 跳过 ——
    那种频道跑起来等于全量拉历史，可能几百 GB，不该由重启替用户决定。
    """
    def _worker():
        # 面板起来后 rclone、网盘凭据、session 文件还要几秒才就绪；
        # 起太早会在第一组就报网络错误。
        time.sleep(10)
        try:
            cfg = load_config()
            if not cfg.get("_ui_engine_autostart"):
                return
            ready = [c for c in cfg.get("chat", [])
                     if int(c.get("last_read_message_id", 0) or 0) > 0]
            if not ready:
                engine_logs.append(
                    f"[{time.strftime('%H:%M:%S')}] ⚠ 自动采集已开启，但没有可增量补的频道"
                    "（水位线都是 0，需先手动跑一次建立采集记录）"
                )
                return
            sync_session_files()
            if _start_engine(auto=True):
                engine_logs.append(
                    f"[{time.strftime('%H:%M:%S')}] 🚀 容器启动，自动补采增量：{len(ready)} 个频道"
                )
        except Exception as exc:
            engine_logs.append(f"[{time.strftime('%H:%M:%S')}] ⚠ 自动采集启动失败：{exc}")

    threading.Thread(target=_worker, daemon=True).start()

# ==================== 预览库：直接读网盘，不读容器本地 ====================
# 引擎上传成功后默认会删掉本地文件（_ui_delete_after_upload），容器里本来就常年是空的；
# 而每个存储方案（网盘账号1 / 网盘账号2 / webdav1 / tgfs …）各自有一份远端目录，
# 所以预览库按「存储方案」分类，逐个 remote 去列目录、拉流。

LIB_SERVE_HOST = "127.0.0.1"
LIB_SERVE_BASE_PORT = 8901          # 每个存储一个只读 serve，端口按存储顺序递增
LIB_LIST_TTL = 180                  # 目录列举缓存（秒）；lsjson 递归列举 2000+ 文件约 7–11 秒
LIB_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif", ".heic"}
LIB_VIDEO_EXT = {".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts", ".flv"}
LIB_MEDIA_EXT = LIB_IMAGE_EXT | LIB_VIDEO_EXT

_lib_servers: dict = {}
_lib_list_cache: dict = {}
_lib_lock = threading.Lock()

# ---- 缩略图 ----
# 原图单张 5 秒（TTFB 1.8s、178KB，pCloud WebDAV 往返慢，不是带宽问题）。
# 一屏 120 张就要 100 秒以上，所以列表一律走缩略图 + 落盘缓存：
#   178KB → 约 25KB，第二次起直接读 NAS 本地盘（毫秒级）。
LIB_THUMB_DIR = Path("/app/config_store/thumbs")
LIB_THUMB_W = 400                   # 长边；格子显示 164px，2x 屏够用
LIB_THUMB_Q = 78
# 实测 pCloud WebDAV 是延迟瓶颈（单张 5s，TTFB 1.8s），并发越高吞吐越好：
# 4 并发 ≈ 0.35 张/秒，12 并发 ≈ 1.25 张/秒，24 并发开始被限流。
# 全局压到 14（含浏览器自己的 6 个连接），后台预热占 10。
LIB_THUMB_PARALLEL = 14
_lib_thumb_sem = threading.Semaphore(LIB_THUMB_PARALLEL)
_lib_thumb_inflight: dict = {}
_lib_thumb_lock = threading.Lock()


def _lib_storage(sid: str, cfg: dict) -> dict:
    st = next((s for s in cfg.get("_ui_storages", []) if s.get("id") == sid), None)
    if not st:
        raise HTTPException(status_code=404, detail="存储不存在，请先在「存储设置」里添加")
    return st


def _lib_port(sid: str, cfg: dict) -> int:
    ids = [s.get("id") for s in cfg.get("_ui_storages", [])]
    return LIB_SERVE_BASE_PORT + (ids.index(sid) if sid in ids else 0)


def stop_lib_servers():
    """存储配置变更后必须重启 serve：进程启动时就把凭据读进内存了。"""
    with _lib_lock:
        for rec in _lib_servers.values():
            proc = rec.get("proc")
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        _lib_servers.clear()
        _lib_list_cache.clear()


def ensure_lib_server(sid: str) -> int:
    """为某个存储懒启动一个只读 rclone serve http，返回本地端口。

    不用「每张图起一次 rclone cat」：那样每张图都要新建进程 + 重新建连，
    一屏几十张图就会把 WebDAV 打到限流。常驻 serve 复用连接，而且原生支持
    Range（视频能拖动进度条），面板只做流式转发。
    """
    cfg = load_config()
    _lib_storage(sid, cfg)
    port = _lib_port(sid, cfg)
    with _lib_lock:
        rec = _lib_servers.get(sid)
        if rec and rec.get("port") == port and rec.get("proc") and rec["proc"].poll() is None:
            return port
        if rec and rec.get("proc") and rec["proc"].poll() is None:
            try:
                rec["proc"].terminate()
            except Exception:
                pass
        proc = subprocess.Popen(
            [RCLONE_BIN, "serve", "http", f"{remote_name_of(sid)}:",
             "--addr", f"{LIB_SERVE_HOST}:{port}", "--read-only",
             "--log-level", "ERROR", "--dir-cache-time", "30s"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        _lib_servers[sid] = {"proc": proc, "port": port, "at": time.time()}
    for _ in range(60):
        try:
            with socket.create_connection((LIB_SERVE_HOST, port), timeout=0.4):
                return port
        except OSError:
            time.sleep(0.25)
    raise HTTPException(status_code=503, detail="存储预览服务启动超时，请检查该网盘的地址与凭据")


def _lib_scan(sid: str, st: dict, refresh: bool = False) -> list:
    """列出某存储 remote_dir 下所有媒体文件（带 TTL 缓存）。"""
    now = time.time()
    cached = _lib_list_cache.get(sid)
    if not refresh and cached and now - cached["at"] < LIB_LIST_TTL:
        return cached["files"]
    base = (st.get("remote_dir", "tg_backup") or "tg_backup").strip("/") or "tg_backup"
    spec = f"{remote_name_of(sid)}:{base}"
    try:
        res = subprocess.run([RCLONE_BIN, "lsjson", "-R", "--files-only", spec],
                             capture_output=True, text=True, timeout=240)
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="列举网盘目录超时，请稍后重试") from None
    if res.returncode != 0:
        tail = (res.stderr or "").strip().splitlines()
        raise HTTPException(status_code=502,
                            detail=f"列举网盘目录失败：{tail[-1] if tail else '未知错误'}")
    try:
        raw = json.loads(res.stdout or "[]")
    except ValueError:
        raw = []
    files = []
    for it in raw:
        if it.get("IsDir"):
            continue
        rel = (it.get("Path") or "").replace("\\", "/").lstrip("/")
        name = it.get("Name") or rel.rsplit("/", 1)[-1]
        ext = os.path.splitext(name)[1].lower()
        if ext not in LIB_MEDIA_EXT:
            continue
        files.append({
            "name": name,
            "path": rel,
            "dir": rel.rsplit("/", 1)[0] if "/" in rel else "",
            "type": "video" if ext in LIB_VIDEO_EXT else "image",
            "size_mb": round(int(it.get("Size") or 0) / 1048576, 2),
            # ISO8601 字符串按字典序排即按时间排，省一次解析
            "mtime": (it.get("ModTime") or "")[:19].replace("T", " "),
        })
    files.sort(key=lambda x: (x["mtime"], x["path"]), reverse=True)
    _lib_list_cache[sid] = {"at": now, "files": files}
    return files


def _safe_rel(path: str) -> str:
    p = (path or "").replace("\\", "/").strip().lstrip("/")
    if not p or ".." in p.split("/"):
        raise HTTPException(status_code=400, detail="非法文件路径")
    return p


def _lib_url(sid: str, st: dict, rel: str) -> str:
    """拼出 rclone serve 的本地 URL。

    serve 挂在 remote 根上，而 lsjson 返回的 Path 是相对 remote_dir 的，
    这里要把 remote_dir 前缀补回去，否则一律 404。
    """
    port = ensure_lib_server(sid)
    base = (st.get("remote_dir", "tg_backup") or "tg_backup").strip("/") or "tg_backup"
    return f"http://{LIB_SERVE_HOST}:{port}/" + quote(f"{base}/{rel}")


def _thumb_path(sid: str, rel: str, w: int) -> Path:
    key = hashlib.md5(f"{sid}|{rel}|{w}".encode("utf-8")).hexdigest()
    d = LIB_THUMB_DIR / key[:2]
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{key}.jpg"


def _fetch_remote_bytes(sid: str, st: dict, rel: str, timeout: float = 120.0) -> bytes:
    """从 rclone serve 取整个文件。

    trust_env=False：容器里配了 HTTP_PROXY（代理是给 Telegram 用的），
    走代理去访问 127.0.0.1 的 serve 会直接失败。
    """
    url = _lib_url(sid, st, rel)
    with httpx.Client(timeout=httpx.Timeout(30.0, read=timeout), trust_env=False) as c:
        r = c.get(url)
    if r.status_code != 200:
        raise RuntimeError(f"上游返回 {r.status_code}")
    return r.content


def _make_thumb(sid: str, st: dict, rel: str, w: int, dest: Path):
    data = _fetch_remote_bytes(sid, st, rel)
    im = Image.open(io.BytesIO(data))
    im = ImageOps.exif_transpose(im)
    if im.mode not in ("RGB", "L"):
        im = im.convert("RGB")
    im.thumbnail((w, w), Image.LANCZOS)
    tmp = dest.with_suffix(".jpg.tmp")
    im.save(tmp, "JPEG", quality=LIB_THUMB_Q, optimize=True, progressive=True)
    os.replace(tmp, dest)


def _gen_thumb(sid: str, st: dict, rel: str, w: int, dest: Path) -> bool:
    """生成一张缩略图；同一张图被并发请求时只让一个去取原图。

    浏览器一屏会同时发 6 个请求，滚动时同一张图也可能被重复请求。
    没有这层去重，同一个文件会被下载十几遍，pCloud 立刻限流。
    """
    if Image is None:
        return False
    key = str(dest)
    with _lib_thumb_lock:
        ev = _lib_thumb_inflight.get(key)
        owner = ev is None
        if owner:
            ev = threading.Event()
            _lib_thumb_inflight[key] = ev
    if not owner:
        ev.wait(timeout=180)
        return dest.exists() and dest.stat().st_size > 0
    try:
        with _lib_thumb_sem:
            _make_thumb(sid, st, rel, w, dest)
    except Exception as exc:
        print(f"[预览库] 缩略图生成失败 {rel}: {exc}")
        return False
    finally:
        with _lib_thumb_lock:
            _lib_thumb_inflight.pop(key, None)
        ev.set()
    return dest.exists() and dest.stat().st_size > 0


def _warm_thumbs(sid: str, st: dict, items: list, w: int = LIB_THUMB_W, workers: int = 10):
    """后台预热当前页的缩略图，用户往下滚时基本已经在盘上了。"""
    todo = [it["path"] for it in items if it.get("type") == "image"]
    if Image is None or not todo:
        return

    def worker(chunk):
        for rel in chunk:
            if stop_event.is_set():
                break
            try:
                dest = _thumb_path(sid, rel, w)
                if dest.exists() and dest.stat().st_size > 0:
                    continue
                _gen_thumb(sid, st, rel, w, dest)
            except Exception:
                continue

    # 轮转分片：若每个线程都从 todo[0] 开始遍历同一个列表，它们会全撞在
    # 同一张图上、被 _gen_thumb 的 in-flight 去重串行化，实际只有 1 个线程
    # 在干活（实测 6 线程预热 10 张要 18 秒，且 inflight 恒为 1）。
    n = max(1, min(workers, len(todo)))
    for i in range(n):
        threading.Thread(target=worker, args=(todo[i::n],), daemon=True).start()


@app.get("/api/library/sources", dependencies=[Depends(verify_auth)])
def library_sources():
    """预览库的分类：一个存储方案一项（网盘账号1 / 网盘账号2 / webdav1 / tgfs …）。"""
    cfg = load_config()
    title_map = cfg.get("_ui_chat_titles", {})
    passes = get_existing_rclone_passes()
    out = []
    for s in cfg.get("_ui_storages", []):
        sid = s.get("id", "")
        out.append({
            "id": sid,
            "name": s.get("name") or f"网盘_{sid[:4]}",
            "type": normalize_storage_type(s.get("type")),
            "remote_dir": s.get("remote_dir", "tg_backup"),
            "has_pass": bool(passes.get(remote_name_of(sid))),
            "chats": [title_map.get(str(c.get("chat_id")), str(c.get("chat_id")))
                      for c in cfg.get("chat", []) if c.get("_storage_id") == sid],
        })
    return {"sources": out, "count": len(out)}


@app.get("/api/library", dependencies=[Depends(verify_auth)])
def library_list(sid: str, q: str = "", kind: str = "", dir: str = "",
                 limit: int = 120, offset: int = 0, refresh: bool = False):
    cfg = load_config()
    st = _lib_storage(sid, cfg)
    files = _lib_scan(sid, st, refresh=refresh)

    kw = (q or "").strip().lower()
    if kw:
        files = [f for f in files if kw in f["name"].lower() or kw in f["dir"].lower()]
    if kind in ("image", "video"):
        files = [f for f in files if f["type"] == kind]

    # 路径分类（存储内部的一级目录），用于筛选 chips
    dir_count: dict = {}
    for f in files:
        top = f["dir"].split("/")[0] if f["dir"] else "(根目录)"
        dir_count[top] = dir_count.get(top, 0) + 1

    if dir:
        want = dir.strip("/")
        files = [f for f in files
                 if (f["dir"].split("/")[0] if f["dir"] else "(根目录)") == want]

    limit = max(1, min(int(limit or 120), 400))
    offset = max(0, int(offset or 0))
    page = files[offset:offset + limit]
    # 后台预热本页缩略图：返回列表时不阻塞，等浏览器来取时多半已经生成好了
    try:
        _warm_thumbs(sid, st, page)
    except Exception:
        pass
    return {
        "sid": sid,
        "name": st.get("name") or sid,
        "type": normalize_storage_type(st.get("type")),
        "remote_dir": st.get("remote_dir", "tg_backup"),
        "total": len(files),
        "offset": offset,
        "limit": limit,
        "has_more": offset + len(page) < len(files),
        "dirs": [{"name": k, "count": v}
                 for k, v in sorted(dir_count.items(), key=lambda x: -x[1])][:80],
        "items": page,
    }


@app.get("/api/library/thumb", dependencies=[Depends(verify_auth)])
async def library_thumb(sid: str, path: str, w: int = LIB_THUMB_W):
    """列表用缩略图。首次生成要回源取原图（约 5s），之后走盘上缓存。"""
    cfg = load_config()
    st = _lib_storage(sid, cfg)
    rel = _safe_rel(path)
    if os.path.splitext(rel)[1].lower() not in LIB_IMAGE_EXT:
        raise HTTPException(status_code=400, detail="只有图片有缩略图")
    w = max(96, min(int(w or LIB_THUMB_W), 800))
    dest = _thumb_path(sid, rel, w)
    if not (dest.exists() and dest.stat().st_size > 0):
        # 生成要等网络，别阻塞事件循环
        ok = await asyncio.to_thread(_gen_thumb, sid, st, rel, w, dest)
        if not ok:
            raise HTTPException(status_code=502, detail="缩略图生成失败")
    return FileResponse(str(dest), media_type="image/jpeg",
                        headers={"cache-control": "public, max-age=604800, immutable"})


@app.get("/api/library/raw", dependencies=[Depends(verify_auth)])
async def library_raw(sid: str, path: str, request: Request):
    """把网盘文件流式转发给浏览器，转发 Range 以支持视频拖动进度条。"""
    cfg = load_config()
    st = _lib_storage(sid, cfg)
    rel = _safe_rel(path)
    # ensure_lib_server 会阻塞（首次要等进程起监听），别在事件循环里等
    url = await asyncio.to_thread(_lib_url, sid, st, rel)

    fwd = {}
    rng = request.headers.get("range")
    if rng:
        fwd["range"] = rng

    client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=300.0))
    try:
        upstream = await client.send(client.build_request("GET", url, headers=fwd), stream=True)
    except httpx.HTTPError as exc:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"读取网盘文件失败：{exc}") from exc
    if upstream.status_code >= 400:
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(status_code=404, detail="网盘上找不到该文件")

    headers = {"cache-control": "private, max-age=86400"}
    for h in ("content-type", "content-length", "content-range",
              "accept-ranges", "last-modified", "etag"):
        v = upstream.headers.get(h)
        if v:
            headers[h] = v

    async def body():
        try:
            async for chunk in upstream.aiter_bytes(65536):
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(body(), status_code=upstream.status_code, headers=headers)

@app.get("/", response_class=HTMLResponse)
def index():
    return Path("/app/templates/index.html").read_text(encoding="utf-8")
