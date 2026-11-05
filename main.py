import os, re, shutil, subprocess, threading, time, mimetypes, configparser, hmac, hashlib, base64, uuid, ipaddress, secrets, socket, asyncio, copy
from collections import deque
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlsplit, quote
import pyotp, jwt, httpx
from ruamel.yaml import YAML
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import SessionPasswordNeeded

app = FastAPI(title="TG Media Downloader Pro Panel")
yaml = YAML()
yaml.preserve_quotes = True

CONFIG_PATH = Path("/app/config_store/config.yaml")
ENGINE_CONFIG_LINK = Path("/app/config.yaml")
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

ALLOWED_STORAGE_TYPES = {"infinicloud", "webdav", "smb", "tgfs"}

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
            yaml.dump(default_cfg, f)
    if ENGINE_CONFIG_LINK.exists() or ENGINE_CONFIG_LINK.is_symlink():
        ENGINE_CONFIG_LINK.unlink()
    ENGINE_CONFIG_LINK.symlink_to(CONFIG_PATH)

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
                cfg = yaml.load(f) or {}
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
    """
    tmp = CONFIG_PATH.with_suffix(".yaml.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CONFIG_PATH)

def link_engine_config(target: Path):
    """把 /app/config.yaml 指向指定配置文件（引擎按相对路径读取此文件）"""
    if ENGINE_CONFIG_LINK.exists() or ENGINE_CONFIG_LINK.is_symlink():
        ENGINE_CONFIG_LINK.unlink()
    ENGINE_CONFIG_LINK.symlink_to(target)

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
    if stype == "infinicloud":
        if not host_val.startswith("http"):
            host_val = f"https://{host_val}.teracloud.jp" if "." not in host_val else f"https://{host_val}"
        if not host_val.endswith("/dav"):
            host_val += "/dav"
        host_val += "/"
    elif stype == "webdav":
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
        stype = s.get("type", "webdav")
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
        if stype in ("infinicloud", "webdav", "tgfs"):
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

def run_all_groups():
    """按网盘分组串行运行下载引擎，实现真正的频道级存储路由"""
    global downloader_proc
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
                yaml.dump(run_cfg, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(gtmp, tmp)
            link_engine_config(tmp)

            proc = subprocess.Popen(
                ["python3", "-u", "media_downloader.py"], cwd="/app",
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
            )
            downloader_proc = proc
            for raw_line in iter(proc.stdout.readline, ""):
                line = raw_line.strip()
                if not line:
                    continue
                engine_logs.append(f"[{time.strftime('%H:%M:%S')}] {line}")
                if any(k in line.lower() for k in ["successfully downloaded", "下载成功", "成功下载", "upload file"]):
                    session_stats["downloaded_count"] += 1
            proc.wait()

            # 回写本组频道的水位线到主配置
            try:
                with open(tmp, "r", encoding="utf-8") as f:
                    updated = yaml.load(f) or {}
                id_map = {str(u.get("chat_id")): int(u.get("last_read_message_id", 0)) for u in updated.get("chat", [])}
                main_cfg = load_config()
                for c in main_cfg.get("chat", []):
                    key = str(c.get("chat_id"))
                    if key in id_map:
                        c["last_read_message_id"] = id_map[key]
                dump_config(main_cfg)
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
        link_engine_config(CONFIG_PATH)

        elapsed = round(time.time() - session_stats["start_time"], 1)
        engine_logs.append(f"[{time.strftime('%H:%M:%S')}] 🏁 全部任务结束 (耗时 {elapsed}s)")

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
        link_engine_config(CONFIG_PATH)

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
    link_engine_config(CONFIG_PATH)
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
    link_engine_config(CONFIG_PATH)

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
    link_engine_config(CONFIG_PATH)

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
    link_engine_config(CONFIG_PATH)

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
        "type": s.get("type", "webdav"),
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
        stype = s.get("type", "webdav")
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
    link_engine_config(CONFIG_PATH)
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
    stype = next((s.get("type") for s in cfg.get("_ui_storages", []) if s.get("id") == sid), "webdav")

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


@app.post("/api/test_notify", dependencies=[Depends(verify_auth)])
def test_notify_api():
    res = send_notification("测试通知", ["✅ **消息通道正常！**", f"网盘状态: `{get_rclone_quota_summary()}`"])
    return {"ok": True, "msg": f"推送结果: {res}"}

# ==================== 引擎控制 ====================

@app.post("/api/control/{action}", dependencies=[Depends(verify_auth)])
def control_engine(action: str):
    global engine_thread
    sync_session_files()
    if action == "start":
        if engine_thread is None or not engine_thread.is_alive():
            stop_event.clear()
            engine_logs.clear()
            session_stats["downloaded_count"] = 0
            session_stats["start_time"] = time.time()
            link_engine_config(CONFIG_PATH)
            engine_thread = threading.Thread(target=run_all_groups, daemon=True)
            engine_thread.start()
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
        stop_event.clear()
        engine_logs.clear()
        session_stats["downloaded_count"] = 0
        session_stats["start_time"] = time.time()
        link_engine_config(CONFIG_PATH)
        engine_thread = threading.Thread(target=run_all_groups, daemon=True)
        engine_thread.start()
        return {"running": True}
    raise HTTPException(status_code=400, detail="未知操作")

@app.get("/api/logs", dependencies=[Depends(verify_auth)])
def get_logs():
    return {
        "running": bool(engine_thread and engine_thread.is_alive()),
        "logs": list(engine_logs), "stats": session_stats,
    }

@app.get("/api/media", dependencies=[Depends(verify_auth)])
def list_media():
    root = Path(load_config().get("save_path", "/app/save"))
    if not root.exists():
        return []
    files = []
    for p in sorted(root.rglob("*"), key=lambda x: x.stat().st_mtime if x.is_file() else 0, reverse=True):
        if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".mkv", ".mov", ".webm"}:
            files.append({
                "name": p.name, "rel_path": p.relative_to(root).as_posix(),
                "type": "video" if p.suffix.lower() in {".mp4", ".mkv", ".mov", ".webm"} else "image",
                "size_mb": round(p.stat().st_size / (1024*1024), 2),
                "mtime": time.strftime("%Y-%m-%d %H:%M", time.localtime(p.stat().st_mtime)),
            })
            if len(files) >= 200:
                break
    return files

@app.get("/api/stream", dependencies=[Depends(verify_auth)])
def stream_media(path: str):
    root = Path(load_config().get("save_path", "/app/save")).resolve()
    target = (root / path).resolve()
    if not str(target).startswith(str(root)) or not target.is_file():
        raise HTTPException(status_code=404)
    mime, _ = mimetypes.guess_type(str(target))
    return FileResponse(target, media_type=mime or "application/octet-stream")

@app.get("/", response_class=HTMLResponse)
def index():
    return Path("/app/templates/index.html").read_text(encoding="utf-8")
