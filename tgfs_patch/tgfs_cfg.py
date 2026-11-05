"""从 TGFS 的 config.yaml 里读脚本要用的连接参数。

**不要把 api_id / api_hash / 代理账号密码 / bot token 硬编码进脚本** ——
config.yaml 里这几样都有，硬编码等于把它们提交进 git 仓库。
（踩过一次：迁移脚本第一版直接写死了 API_HASH 和代理密码。）

同时负责推断另外两个「脚本自己知道不了」的东西：
  - 存储频道：config.yaml 的 tgfs.metadata 键就是去掉 `-100` 前缀的频道 id
  - 索引频道：优先环境变量 TGFS_FD_CHANNEL，其次迁移时落盘的 fd_id_mapping.json
"""
import json
import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml

DATA_DIR = Path("/app/tgfs_data")
CONFIG = DATA_DIR / "config.yaml"
MIGRATION_DIR = DATA_DIR / "_migration"


def load() -> dict:
    cfg = yaml.safe_load(CONFIG.read_text(encoding="utf-8")) or {}
    tg = cfg.get("telegram") or {}

    proxy = None
    if raw_proxy := (tg.get("proxy") or "").strip():
        u = urlsplit(raw_proxy)
        proxy = {
            "scheme": u.scheme,
            "hostname": u.hostname,
            "port": u.port,
        }
        if u.username:
            proxy["username"] = unquote(u.username)
            proxy["password"] = unquote(u.password or "")

    return {
        "api_id": int(tg["api_id"]),
        "api_hash": tg["api_hash"],
        "proxy": proxy,
        "storage": storage_channel(cfg),
        "index": index_channel(),
    }


def storage_channel(cfg: dict) -> int:
    """config.yaml 里 metadata 的键就是频道 id 去掉 `-100` 前缀。"""
    keys = list((cfg.get("tgfs") or {}).get("metadata") or {})
    if not keys:
        raise SystemExit("config.yaml 里没有 tgfs.metadata，无法确定存储频道")
    return int(f"-100{keys[0]}")


def index_channel():
    raw = (os.environ.get("TGFS_FD_CHANNEL") or "").strip()
    if raw.lstrip("-").isdigit():
        return int(raw)
    mapping = MIGRATION_DIR / "fd_id_mapping.json"
    if mapping.exists():
        return json.loads(mapping.read_text(encoding="utf-8")).get("index")
    return None


def bot_session_file() -> Path:
    """TGFS 的 bot 登录态。是**纯文本 session_string**，不是 SQLite session 文件。"""
    files = sorted((DATA_DIR / "bot.session").glob("*.session"))
    if not files:
        raise SystemExit(
            "找不到 TGFS 的 bot session（tgfs_data/bot.session/*.session）；"
            "先启动一次 tgfs 容器让它自己登录"
        )
    return files[0]


def bot_session_string() -> str:
    raw = bot_session_file().read_text(encoding="utf-8").strip()
    if not raw:
        raise SystemExit(f"{bot_session_file()} 是空的")
    return raw
