"""给 TGFS 容器里的 Pyrogram 注入 SOCKS5 / HTTP 代理。

官方镜像 wheatcarrier/tgfs 的 `Client(...)` 构造里没有 proxy 参数，而境内网络
直连 Telegram 会一直 `Connection timed out`（实测 DC2 连了 3 轮全超时），
TGFS 于是永远起不来、WebDAV 端口也不监听。

这个文件放在 site-packages/sitecustomize.py，Python 解释器启动时会自动导入，
所以**不需要改 TGFS 任何源码** —— 升级基础镜像也不会失效（对比 patch/ 下
那两份覆盖引擎源码的文件，那种方式每次升级都要重新比对 md5）。

代理地址从 TGFS 自己的配置文件读 `telegram.proxy`，两种写法都支持：

    telegram:
      proxy: socks5://127.0.0.1:1080

    telegram:
      proxy:
        scheme: socks5      # socks5 / socks4 / http
        hostname: 127.0.0.1
        port: 1080
        username: u         # 可选
        password: p         # 可选

留空或读不到就完全不注入，行为与官方镜像一致。
"""

import logging
import os
import re

_log = logging.getLogger("tgfs.proxy_patch")

# 注意：sitecustomize 在解释器启动早期执行，此时 sys.path[0] 还没被设成脚本目录，
# 所以这里**不能** import tgfs.config，只能按它的同一套规则自己算路径。
# 规则见 tgfs/config.py：DATA_DIR = TGFS_DATA_DIR 或 ~/.tgfs，配置名取 TGFS_CONFIG_FILE。
_DATA_DIR = os.environ.get("TGFS_DATA_DIR") or os.path.expanduser("~/.tgfs")
_CONF_NAME = os.environ.get("TGFS_CONFIG_FILE") or "config.yaml"
_CONF_PATH = os.path.join(_DATA_DIR, _CONF_NAME)


def _read_proxy():
    try:
        import yaml

        with open(_CONF_PATH, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return None
    except Exception as exc:                                  # noqa: BLE001
        _log.warning("代理补丁：读取 %s 失败（%s），按无代理启动", _CONF_PATH, exc)
        return None

    p = (raw.get("telegram") or {}).get("proxy")
    if not p:
        return None

    if isinstance(p, str):
        from urllib.parse import unquote, urlsplit

        u = urlsplit(p if "://" in p else "socks5://" + p)
        if not u.hostname:
            _log.warning("代理补丁：%r 里解析不出主机名，忽略", p)
            return None
        d = {
            "scheme": (u.scheme or "socks5").lower(),
            "hostname": u.hostname,
            "port": u.port or 1080,
        }
        # 必须 unquote：面板生成地址时会把密码里的 @ : 之类百分号编码
        # （代理密码常含这些字符），而 urlsplit 取出来的是编码后的原样字符串，
        # 直接喂给 Pyrogram 会一直「SOCKS5 authentication failed」。
        if u.username:
            d["username"] = unquote(u.username)
        if u.password:
            d["password"] = unquote(u.password)
        return d

    if isinstance(p, dict):
        return dict(p)

    _log.warning("代理补丁：telegram.proxy 类型不支持（%s），忽略", type(p).__name__)
    return None


def _install():
    proxy = _read_proxy()
    if not proxy:
        return
    try:
        import pyrogram
    except Exception as exc:                                  # noqa: BLE001
        _log.warning("代理补丁：import pyrogram 失败（%s），跳过", exc)
        return

    orig = pyrogram.Client.__init__
    if getattr(orig, "_tgfs_proxy_patched", False):
        return

    def __init__(self, *args, **kwargs):
        # 调用方若自己传了 proxy 就尊重调用方
        kwargs.setdefault("proxy", proxy)
        return orig(self, *args, **kwargs)

    __init__._tgfs_proxy_patched = True
    pyrogram.Client.__init__ = __init__
    _log.info(
        "代理补丁已生效：Pyrogram 经 %s://%s:%s 连接 Telegram",
        proxy.get("scheme"), proxy.get("hostname"), proxy.get("port"),
    )


_install()


# ==================== 文件名去 "[partN]" 前缀 ====================
# TGFS 的 core/repository/impl/file_content/__init__.py:86 无条件执行
#     file_msg.name = f"[part{i+1}]{file_name}"
# 而分片阈值 PART_SIZE_DEFAULT 是 2GB —— 实际文件只有几十 MB，永远只有一个分片，
# 于是频道里每个文件都叫 "[part1]xxx.MOV"，这个前缀一点信息量都没有。
#
# 同样不改 TGFS 源码：TGFS 上传走 messages.SendMedia + InputMediaUploadedDocument，
# 文件名最终落在 DocumentAttributeFilename.file_name 上，包一层构造就够。
# 索引靠 message_id 而不是名字（_validate_fv 只读 document.size），
# 所以这里只影响 Telegram 里的显示效果，不影响 WebDAV 读写。
_PART_PREFIX = re.compile(r"^\[part\d+\]")


def _install_filename_patch():
    try:
        from pyrogram.raw.types import DocumentAttributeFilename
    except Exception as exc:                                  # noqa: BLE001
        _log.warning("文件名补丁：import DocumentAttributeFilename 失败（%s），跳过", exc)
        return

    orig = DocumentAttributeFilename.__init__
    if getattr(orig, "_tgfs_part_patched", False):
        return

    def __init__(self, *args, **kwargs):
        # pyrogram 的 raw 类型签名是 __init__(self, *, file_name)，关键字必传；
        # 这里仍兼容一下位置传参，免得 pyrogram 换版本后静默失效。
        if isinstance(kwargs.get("file_name"), str):
            kwargs["file_name"] = _PART_PREFIX.sub("", kwargs["file_name"])
        elif args and isinstance(args[0], str):
            args = (_PART_PREFIX.sub("", args[0]),) + args[1:]
        return orig(self, *args, **kwargs)

    __init__._tgfs_part_patched = True
    DocumentAttributeFilename.__init__ = __init__
    _log.info("文件名补丁已生效：上传到频道的文件不再带 [partN] 前缀")


_install_filename_patch()
