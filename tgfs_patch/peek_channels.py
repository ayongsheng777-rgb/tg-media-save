"""看一眼存储频道 / 索引频道最近几条消息，用来验证描述符确实搬走了。

用面板的用户账号（会话副本，不和正在跑的引擎抢 SQLite 锁）读两个频道：
  - 存储频道：应该只有「文件 + 置顶 metadata.json」，**不该再出现 {"type": "F"…}
  - 索引频道：应该是一串 {"type": "F"…} 描述符

用法：
    docker exec tg-pro-downloader python3 -u /tmp/peek_channels.py [条数]
    docker exec tg-pro-downloader python3 -u /tmp/peek_channels.py --del <chat_id> <msg_id> [...]
"""
import asyncio
import shutil
import sys
import tempfile
from pathlib import Path

from pyrogram import Client

import tgfs_cfg

USER_SESSION = Path("/app/sessions/media_downloader.session")

_CFG = tgfs_cfg.load()
API_ID = _CFG["api_id"]
API_HASH = _CFG["api_hash"]
PROXY = _CFG["proxy"]
STORAGE = _CFG["storage"]
INDEX = _CFG["index"]


def kind(m):
    if m.document:
        return f"DOC  {m.document.file_name}"
    if m.photo:
        return "PHOTO"
    if m.text:
        t = m.text.replace("\n", " ")
        if t.startswith('{"type"'):
            return f"JSON {t[:70]}"
        return f"TEXT {t[:70]}"
    return "OTHER"


async def dump(app, chat_id, label, limit):
    print(f"\n===== {label} ({chat_id}) 最近 {limit} 条 =====")
    n = 0
    async for m in app.get_chat_history(chat_id, limit=limit):
        n += 1
        print(f"  {m.id:>6}  {kind(m)}")
    if n == 0:
        print("  （空）")


async def main():
    tmp = tempfile.mkdtemp(prefix="peek_")
    try:
        shutil.copy2(USER_SESSION, Path(tmp) / "peek.session")
        app = Client("peek", api_id=API_ID, api_hash=API_HASH,
                     workdir=tmp, proxy=PROXY, no_updates=True)
        await app.start()
        try:
            if sys.argv[1:2] == ["--del"]:
                chat_id = int(sys.argv[2])
                ids = [int(x) for x in sys.argv[3:]]
                await app.delete_messages(chat_id, ids)
                print(f"已删除 {chat_id} 里的 {len(ids)} 条消息: {ids}")
                return
            limit = int(sys.argv[1]) if len(sys.argv) > 1 else 12
            await dump(app, STORAGE, "存储频道", limit)
            if INDEX:
                await dump(app, INDEX, "索引频道", limit)
            else:
                print("\n（未配置 TGFS_FD_CHANNEL，也没有迁移记录，跳过索引频道）")
        finally:
            await app.stop()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


asyncio.run(main())
