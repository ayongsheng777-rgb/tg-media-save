"""把 TGFS 已有的文件描述符从存储频道迁移到独立索引频道。

背景见 tgfs_patch/fd_tg_msg.py。这里只做数据搬迁，步骤是：

  1. 用面板的用户账号建一个私有频道当索引库，把 bot 拉进去当管理员
  2. 用 bot 读存储频道里**置顶的 metadata.json**（整个文件树都在里面）
  3. 树里每个文件存的是 {"name": ..., "messageId": <描述符消息id>}
     逐条把描述符文本从存储频道读出来、发到索引频道、记下新 id
  4. 用新 id 重写 metadata.json，作为**新的一条消息**发回存储频道并置顶，
     旧的置顶取消但**保留不删**（出问题还能翻出来对照）
  5. 把原始 metadata.json 和 id 映射备份到本地

刻意不删任何东西：描述符消息、旧 metadata 都留着，验证通过后再单独清理。

bot 用的是 TGFS 自己那份已授权的 session_string（`tgfs_data/bot.session/<bot_id>.session`），
**不要**改成 bot_token 重新登录 —— 反复 auth.ImportBotAuthorization 会被 Telegram
罚 420 FLOOD_WAIT，实测一次 3079 秒。

用法（在 tg-pro-downloader 容器里跑，那里有 pyrogram 和会话文件）。
tgfs_cfg.py 必须和本文件放在同一个目录：

    docker cp migrate_fd.py tgfs_cfg.py tg-pro-downloader:/tmp/
    docker exec tg-pro-downloader python3 -u /tmp/migrate_fd.py

断点续跑 / 收尾：

    # 描述符搬完但卡在最后一步时，只补发 metadata.json
    docker exec -e ONLY_META=1 tg-pro-downloader python3 -u /tmp/migrate_fd.py

    # WebDAV 验证过文件都能列出来之后，再删存储频道里的旧描述符
    docker exec -e PURGE_OLD_FD=1 tg-pro-downloader python3 -u /tmp/migrate_fd.py
"""
import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path

from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.types import ChatPrivileges

import tgfs_cfg

USER_SESSION = Path("/app/sessions/media_downloader.session")
INDEX_TITLE = "TGFS 索引（勿删）"
OUT_DIR = tgfs_cfg.MIGRATION_DIR   # 与宿主机 tgfs_data 同一个卷

# 连接参数一律从 tgfs_data/config.yaml 读，不硬编码 —— 见 tgfs_cfg.py 文件头。
_CFG = tgfs_cfg.load()
API_ID = _CFG["api_id"]
API_HASH = _CFG["api_hash"]
PROXY = _CFG["proxy"]
STORAGE = _CFG["storage"]

# TGFS 自己的 bot 登录态。注意它**不是** Pyrogram 的 SQLite session 文件，
# 而是纯文本的 session_string（TGFS 的 telegram/impl/pyrogram.py:Session.get()
# 就是 open().read()）。直接拿它当 session_string 用，connect() 会走
# 「已授权」分支，完全不碰 auth.ImportBotAuthorization —— 这一点很关键：
# 反复用 bot_token 重新登录会吃 420 FLOOD_WAIT，实测一次就是 3079 秒。
BOT_SESSION_FILE = tgfs_cfg.bot_session_file()

# bot 在同一个频道里发消息被限到 **约 20 条/分钟**（不是 30 条/秒那种全局限制），
# 实测连发到第 21 条就吃 420 FLOOD_WAIT_X 40 秒。所以：
#   - 间隔拉到 3.2s，正常情况刚好卡在配额下面
#   - sleep_threshold 抬到 600，让 Pyrogram 遇到 FloodWait 自己睡够再重试
#     （默认只有 10 秒，超过就抛异常 —— 上一版就是死在这里）
SEND_DELAY = 3.2
SLEEP_THRESHOLD = 600

# 索引频道还没被 TGFS 正式启用（容器环境变量尚未指向它），所以每次跑之前
# 把里面的消息清空，保证脚本可重复执行、不会搬出重复的描述符。
CLEAN_INDEX = os.environ.get("CLEAN_INDEX", "1") != "0"

# 迁移是「先复制、不删原件」：存储频道里那批旧描述符要另外显式清掉。
# 顺序必须是 迁移 → 起 tgfs 验证 WebDAV 能列出全部文件 → 再 PURGE_OLD_FD=1。
# 反了就没法回头：描述符一删，metadata 又没指向新副本，文件全部读不出来。
PURGE_OLD_FD = os.environ.get("PURGE_OLD_FD") == "1"


def walk_files(node):
    """深度优先遍历 metadata 树，产出所有 file ref（带 messageId 的那些）。"""
    for f in node.get("files") or []:
        if f.get("name") and f.get("messageId"):
            yield f
    for c in node.get("children") or []:
        yield from walk_files(c)


def load_bot_session() -> str:
    raw = BOT_SESSION_FILE.read_text(encoding="utf-8").strip()
    if not raw:
        raise SystemExit(f"{BOT_SESSION_FILE} 是空的")
    return raw


def bot_client(session_string: str) -> Client:
    """复用 TGFS 已授权的 session_string，不触发任何登录请求。"""
    return Client(
        "migbot", api_id=API_ID, api_hash=API_HASH,
        session_string=session_string, proxy=PROXY, no_updates=True,
        sleep_threshold=SLEEP_THRESHOLD,
    )


async def publish_metadata(bot: Client) -> int:
    """把 metadata.new.json 发到存储频道并置顶，取消旧置顶（旧消息保留不删）。

    拆成独立函数是为了支持 ONLY_META=1 断点续跑：描述符搬完后如果卡在
    这一步（比如 send_document 报错），不用再花 5 分钟重发 99 条描述符。
    """
    out = OUT_DIR / "metadata.new.json"
    if not out.exists():
        raise SystemExit(f"{out} 不存在，先完整跑一遍迁移")

    old_pinned_id = None
    try:
        chat = await bot.get_chat(STORAGE)
        if chat.pinned_message:
            old_pinned_id = chat.pinned_message.id
    except Exception as e:
        print(f"[4] ⚠ 读取当前置顶失败: {e}")

    sent = await bot.send_document(STORAGE, document=str(out), file_name="metadata.json")
    await bot.pin_chat_message(STORAGE, sent.id, disable_notification=True)
    print(f"[4] 新 metadata.json 已发送并置顶: message_id={sent.id}")

    # 频道里置顶本来就是「顶掉上一条」，这里再显式取消一次只是兜底
    if old_pinned_id and old_pinned_id != sent.id:
        try:
            await bot.unpin_chat_message(STORAGE, old_pinned_id)
            print(f"[4] 旧置顶已取消（消息 {old_pinned_id} 保留未删）")
        except Exception as e:
            print(f"[4] ⚠ 取消旧置顶失败: {e}")
    return sent.id


async def _purge_old_descriptors(tmp: str):
    """删掉存储频道里那批**旧**描述符消息（新副本已在索引频道）。

    只在 WebDAV 验证过「全部文件都能列出来」之后才允许跑 —— 这些消息一删，
    旧 metadata 就彻底废了，没法回头。
    """
    data = json.loads((OUT_DIR / "fd_id_mapping.json").read_text(encoding="utf-8"))
    old_ids = [int(k) for k in data["mapping"]]
    print(f"[purge] 待删除的旧描述符 {len(old_ids)} 条（存储频道 {STORAGE}）")

    shutil.copy2(USER_SESSION, Path(tmp) / "purge.session")
    app = Client("purge", api_id=API_ID, api_hash=API_HASH,
                 workdir=tmp, proxy=PROXY, no_updates=True,
                 sleep_threshold=SLEEP_THRESHOLD)
    await app.start()
    try:
        deleted = 0
        for i in range(0, len(old_ids), 100):
            chunk = old_ids[i:i + 100]
            await app.delete_messages(STORAGE, chunk)
            deleted += len(chunk)
            print(f"    已删除 {deleted}/{len(old_ids)}")
    finally:
        await app.stop()
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"[purge] 完成，共删除 {deleted} 条旧描述符")


async def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="mig_")
    bot_session = load_bot_session()

    if PURGE_OLD_FD:
        await _purge_old_descriptors(tmp)
        return

    # ONLY_META=1 = 描述符已经搬完，只补「发新 metadata.json 并置顶」这一步。
    # ⚠️ 这个模式下**必须**跳过下面整个第 1 步：第 1.5 步会把索引频道清空，
    # 而索引频道里正是刚搬好的那批描述符 —— 一清就白搬了。
    if os.environ.get("ONLY_META") == "1":
        index_chat_id = json.loads(
            (OUT_DIR / "fd_id_mapping.json").read_text(encoding="utf-8")
        ).get("index")
        print(f"[only-meta] 跳过第 1 步（含清空索引频道），索引频道 id={index_chat_id}")
        await _run_bot_phase(bot_session, index_chat_id, only_meta=True)
        return

    # ---------- 1. 用户账号：建索引频道 + 拉 bot 当管理员 ----------
    shutil.copy2(USER_SESSION, Path(tmp) / "mig.session")
    user = Client("mig", api_id=API_ID, api_hash=API_HASH,
                  workdir=tmp, proxy=PROXY, no_updates=True,
                  sleep_threshold=SLEEP_THRESHOLD)
    await user.connect()
    index_chat_id = None
    try:
        me = await user.get_me()
        print(f"[1] 用户账号: {me.first_name} (@{me.username})")

        # bot 的 username 用于提权：直接传 user id 会 PeerIdInvalid ——
        # 用户账号从没和这个 bot 私聊过，peer 还没进本地缓存。
        bot_probe = bot_client(bot_session)
        await bot_probe.start()
        bot_me = await bot_probe.get_me()
        await bot_probe.stop()
        bot_ref = f"@{bot_me.username}" if bot_me.username else bot_me.id
        print(f"    bot: {bot_ref} (id={bot_me.id})")

        # 脚本可能中途失败过，先把上次建好的频道捞回来复用，别重复建
        index_chat_id = None
        async for d in user.get_dialogs(limit=200):
            if d.chat.title == INDEX_TITLE and d.chat.type == ChatType.CHANNEL:
                index_chat_id = d.chat.id
                print(f"[1] 复用已有索引频道: id={index_chat_id}")
                break
        if index_chat_id is None:
            ch = await user.create_channel(
                INDEX_TITLE,
                "TGFS 的文件描述符存储。删掉这个频道会导致网盘里的文件全部读不出来。",
            )
            index_chat_id = ch.id
            print(f"[1] 已创建索引频道: {ch.title} id={index_chat_id}")

        priv = ChatPrivileges(
            can_manage_chat=True, can_change_info=True, can_post_messages=True,
            can_edit_messages=True, can_delete_messages=True, can_invite_users=True,
        )
        await user.promote_chat_member(index_chat_id, bot_ref, privileges=priv)
        print("[1] bot 已设为索引频道管理员")

        # ---------- 1.5 清空索引频道，让脚本可重复执行 ----------
        # ⚠️ 这一步必须用**用户账号**做：bots 被硬性禁止调用 messages.GetHistory
        # （400 BOT_METHOD_INVALID），连自己当管理员的频道也读不了历史。
        if CLEAN_INDEX:
            stale = [m.id async for m in user.get_chat_history(index_chat_id, limit=500)]
            if stale:
                for i in range(0, len(stale), 100):
                    await user.delete_messages(index_chat_id, stale[i:i + 100])
                print(f"[1.5] 已清空索引频道里 {len(stale)} 条旧消息")
            else:
                print("[1.5] 索引频道是空的，无需清理")
    finally:
        await user.disconnect()
        shutil.rmtree(tmp, ignore_errors=True)

    await _run_bot_phase(bot_session, index_chat_id, only_meta=False)


async def _run_bot_phase(bot_session: str, index_chat_id: int, only_meta: bool):
    # ---------- 2. bot：读置顶 metadata.json ----------
    bot = bot_client(bot_session)
    # 必须用 start() 而不是 connect()：connect() 只建连、**不会**填 self.me，
    # 而 send_document 里要读 self.me.is_premium（判断 2GB / 4GB 上限），
    # self.me 为 None 时直接 AttributeError。session_string 已授权，
    # start() 内部的 connect() 会返回 True，因此不会触发 authorize() 重新登录。
    await bot.start()
    try:
        if only_meta:
            await publish_metadata(bot)
            print("\n完成。索引频道 id =", index_chat_id)
            print("下一步：给 tgfs 容器加环境变量 TGFS_FD_CHANNEL=", index_chat_id)
            return

        chat = await bot.get_chat(STORAGE)
        pinned = chat.pinned_message
        if not pinned or not pinned.document:
            raise SystemExit("存储频道没有置顶的 metadata.json，终止")
        print(f"[2] 置顶 metadata.json: message_id={pinned.id} size={pinned.document.file_size}")

        raw = await bot.download_media(pinned, in_memory=True)
        # Pyrogram 2.x 的 in_memory 返回 BytesIO，不是 bytes
        if hasattr(raw, "getvalue"):
            raw = raw.getvalue()
        (OUT_DIR / "metadata.backup.json").write_bytes(raw)
        meta = json.loads(raw)
        print(f"[2] 已备份原始 metadata.json（{len(raw)} 字节）")

        # ---------- 3. 逐条搬描述符 ----------
        refs = list(walk_files(meta.get("dir") or meta))
        print(f"[3] 待迁移文件引用 {len(refs)} 条")

        old_ids = [int(f["messageId"]) for f in refs]
        texts = {}
        BATCH = 100
        for i in range(0, len(old_ids), BATCH):
            chunk = old_ids[i:i + BATCH]
            msgs = await bot.get_messages(STORAGE, chunk)
            for m in msgs:
                if m and not m.empty and m.text:
                    texts[m.id] = m.text
            print(f"    读取描述符 {min(i + BATCH, len(old_ids))}/{len(old_ids)}")

        mapping = {}
        failed = []
        done = 0
        for n, f in enumerate(refs, 1):
            old_id = int(f["messageId"])
            # 同一个描述符可能被多个路径引用（同一文件放进两个目录），只搬一次
            if old_id in mapping:
                f["messageId"] = mapping[old_id]
                continue
            text = texts.get(old_id)
            if not text:
                failed.append({"name": f.get("name"), "old": old_id})
                continue
            # ⚠️ Pyrogram 的 Client 没有 send_text —— send_text 是 TGFS 内部
            # MessageApi 的方法名，这里要用 send_message，并且拿 .id。
            sent_fd = await bot.send_message(
                index_chat_id, text, disable_web_page_preview=True
            )
            new_id = sent_fd.id
            mapping[old_id] = new_id
            f["messageId"] = new_id
            done += 1
            if done % 10 == 0:
                print(f"    已搬迁 {done} 条（扫描到 {n}/{len(refs)}）")
                # 每 10 条落一次盘：万一半路被 flood wait 打断，
                # 至少知道已经搬到哪了，不用从零开始猜
                (OUT_DIR / "fd_id_mapping.partial.json").write_text(
                    json.dumps(mapping, ensure_ascii=False, indent=2),
                    encoding="utf-8")
            await asyncio.sleep(SEND_DELAY)

        (OUT_DIR / "fd_id_mapping.json").write_text(
            json.dumps({"storage": STORAGE, "index": index_chat_id,
                        "mapping": mapping, "failed": failed},
                       ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[3] 搬迁完成 {len(mapping)} 条，失败 {len(failed)} 条")
        if failed:
            print("    失败的（这些文件会读不出来，需要单独处理）:")
            for x in failed[:10]:
                print("     ", x)
            raise SystemExit("有描述符搬迁失败，已终止，未改写 metadata.json")

        # ---------- 4. 发新 metadata.json 并置顶 ----------
        out = OUT_DIR / "metadata.new.json"
        out.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
        await publish_metadata(bot)

        print("\n完成。索引频道 id =", index_chat_id)
        print("下一步：给 tgfs 容器加环境变量 TGFS_FD_CHANNEL=", index_chat_id)
    finally:
        await bot.stop()


asyncio.run(main())
