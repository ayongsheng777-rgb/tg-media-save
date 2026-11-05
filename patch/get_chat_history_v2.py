"""Rewrite pyrogram.get_chat_history

本文件是基础镜像 tangyoha/telegram_media_downloader:2.2.5 中
/app/module/get_chat_history_v2.py 的补丁副本，由 Dockerfile COPY 覆盖进镜像。
上游版本 md5：9a5a71be726fa2f17084b4ca475110af（98 行）

改动只有两处，其余逐字节保持原样：

1. 删掉「块为空时用 Pyrogram 自带 get_chat_history 再遍历一次」的兜底分支。
   原实现 break_count = offset_id - 1，这个写法假设消息 id 从 1 开始连续递增；
   而实际频道 id 稀疏、且起始 id 可能上万，break_count 几乎不可能归零，
   于是每次走到历史末尾都会把整段历史用 parse_messages 再解析一遍（上万条消息、
   数分钟纯 CPU、零输出）。实测该分支从不产出任何消息，纯属空转。

2. 每扫描 _ENUM_LOG_EVERY 条打印一行进度。
   面板（panel_main.py 的 run_all_groups）是逐行读引擎 stdout 转发到 WebUI 日志的，
   枚举期间不打印任何东西，界面上就等于「卡死」。加上进度后可以看到扫描到第几条。

注意：升级基础镜像时需同步本补丁（比对上游 md5）。
"""

import time
from datetime import datetime
from typing import AsyncGenerator, Optional, Union

import pyrogram

# pylint: disable = W0611
from pyrogram import raw, types, utils

# 每扫描多少条打印一次进度（0 表示关闭）
_ENUM_LOG_EVERY = 500


async def get_chunk_v2(
    *,
    client: pyrogram.Client,
    chat_id: Union[int, str],
    limit: int = 0,
    offset: int = 0,
    max_id: int = 0,
    from_message_id: int = 0,
    from_date: datetime = utils.zero_datetime(),
    reverse: bool = False
):
    """get chunk"""
    from_message_id = from_message_id or (1 if reverse else 0)

    messages = await utils.parse_messages(
        client,
        await client.invoke(
            raw.functions.messages.GetHistory(
                peer=await client.resolve_peer(chat_id),
                offset_id=from_message_id,
                offset_date=utils.datetime_to_timestamp(from_date),
                add_offset=offset * (-1 if reverse else 1) - (limit if reverse else 0),
                limit=limit,
                max_id=max_id,
                min_id=0,
                hash=0,
            ),
            sleep_threshold=60,
        ),
        replies=0,
    )

    if reverse:
        messages.reverse()

    return messages


# pylint: disable = C0301
async def get_chat_history_v2(
    self: pyrogram.Client,
    chat_id: Union[int, str],
    limit: int = 0,
    max_id: int = 0,
    offset: int = 0,
    offset_id: int = 0,
    offset_date: datetime = utils.zero_datetime(),
    reverse: bool = False,
) -> Optional[AsyncGenerator["types.Message", None]]:
    """Get messages from a chat history."""
    current = 0
    total = limit or (1 << 31) - 1
    limit = min(100, total)

    started = time.time()
    chunks = 0
    last_logged = 0

    while True:
        messages = await get_chunk_v2(
            client=self,
            chat_id=chat_id,
            limit=limit,
            offset=offset,
            max_id=max_id + 1 if max_id else 0,
            from_message_id=offset_id,
            from_date=offset_date,
            reverse=reverse,
        )

        chunks += 1

        if not messages:
            # 上游此处会再遍历一遍整段历史（见文件头说明），已移除，直接结束。
            print(
                f"[枚举] chat={chat_id} 已到历史末尾：共 {current} 条 / {chunks} 块 / "
                f"耗时 {time.time() - started:.0f}s",
                flush=True,
            )
            return

        offset_id = messages[-1].id + (1 if reverse else 0)

        for message in messages:
            yield message

            current += 1

            if _ENUM_LOG_EVERY and current - last_logged >= _ENUM_LOG_EVERY:
                last_logged = current
                print(
                    f"[枚举] chat={chat_id} 已扫描 {current} 条 · 当前 msg_id={message.id} · "
                    f"耗时 {time.time() - started:.0f}s",
                    flush=True,
                )

            if current >= total:
                return
