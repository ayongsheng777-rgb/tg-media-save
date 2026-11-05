"""把 TGFS 的「文件描述符」从存储频道挪到独立的索引频道。

背景
----
TGFS 的设计是「频道即数据库」：每个文件在存储频道里对应**两条**消息

    1. {"type":"F","versions":[{"type":"FV","id":…,"messageIds":[…],"size":…}]}
       —— 文件描述符，core/repository/impl/fd/tg_msg.py 用 send_text 发出来，
          之后用 edit_message_text 就地更新（版本、分片 id 都记在这里）
    2. 文件本身（document）

描述符是纯 JSON，对用户完全是噪音，但又**删不得** —— `get()` 里就是
`json.loads(message.text)`，删了 TGFS 就不知道文件内容消息是哪几条。

所以这里只做一件事：给描述符单独一个私有频道。存储频道里只剩文件 +
置顶的 metadata.json，看起来就干净了。

配置
----
环境变量 `TGFS_FD_CHANNEL`（频道 id，`-100` 开头的完整形式）。

不配、或配成和存储频道同一个，行为与官方镜像完全一致（都写存储频道），
所以这个补丁在没有配置时是零影响。

注意
----
- 迁移已有描述符要配套做（metadata.json 里 files[].messageId 指向旧频道），
  否则老文件会全部读成空 —— 见 tgfs_patch/migrate_fd.py。
- 内容消息（version.message_ids）**始终在存储频道**，_validate_fv 必须继续用
  原始 message_api，不能用索引频道的实例，否则文件大小校验全部失败。
"""

import json
import logging
import os
from itertools import chain
from typing import Optional

from tgfs.core.api import MessageApi
from tgfs.core.model import TGFSFileDesc, TGFSFileRef
from tgfs.core.repository.interface import (
    FDRepositoryResp,
    IFDRepository,
)
from tgfs.errors import MessageNotFound

logger = logging.getLogger(__name__)


def _index_channel_of(message_api: MessageApi) -> Optional[int]:
    """读 TGFS_FD_CHANNEL；没配、非法、或与存储频道相同都返回 None。"""
    raw = (os.environ.get("TGFS_FD_CHANNEL") or "").strip()
    if not raw:
        return None
    try:
        channel = int(raw)
    except ValueError:
        logger.error("TGFS_FD_CHANNEL=%r 不是整数，忽略，描述符仍写存储频道", raw)
        return None
    if channel == message_api.private_file_channel:
        return None
    return channel


class TGMsgFDRepository(IFDRepository):
    def __init__(self, message_api: MessageApi):
        self._message_api = message_api
        index_channel = _index_channel_of(message_api)
        if index_channel is None:
            self._fd_api = message_api
        else:
            # 必须新建一个 MessageApi 实例，不能改私有字段：
            # MessageBroker 的消息缓存是按 self.private_file_channel 分桶的，
            # 共用一个实例会把两个频道的消息塞进同一份缓存。
            self._fd_api = MessageApi(message_api.tdlib, index_channel)
            logger.info(
                "文件描述符改写到索引频道 %s（存储频道 %s）",
                index_channel,
                message_api.private_file_channel,
            )

    async def save(
        self, fd: TGFSFileDesc, fr: Optional[TGFSFileRef] = None
    ) -> FDRepositoryResp:
        # fr 为空 = 新文件，发一条新的描述符消息
        if fr is None:
            return FDRepositoryResp(
                message_id=await self._fd_api.send_text(fd.to_json()),
                fd=fd,
            )

        # 已有文件：就地 edit。消息被人手动删掉时会抛 MessageNotFound，
        # 这时退回新建一条（上游原有行为，保留）。
        try:
            return FDRepositoryResp(
                message_id=await self._fd_api.edit_message_text(
                    message_id=fr.message_id, message=fd.to_json()
                ),
                fd=fd,
            )
        except MessageNotFound:
            return await self.save(fd)

    async def _validate_fv(
        self, fd: TGFSFileDesc, include_all_versions: bool
    ) -> TGFSFileDesc:
        versions = fd.get_versions(exclude_invalid=True)

        # Files in the channel may be deleted manually, so we need to check if the messages for the versions exist.

        # 内容消息在存储频道，必须用原始 message_api，不能用 _fd_api
        file_messages = await self._message_api.get_messages(
            list(chain(*(version.message_ids for version in versions)))
        )

        message_map = {msg.message_id: msg for msg in file_messages if msg}

        has_valid_version = False

        for i, version in enumerate(versions):
            for j, message_id in enumerate(version.message_ids):
                if (
                    not (file_message := message_map.get(message_id, None))
                    or not file_message.document
                ):
                    logger.warning(
                        f"File message {message_id} for part {j + 1} of {fd.name}@{version.id} not found"
                    )
                    version.set_invalid()
                    break
                version.part_sizes.append(file_message.document.size)
            if version.is_valid():
                has_valid_version = True
                if not include_all_versions:
                    # Found a valid version, no need to check further
                    return fd

        return fd if has_valid_version else TGFSFileDesc.empty(fd.name)

    async def get(
        self, fr: TGFSFileRef, include_all_versions: bool = False
    ) -> TGFSFileDesc:
        # 描述符在索引频道
        message = (await self._fd_api.get_messages([fr.message_id]))[0]

        if not message:
            logging.error(
                f"File descriptor (message_id: {fr.message_id}) for {fr.name} not found"
            )
            return TGFSFileDesc.empty(fr.name)

        fd = TGFSFileDesc.from_dict(json.loads(message.text), name=fr.name)
        return await self._validate_fv(fd, include_all_versions)
