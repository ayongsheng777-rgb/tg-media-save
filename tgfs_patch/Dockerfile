# 在官方 TGFS 镜像上叠一层代理补丁。
#
# 为什么需要：官方 wheatcarrier/tgfs 的 Pyrogram 客户端不接受 proxy 参数，
# 境内直连 Telegram 必然超时，TGFS 起不来、WebDAV 端口也不监听。
#
# 为什么用 sitecustomize 而不是改源码：Python 解释器启动时会自动 import
# site-packages/sitecustomize.py，所以补丁与 TGFS 源码完全解耦 ——
# 升级基础镜像只要 tag 还能拉下来，补丁就继续生效，不必重新比对 md5。
#
# 基础镜像在境内直连 Docker Hub 会超时，默认走 1ms 镜像源；
# 想换回官方源就设 TGFS_IMAGE=wheatcarrier/tgfs:latest。
ARG TGFS_BASE=docker.1ms.run/wheatcarrier/tgfs:latest
FROM ${TGFS_BASE}

# 必须显式 chmod：在群晖/绿联这类 NAS 上，宿主文件带 ACL，`COPY --chmod=644`
# 会被 ACL 覆盖成 0000，容器里非 root 的 tgfs 用户直接 Permission denied，
# 表现为 "Error in sitecustomize" 而补丁静默失效（踩过）。
USER root
COPY sitecustomize.py /usr/local/lib/python3.13/site-packages/sitecustomize.py
RUN chmod 644 /usr/local/lib/python3.13/site-packages/sitecustomize.py \
    && test -r /usr/local/lib/python3.13/site-packages/sitecustomize.py
USER tgfs
