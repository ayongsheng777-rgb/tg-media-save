FROM tangyoha/telegram_media_downloader:latest

WORKDIR /app

# 安装 Web 管理、OTP 鉴权、JWT、ruamel.yaml 及支持 Socks5 代理的 httpx 依赖
# 境内网络 pip 直连较慢时，可用本地 .env 里的 BUILD_HTTP_PROXY / BUILD_HTTPS_PROXY 注入构建代理。
# 代理账号密码只允许放在本地 .env（已 gitignore），不要写进镜像或提交到仓库。
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
RUN pip install --no-cache-dir --timeout 60 \
    fastapi \
    uvicorn \
    pyotp \
    pyjwt \
    ruamel.yaml \
    python-multipart \
    aiofiles \
    "httpx[socks]"

# 覆盖引擎的两个文件（见 patch/ 下各文件头说明）：
#   get_chat_history_v2.py  去掉走到历史末尾时把整段历史再解析一遍的兜底分支，
#                          并每扫描 500 条打印一行进度（否则枚举期间界面完全无反馈）
#   media_downloader.py    无媒体的消息不再进下载队列（否则 worker 先空转上万次）
# 升级基础镜像时需比对上游 md5：
#   get_chat_history_v2.py  9a5a71be726fa2f17084b4ca475110af（98 行）
#   media_downloader.py     4423473362625e569bae39bcc5de70ed（704 行）
COPY patch/get_chat_history_v2.py /app/module/get_chat_history_v2.py
COPY patch/media_downloader.py /app/media_downloader.py

COPY main.py /app/panel_main.py
COPY templates /app/templates

EXPOSE 8080 5000

CMD ["uvicorn", "panel_main:app", "--host", "0.0.0.0", "--port", "8080"]
