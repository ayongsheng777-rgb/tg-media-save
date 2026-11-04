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

COPY main.py /app/panel_main.py
COPY templates /app/templates

EXPOSE 8080 5000

CMD ["uvicorn", "panel_main:app", "--host", "0.0.0.0", "--port", "8080"]
