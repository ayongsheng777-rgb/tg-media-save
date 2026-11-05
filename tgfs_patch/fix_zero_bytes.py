"""修复 TGFS 里 0 字节的「僵尸条目」。

成因：TGFS 上传是「先建 metadata 条目（size 0）→ 把内容发到 Telegram →
回填 size」。中间被打断（容器重启 / 上传失败）就永久停在 0 字节：
文件在 WebDAV 里看得见，下载却是空的。

修法：拿本地 downloads/ 里的同名文件直接 PUT 覆盖。TGFS 会新建一个版本，
`_validate_fv` 会挑出有效那个 —— **不需要先删条目**（先删风险大得多，
万一本地也没有源文件，文件就真的没了）。

用法（NAS 宿主上直接跑，走本机回环，不需要代理）：
    python3 fix_zero_bytes.py --dry-run
    python3 fix_zero_bytes.py

凭据从 tgfs_data/config.yaml 读（`tgfs.users` 下第一个账号），
也可以用环境变量 TGFS_WEBDAV_USER / TGFS_WEBDAV_PASS 覆盖。
"""
import base64
import http.client
import os
import re
import sys
import urllib.parse

HOST, PORT = "127.0.0.1", 1900
CONFIG = "/vol1/1000/tg-pro-panel/tgfs_data/config.yaml"
DOWNLOADS = "/vol1/1000/tg-pro-panel/downloads"
# 相对路径，默认就是当前这套目录结构
DEFAULT_SUB = "精选厕拍【加入会员不走丢！】/2026_10"
CHUNK = 1024 * 1024


def credentials():
    user = os.environ.get("TGFS_WEBDAV_USER")
    password = os.environ.get("TGFS_WEBDAV_PASS")
    if user and password:
        return user, password

    text = open(CONFIG, encoding="utf-8").read()
    try:
        import yaml
        users = (yaml.safe_load(text).get("tgfs") or {}).get("users") or {}
        user = next(iter(users))
        return user, users[user]["password"]
    except ImportError:
        # 宿主上没装 pyyaml 时退化成最小解析：tgfs.users 下第一组 key/password
        m = re.search(r"^\s+(\S+):\s*\n\s+password:\s*(\S+)\s*$", text, re.M)
        if not m:
            raise SystemExit(f"没能从 {CONFIG} 里解析出 WebDAV 账号密码")
        return m.group(1), m.group(2)


def auth_header():
    user, password = credentials()
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def request(method, path, auth, headers=None, body_file=None):
    conn = http.client.HTTPConnection(HOST, PORT, timeout=600)
    conn.putrequest(method, urllib.parse.quote(path))
    conn.putheader("Authorization", auth)
    for k, v in (headers or {}).items():
        conn.putheader(k, v)
    conn.endheaders()
    if body_file is not None:
        while True:
            buf = body_file.read(CHUNK)
            if not buf:
                break
            conn.send(buf)
    r = conn.getresponse()
    body = r.read().decode("utf-8", "replace")
    conn.close()
    return r.status, body


def zero_entries(remote_dir, auth):
    _, xml = request("PROPFIND", remote_dir, auth, {"Depth": "1"})
    out = []
    for block in re.findall(r"<D:response>(.*?)</D:response>", xml, re.S):
        n = re.search(r"<D:displayname>(.*?)</D:displayname>", block)
        s = re.search(r"<D:getcontentlength>(\d+)</D:getcontentlength>", block)
        if n and s and s.group(1) == "0":
            out.append(n.group(1))
    return out


def main():
    dry = "--dry-run" in sys.argv
    sub = DEFAULT_SUB
    if "--dir" in sys.argv:
        sub = sys.argv[sys.argv.index("--dir") + 1]

    auth = auth_header()
    remote_dir = f"/webdav/tgfs/{sub}"
    local_dir = os.path.join(DOWNLOADS, sub)

    zeros = zero_entries(remote_dir, auth)
    print(f"0 字节条目 {len(zeros)} 个", flush=True)

    todo, skip = [], []
    for name in zeros:
        local = os.path.join(local_dir, name)
        (todo if os.path.exists(local) and os.path.getsize(local) > 0 else skip).append(name)

    print(f"有本地源文件 {len(todo)} 个，缺源文件 {len(skip)} 个", flush=True)
    for name in skip:
        print(f"  跳过（无源）: {name[:70]}", flush=True)
    if dry:
        for name in todo:
            print(f"  待修: {name[:70]}", flush=True)
        return

    ok = fail = 0
    for i, name in enumerate(todo, 1):
        local = os.path.join(local_dir, name)
        size = os.path.getsize(local)
        try:
            with open(local, "rb") as f:
                status, _ = request(
                    "PUT", f"{remote_dir}/{name}", auth,
                    {"Content-Length": str(size),
                     "Content-Type": "application/octet-stream"},
                    body_file=f,
                )
            if status in (200, 201, 204):
                ok += 1
                print(f"  [{i}/{len(todo)}] OK {status} {size} 字节  {name[:60]}", flush=True)
            else:
                fail += 1
                print(f"  [{i}/{len(todo)}] 失败 {status}  {name[:60]}", flush=True)
        except Exception as e:
            fail += 1
            print(f"  [{i}/{len(todo)}] 异常 {e}  {name[:60]}", flush=True)
    print(f"完成：成功 {ok}，失败 {fail}", flush=True)


if __name__ == "__main__":
    main()
