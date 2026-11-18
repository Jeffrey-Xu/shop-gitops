#!/usr/bin/env python3
"""兜底推送：github.com:443 的 git push 不通时，用 Git Data API 把本地 HEAD 落上去。

效果等价于 `git push --force-with-lease`，但走的是 api.github.com，因此只要求
API 可达。正常情况下不要用它——先跑 scripts/git-doctor.sh 确认确实是第 2 步失败。

保护：远端如果出现了本地没有的文件（说明别人推过），脚本拒绝覆盖并退出 2。
"""
import base64
import json
import os
import re
import subprocess
import sys

API = "https://api.github.com"
BRANCH = os.environ.get("BRANCH", "main")


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True)


def resolve_repo() -> str:
    url = git("remote", "get-url", "origin").strip()
    m = re.search(r"github\.com[:/]+([^/]+)/(.+?)(?:\.git)?$", url)
    if not m:
        sys.exit(f"无法从 remote 解析 owner/repo：{url}")
    return f"{m.group(1)}/{m.group(2)}"


REPO = resolve_repo()
TOKEN = subprocess.check_output(["gh", "auth", "token"], text=True).strip()


def req(method: str, path: str, payload=None):
    import urllib.request

    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "shop-gitops-push",
        },
    )
    with urllib.request.urlopen(r, timeout=90) as resp:
        return json.loads(resp.read().decode())


def ls_tree(rev: str) -> dict[str, tuple[str, str]]:
    """path -> (mode, blob sha)，保留真实 mode（脚本带可执行位）。"""
    out = {}
    for line in git("ls-tree", "-r", rev).splitlines():
        meta, path = line.split("\t", 1)
        mode, _typ, sha = meta.split()
        out[path] = (mode, sha)
    return out


def main() -> int:
    local_head = git("rev-parse", "HEAD").strip()
    message = git("log", "-1", "--pretty=%B").strip()

    base = req("GET", f"/repos/{REPO}/commits/{BRANCH}")["sha"]
    if base == local_head:
        print("远端已经是这个提交，无需推送")
        return 0

    base_blobs = {
        e["path"]: e["sha"]
        for e in req("GET", f"/repos/{REPO}/git/trees/{base}?recursive=1")["tree"]
        if e["type"] == "blob"
    }
    local_blobs = ls_tree("HEAD")

    # 远端多出来的文件 = 别人推过东西，不能盲覆盖
    unknown = set(base_blobs) - set(local_blobs)
    if unknown:
        print(f"远端有本地不存在的文件，拒绝覆盖：{sorted(unknown)}")
        return 2

    entries, uploaded = [], 0
    for path, (mode, sha) in local_blobs.items():
        if base_blobs.get(path) != sha:
            with open(path, "rb") as f:
                content = base64.b64encode(f.read()).decode()
            sha = req(
                "POST",
                f"/repos/{REPO}/git/blobs",
                {"content": content, "encoding": "base64"},
            )["sha"]
            uploaded += 1
            print(f"  blob {path}")
        entries.append((mode, path, sha))

    root: dict = {}
    for mode, path, sha in entries:
        parts = path.split("/")
        cur = root
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = (mode, sha)

    def make_tree(node: dict) -> str:
        tree = []
        for name, val in node.items():
            if isinstance(val, dict):
                tree.append(
                    {"path": name, "mode": "040000", "type": "tree", "sha": make_tree(val)}
                )
            else:
                mode, sha = val
                tree.append({"path": name, "mode": mode, "type": "blob", "sha": sha})
        return req("POST", f"/repos/{REPO}/git/trees", {"tree": tree})["sha"]

    commit = req(
        "POST",
        f"/repos/{REPO}/git/commits",
        {"message": message, "tree": make_tree(root), "parents": [base]},
    )
    req("PATCH", f"/repos/{REPO}/git/refs/heads/{BRANCH}",
        {"sha": commit["sha"], "force": False})

    print(f"uploaded blobs: {uploaded}")
    print(f"remote commit : {commit['sha']}")
    print(f"local  commit : {local_head}  (hash 会不同，内容一致)")
    print("提示：推送后 `git fetch && git reset --hard origin/" + BRANCH + "` 对齐本地历史")
    return 0


if __name__ == "__main__":
    sys.exit(main())
