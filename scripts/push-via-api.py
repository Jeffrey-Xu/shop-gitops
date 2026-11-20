#!/usr/bin/env python3
"""兜底推送：github.com:443 的 git push 不通时，用 Git Data API 把本地 HEAD 落上去。

效果等价于 `git push --force-with-lease`，但走的是 api.github.com，因此只要求
API 可达。正常情况下不要用它——先跑 scripts/git-doctor.sh 确认确实是第 2 步失败。

保护：远端内容和本地历史对不上时拒绝覆盖并退出 2。判断依据是**树哈希**——
远端 HEAD 那棵树必须在本地历史里出现过，否则说明远端有本地没见过的改动
（最常见的来源：CI 的 bump 提交、别人推的提交）。只看「远端多出来的文件」
是不够的：远端改了同一个文件时，文件集合一样，照样会被整树覆盖掉。
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


def local_commit_with_tree(tree_sha: str, depth: int = 300) -> str | None:
    """找出本地历史里树哈希等于 tree_sha 的提交。

    命中说明「远端当前内容」是本地见过的一个状态，可以安全地在其之上落提交；
    不命中说明远端有本地从未有过的改动，此时整树覆盖会把它抹掉，必须拒绝。
    """
    out = git("log", f"-n{depth}", "--format=%H %T")
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == tree_sha:
            return parts[0]
    return None


def main() -> int:
    local_head = git("rev-parse", "HEAD").strip()
    message = git("log", "-1", "--pretty=%B").strip()

    head = req("GET", f"/repos/{REPO}/commits/{BRANCH}")
    base = head["sha"]
    if base == local_head:
        print("远端已经是这个提交，无需推送")
        return 0

    base_tree = head["commit"]["tree"]["sha"]
    anchor = local_commit_with_tree(base_tree)
    if anchor is None:
        print("拒绝覆盖：远端 HEAD 的树在本地历史里找不到。")
        print(f"  远端 {base[:12]}  tree {base_tree[:12]}")
        print("  说明远端有本地没见过的改动（CI bump 或他人推送）。先对齐再推：")
        print(f"    git fetch origin {BRANCH}")
        print(f"    git log --oneline HEAD..origin/{BRANCH}   # 看看差了什么")
        print(f"    git reset --hard origin/{BRANCH}          # 或 rebase 到上面")
        return 2

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

    if anchor == local_head:
        print("远端内容与本地一致（仅提交哈希不同），无需推送")
        return 0
    print(f"远端内容对应本地提交 {anchor[:12]}，在其之上落提交")

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
