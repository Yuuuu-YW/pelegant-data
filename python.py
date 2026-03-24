#!/usr/bin/env python3
"""
python.py — 查找已接受协作邀请的 GitHub 仓库

使用方法：
    python python.py [--token <GITHUB_TOKEN>]

如果未传 --token，脚本会从环境变量 GITHUB_TOKEN 读取。

输出：
    1. 你已接受邀请的协作仓库列表（affiliation=collaborator）
    2. 还未处理的待接受邀请列表
"""

from __future__ import annotations

import argparse
import os
import sys

import requests


GITHUB_API = "https://api.github.com"


def get_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def paginate(url: str, headers: dict[str, str]) -> list[dict]:
    """Fetch all pages from a GitHub REST endpoint."""
    results: list[dict] = []
    while url:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code in (401, 403):
            sys.exit(
                f"错误：GitHub API 返回 {resp.status_code}，"
                "Token 无效、已过期或权限不足，请检查后重试。"
            )
        resp.raise_for_status()
        results.extend(resp.json())
        # Follow Link header for next page
        url = ""
        link_header = resp.headers.get("Link", "")
        for part in link_header.split(","):
            part = part.strip()
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                break
    return results


def list_collaborated_repos(token: str) -> list[dict]:
    """返回用户以协作者身份加入的仓库列表（已接受邀请）。"""
    url = f"{GITHUB_API}/user/repos?affiliation=collaborator&per_page=100"
    return paginate(url, get_headers(token))


def list_pending_invitations(token: str) -> list[dict]:
    """返回用户尚未处理的仓库协作邀请列表。"""
    url = f"{GITHUB_API}/user/repository_invitations?per_page=100"
    return paginate(url, get_headers(token))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="查找已接受或待接受的 GitHub 仓库协作邀请"
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GITHUB_TOKEN", ""),
        help="GitHub Personal Access Token（也可通过环境变量 GITHUB_TOKEN 传入）",
    )
    args = parser.parse_args()

    if not args.token:
        sys.exit(
            "错误：未提供 GitHub Token。\n"
            "请通过 --token <TOKEN> 参数或 GITHUB_TOKEN 环境变量传入。"
        )

    # ── 1. 已接受邀请的协作仓库 ──────────────────────────────────────────
    print("正在获取已接受协作邀请的仓库…\n")
    collaborated = list_collaborated_repos(args.token)

    if collaborated:
        print(f"找到 {len(collaborated)} 个协作仓库：")
        for repo in collaborated:
            visibility = "私有" if repo.get("private") else "公开"
            print(f"  • {repo['full_name']}  [{visibility}]  {repo['html_url']}")
    else:
        print("未找到已接受协作邀请的仓库。")

    print()

    # ── 2. 待处理的邀请 ──────────────────────────────────────────────────
    print("正在获取待处理的仓库协作邀请…\n")
    pending = list_pending_invitations(args.token)

    if pending:
        print(f"找到 {len(pending)} 条待接受的邀请：")
        for inv in pending:
            repo_info = inv.get("repository", {})
            inviter = inv.get("inviter", {}).get("login", "未知")
            print(
                f"  • {repo_info.get('full_name', '未知仓库')}  "
                f"（邀请人：{inviter}）  "
                f"邀请链接：{inv.get('html_url', '')}"
            )
    else:
        print("没有待处理的邀请。")


if __name__ == "__main__":
    main()

