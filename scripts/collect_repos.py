#!/usr/bin/env python3
"""
Collect 50-200 Python repositories using GitHub Search API and save to CSV.

What it saves:
- full_name, html_url, clone_url, stars, forks, open_issues, size, default_branch,
  license, pushed_at, created_at, repo_id
Optionally (recommended): latest commit SHA on default branch for reproducibility.

Usage:
  python collect_repos.py --n 200 --out repos.csv --min-stars 50 --min-size 1000 --with-sha

Notes:
- GitHub Search API returns at most 1000 results per query.
- Use a token to avoid low rate limits.
"""

import argparse
import csv
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests

from dotenv import load_dotenv


GITHUB_API = "https://api.github.com"


def gh_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "code-smell-dataset-collector",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def request_json(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
    r = requests.get(url, headers=headers, params=params, timeout=30)

    # Basic rate-limit handling
    if r.status_code == 403 and "rate limit" in r.text.lower():
        reset = r.headers.get("X-RateLimit-Reset")
        if reset:
            wait_s = max(0, int(reset) - int(time.time()) + 2)
            print(f"[rate-limit] Sleeping {wait_s}s until reset...", file=sys.stderr)
            time.sleep(wait_s)
            r = requests.get(url, headers=headers, params=params, timeout=30)

    r.raise_for_status()
    return r.json()


def search_repos(
    token: Optional[str],
    query: str,
    n: int,
    sort: str = "stars",
    order: str = "desc",
) -> List[Dict[str, Any]]:
    headers = gh_headers(token)
    per_page = 100
    repos: List[Dict[str, Any]] = []
    page = 1

    while len(repos) < n:
        params = {
            "q": query,
            "sort": sort,
            "order": order,
            "per_page": per_page,
            "page": page,
        }
        data = request_json(f"{GITHUB_API}/search/repositories", headers, params=params)
        items = data.get("items", [])
        if not items:
            break

        repos.extend(items)
        print(f"[search] fetched page {page}, total repos={len(repos)}", file=sys.stderr)

        page += 1
        if page > 10:  # Search API: 100 results/page; 10 pages = 1000 cap
            break

    return repos[:n]


def get_latest_commit_sha(token: Optional[str], full_name: str, branch: str) -> Optional[str]:
    headers = gh_headers(token)
    # Commits API: /repos/{owner}/{repo}/commits/{branch}
    url = f"{GITHUB_API}/repos/{full_name}/commits/{branch}"
    try:
        data = request_json(url, headers)
        return data.get("sha")
    except Exception as e:
        print(f"[warn] could not get sha for {full_name}@{branch}: {e}", file=sys.stderr)
        return None


def normalize_repo_row(repo: Dict[str, Any]) -> Dict[str, Any]:
    license_info = repo.get("license") or {}
    return {
        "repo_id": repo.get("id"),
        "full_name": repo.get("full_name"),
        "html_url": repo.get("html_url"),
        "clone_url": repo.get("clone_url"),
        "ssh_url": repo.get("ssh_url"),
        "stars": repo.get("stargazers_count"),
        "forks": repo.get("forks_count"),
        "open_issues": repo.get("open_issues_count"),
        "size_kb": repo.get("size"),
        "default_branch": repo.get("default_branch"),
        "license": license_info.get("spdx_id") or license_info.get("key"),
        "pushed_at": repo.get("pushed_at"),
        "created_at": repo.get("created_at"),
        "archived": repo.get("archived"),
        "disabled": repo.get("disabled"),
        "is_fork": repo.get("fork"),
    }


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("No rows to write.")
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200, help="Number of repos to collect (50-200 recommended).")
    ap.add_argument("--out", type=str, default="repos.csv", help="Output CSV path.")
    ap.add_argument("--min-stars", type=int, default=50)
    ap.add_argument("--min-size", type=int, default=1000, help="GitHub 'size' is KB of repo. Use to avoid tiny repos.")
    ap.add_argument("--pushed-after", type=str, default=None,
                    help="Optional: ISO date like 2024-01-01 to prefer recently updated repos (query uses pushed:>DATE).")
    ap.add_argument("--with-sha", action="store_true", help="Also fetch latest commit SHA on default branch.")
    args = ap.parse_args()

    load_dotenv()

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("[error] GITHUB_TOKEN not found. Create a local .env with GITHUB_TOKEN=... or export it in your shell.", file=sys.stderr)
        print("See .env.example. Example: export GITHUB_TOKEN=ghp_xxx", file=sys.stderr)
        sys.exit(1)

    # Build query
    query_parts = [
        "language:Python",
        "fork:false",
        f"stars:>={args.min_stars}",
        f"size:>={args.min_size}",
        "archived:false",
    ]
    if args.pushed_after:
        query_parts.append(f"pushed:>={args.pushed_after}")

    query = " ".join(query_parts)
    print(f"[query] {query}", file=sys.stderr)

    raw = search_repos(token=token, query=query, n=args.n)

    rows: List[Dict[str, Any]] = []
    for repo in raw:
        row = normalize_repo_row(repo)
        if args.with_sha:
            sha = get_latest_commit_sha(token, row["full_name"], row["default_branch"])
            row["snapshot_sha"] = sha
        rows.append(row)

    write_csv(args.out, rows)
    print(f"Saved {len(rows)} repos to {args.out}")


if __name__ == "__main__":
    main()