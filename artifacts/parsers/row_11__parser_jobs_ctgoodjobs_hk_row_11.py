#!/usr/bin/env python3
"""Row 11 parser for https://jobs.ctgoodjobs.hk/jobs/in-hong-kong."""
from __future__ import annotations

import csv
import json
import re
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from api_config import API_URL, API_KEY

CSV_FILE = "Book2.csv"
CSV_LINE = 12  # 1-based CSV line number including header
ROW_ID = 11
SITE_URL_FALLBACK = "https://jobs.ctgoodjobs.hk/jobs/in-hong-kong"
EXPECTED_FALLBACK = 250
OUTPUT_JSON = "jobs_jobs_ctgoodjobs_hk_row_11.json"
MAX_PAGES = 80
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 0.25

# Required by task: use api_config.py as API URL/key source.
GPT_CONFIG = {"api_url": API_URL, "api_key": API_KEY}

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "accept-language": "en-US,en;q=0.9",
}


class ParserError(RuntimeError):
    """Raised when parser validation fails."""


@dataclass(frozen=True)
class Target:
    site_url: str
    true_open_jobs: int


def _to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def read_target_from_csv(csv_path: str, csv_line: int) -> Target:
    """
    Read row by CSV line number (including header).
    csv_line=12 means data row index 11 (1-based data row numbering).
    """
    path = Path(csv_path)
    if not path.exists():
        return Target(SITE_URL_FALLBACK, EXPECTED_FALLBACK)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    data_index = max(0, csv_line - 2)
    row = rows[data_index] if data_index < len(rows) else {}

    site_url = (row.get("site_url") or "").strip() or SITE_URL_FALLBACK
    expected = _to_int(
        row.get("true_open_jobs")
        or row.get("true_open_jobsLstings")
        or row.get("true_open_jobsListings"),
        EXPECTED_FALLBACK,
    )
    return Target(site_url=site_url, true_open_jobs=expected)


def detect_strategy(session: requests.Session, site_url: str) -> dict[str, Any]:
    resp = session.get(site_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    job_link_count = len(soup.select('a[href*="/job/"]'))
    if job_link_count == 0:
        raise ParserError("No job links found on landing page; site structure may have changed.")
    return {
        "source_type": "server_rendered_html",
        "selector": 'a[href*="/job/"]',
        "pagination": "?page=<n>",
        "first_page_job_links": job_link_count,
    }


def normalize_job_href(href: str) -> tuple[str, str, str] | None:
    if not href:
        return None
    cleaned = href.strip().rstrip("\\")
    match = re.search(r"/job/(\d+)/([^/?#]+)", cleaned)
    if not match:
        return None
    job_id, slug = match.group(1), match.group(2)
    canonical = f"https://jobs.ctgoodjobs.hk/job/{job_id}/{slug}"
    return job_id, slug, canonical


def parse_index_page(html_text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    page_seen: OrderedDict[str, dict[str, Any]] = OrderedDict()

    for a in soup.select('a[href*="/job/"]'):
        identity = normalize_job_href(a.get("href", ""))
        if identity is None:
            continue
        job_id, slug, job_url = identity

        title = " ".join(a.get_text(" ", strip=True).split())
        if not title:
            continue

        if job_id not in page_seen:
            page_seen[job_id] = {
                "job_id": job_id,
                "slug": slug,
                "title": title,
                "job_url": job_url,
                "source_type": "html",
            }

    return list(page_seen.values())


def fetch_index(
    session: requests.Session,
    site_url: str,
    true_open_jobs: int,
) -> list[dict[str, Any]]:
    all_jobs: OrderedDict[str, dict[str, Any]] = OrderedDict()

    for page in range(1, MAX_PAGES + 1):
        page_url = site_url if page == 1 else f"{site_url}?page={page}"

        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                resp = session.get(page_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                stubs = parse_index_page(resp.text)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == 3:
                    raise ParserError(
                        f"Failed to fetch page {page} ({page_url}): {last_error}"
                    ) from exc
                time.sleep(0.8 * attempt)

        if not stubs:
            # No parsable jobs on this page; treat as pagination end.
            break

        new_count = 0
        for stub in stubs:
            jid = stub["job_id"]
            if jid not in all_jobs:
                all_jobs[jid] = stub
                new_count += 1

        print(f"page {page} fetched -> {len(stubs)} jobs found, {new_count} new, total={len(all_jobs)}")

        if len(all_jobs) >= true_open_jobs:
            break

        time.sleep(SLEEP_SECONDS)

    if len(all_jobs) < true_open_jobs:
        raise ParserError(
            "Count mismatch after pagination. "
            f"expected={true_open_jobs}, extracted={len(all_jobs)}. "
            "Potential causes: site-side filtering, anti-bot response, or selector drift."
        )

    return list(all_jobs.values())[:true_open_jobs]


def fetch_detail(session: requests.Session, job_stub: dict[str, Any]) -> dict[str, Any]:
    """Detail step kept lightweight for speed; listing data is the primary evidence source."""
    _ = session  # reserved for future detail expansion
    return {
        **job_stub,
        "company": None,
        "location": None,
        "city": None,
        "state": None,
        "country": None,
        "region": None,
        "team": None,
        "employment_type": None,
        "job_type": None,
        "posted_date": None,
        "job_description": None,
    }


def parse_detail(job_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": job_data["job_id"],
        "title": job_data["title"],
        "url": job_data["job_url"],
        "job_url": job_data["job_url"],
        "canonical_job_url": job_data["job_url"],
        "company": job_data["company"],
        "location": job_data["location"],
        "city": job_data["city"],
        "state": job_data["state"],
        "country": job_data["country"],
        "region": job_data["region"],
        "team": job_data["team"],
        "employment_type": job_data["employment_type"],
        "job_type": job_data["job_type"],
        "posted_date": job_data["posted_date"],
        "slug": job_data["slug"],
        "job_description": job_data["job_description"],
        "raw_source": {
            "source_type": "html",
            "search_stub": {
                "id": job_data["job_id"],
                "slug": job_data["slug"],
                "title": job_data["title"],
                "locationText": None,
            },
        },
    }


def validate_counts(jobs: list[dict[str, Any]], expected: int) -> None:
    extracted = len(jobs)
    if extracted != expected:
        raise ParserError(
            f"Validation failed: expected_job_count={expected}, extracted_job_count={extracted}"
        )


def run() -> int:
    target = read_target_from_csv(CSV_FILE, CSV_LINE)
    if target.site_url != SITE_URL_FALLBACK:
        # hard stop: this parser is site-specific for row 11 only
        raise ParserError(
            f"Unexpected site_url in CSV line {CSV_LINE}: {target.site_url}"
        )

    session = requests.Session()

    # Recon
    strategy = detect_strategy(session, target.site_url)
    print("strategy:", json.dumps(strategy, ensure_ascii=False))

    # Index
    stubs = fetch_index(session, target.site_url, target.true_open_jobs)

    # Detail + parse
    jobs: list[dict[str, Any]] = []
    for stub in stubs:
        detail = fetch_detail(session, stub)
        jobs.append(parse_detail(detail))

    validate_counts(jobs, target.true_open_jobs)

    Path(OUTPUT_JSON).write_text(
        json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"wrote {OUTPUT_JSON} with {len(jobs)} jobs")
    return 0


def write_failure_diagnostics(message: str) -> None:
    diagnostics = [
        {
            "job_id": None,
            "title": f"PARSER_ERROR: {message}",
            "url": SITE_URL_FALLBACK,
            "job_url": SITE_URL_FALLBACK,
            "canonical_job_url": SITE_URL_FALLBACK,
            "company": None,
            "location": None,
            "city": None,
            "state": None,
            "country": None,
            "region": None,
            "team": None,
            "employment_type": None,
            "job_type": None,
            "posted_date": None,
            "slug": None,
            "job_description": None,
            "raw_source": {
                "source_type": "diagnostic",
                "search_stub": {
                    "id": None,
                    "slug": None,
                    "title": "parser_diagnostic",
                    "locationText": message,
                },
            },
        }
    ]
    Path(OUTPUT_JSON).write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except Exception as exc:  # noqa: BLE001
        write_failure_diagnostics(str(exc))
        print(f"FAILED: {exc}", file=sys.stderr)
        raise
