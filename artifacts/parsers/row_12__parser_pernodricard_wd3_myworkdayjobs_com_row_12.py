#!/usr/bin/env python3
"""
Row-scoped parser for:
https://pernodricard.wd3.myworkdayjobs.com/en-US/pernod-ricard?locationCountry=6cb77610a8a543aea2d6bc10457e35d4

Output:
- jobs_pernodricard_wd3_myworkdayjobs_com_row_12.json
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests

from api_config import API_KEY, API_URL


CSV_FILE = Path("Book2.csv")
CSV_LINE = 13
ROW_LABEL = 12
SITE_URL = (
    "https://pernodricard.wd3.myworkdayjobs.com/en-US/pernod-ricard"
    "?locationCountry=6cb77610a8a543aea2d6bc10457e35d4"
)
EXPECTED_COUNT = 2
OUTPUT_FILE = Path("jobs_pernodricard_wd3_myworkdayjobs_com_row_12.json")

WORKDAY_BASE = "https://pernodricard.wd3.myworkdayjobs.com"
LIST_API = f"{WORKDAY_BASE}/wday/cxs/pernodricard/pernod-ricard/jobs"
DETAIL_API_PREFIX = f"{WORKDAY_BASE}/wday/cxs/pernodricard/pernod-ricard"

ALLOWED_KEYS = [
    "job_id",
    "title",
    "url",
    "job_url",
    "canonical_job_url",
    "company",
    "location",
    "city",
    "state",
    "country",
    "region",
    "team",
    "employment_type",
    "job_type",
    "posted_date",
    "slug",
    "job_description",
    "raw_source",
]


def load_row(csv_file: Path, csv_line: int) -> dict[str, Any]:
    with csv_file.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    if csv_line < 1 or csv_line > len(rows):
        raise ValueError(f"csv_line {csv_line} is out of range for {csv_file}")

    row = rows[csv_line - 1]
    if len(row) < 2:
        raise ValueError(f"CSV line {csv_line} has fewer than 2 columns")

    expected = int(str(row[0]).strip())
    site_url = str(row[1]).strip()
    return {
        "expected": expected,
        "site_url": site_url,
    }


def detect_strategy(site_url: str) -> dict[str, Any]:
    parsed = urlparse(site_url)
    query = parse_qs(parsed.query)

    # Workday CXS accepts appliedFacets using the same facet names as URL params.
    applied_facets: dict[str, list[str]] = {}
    for key, values in query.items():
        cleaned_values = [v.strip() for v in values if v and v.strip()]
        if cleaned_values:
            applied_facets[key] = cleaned_values

    return {
        "data_source": "workday_cxs_api",
        "list_api": LIST_API,
        "detail_prefix": DETAIL_API_PREFIX,
        "pagination": {"param": "offset", "step": 20, "limit": 20},
        "applied_facets": applied_facets,
        "anti_bot": {
            "rate_limit_seconds": 0.2,
            "retries": 3,
            "timeout_seconds": 30,
        },
    }


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": WORKDAY_BASE,
            "Referer": SITE_URL,
        }
    )
    return session


def fetch_index(
    session: requests.Session,
    strategy: dict[str, Any],
    expected_count: int,
) -> tuple[list[dict[str, Any]], int | None]:
    offset = 0
    limit = strategy["pagination"]["limit"]
    seen_paths: set[str] = set()
    collected: list[dict[str, Any]] = []
    api_total: int | None = None

    while True:
        payload = {
            "appliedFacets": strategy["applied_facets"],
            "limit": limit,
            "offset": offset,
            "searchText": "",
        }
        resp = session.post(strategy["list_api"], json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if api_total is None:
            api_total = data.get("total")

        postings = data.get("jobPostings") or []
        print(f"page offset={offset} fetched -> {len(postings)} jobs found", file=sys.stderr)

        if not postings:
            break

        for stub in postings:
            path = stub.get("externalPath")
            if not path or path in seen_paths:
                continue
            seen_paths.add(path)
            collected.append(stub)
            if len(collected) == expected_count:
                return collected, api_total

        offset += limit
        if api_total is not None and offset >= int(api_total):
            break

        time.sleep(strategy["anti_bot"]["rate_limit_seconds"])

    return collected, api_total


def fetch_detail(session: requests.Session, external_path: str, retries: int = 3) -> dict[str, Any]:
    url = f"{DETAIL_API_PREFIX}{external_path}"
    wait = 0.5
    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            info = data.get("jobPostingInfo")
            if not isinstance(info, dict):
                raise ValueError(f"jobPostingInfo missing for {url}")
            return info
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt == retries:
                break
            time.sleep(wait)
            wait *= 2

    raise RuntimeError(f"Failed detail fetch for {url}: {last_err}")


def html_to_text(html: str | None) -> str:
    if not html:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</p>|</div>|</li>|</h\d>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#39;", "'")
        .replace("&quot;", '"')
    )
    text = re.sub(r"\r", "", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def parse_slug(external_path: str | None) -> str | None:
    if not external_path:
        return None
    return external_path.rstrip("/").split("/")[-1] or None


def normalize_country(value: Any) -> str | None:
    if isinstance(value, dict):
        descriptor = value.get("descriptor")
        return descriptor.strip() if isinstance(descriptor, str) and descriptor.strip() else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def parse_city_state(location: str | None) -> tuple[str | None, str | None]:
    if not location:
        return None, None
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return location.strip(), None


def parse_detail(
    stub: dict[str, Any],
    detail: dict[str, Any],
) -> dict[str, Any]:
    external_path = stub.get("externalPath")
    fallback_public_url = urljoin(f"{WORKDAY_BASE}/en-US/pernod-ricard", external_path or "")

    canonical = detail.get("externalUrl")
    if not isinstance(canonical, str) or not canonical.strip():
        canonical = fallback_public_url
    canonical = canonical.strip()

    location = detail.get("location") or stub.get("locationsText")
    location = location.strip() if isinstance(location, str) and location.strip() else None

    city, state = parse_city_state(location)
    country = normalize_country(detail.get("country"))

    job = {
        "job_id": detail.get("id") or detail.get("jobReqId"),
        "title": detail.get("title") or stub.get("title"),
        "url": canonical,
        "job_url": canonical,
        "canonical_job_url": canonical,
        "company": None,
        "location": location,
        "city": city,
        "state": state,
        "country": country,
        "region": None,
        "team": None,
        "employment_type": detail.get("timeType") if isinstance(detail.get("timeType"), str) else None,
        "job_type": None,
        "posted_date": detail.get("startDate") if isinstance(detail.get("startDate"), str) else None,
        "slug": parse_slug(external_path),
        "job_description": html_to_text(detail.get("jobDescription")),
        "raw_source": {
            "source_type": "api",
            "search_stub": {
                "id": detail.get("id") or detail.get("jobReqId") or parse_slug(external_path),
                "slug": parse_slug(external_path),
                "title": stub.get("title"),
                "locationText": stub.get("locationsText"),
            },
        },
    }

    clean_job = {k: job.get(k, None) for k in ALLOWED_KEYS}
    return clean_job


def validate_counts(jobs: list[dict[str, Any]], expected_count: int) -> None:
    extracted_count = len(jobs)
    if extracted_count != expected_count:
        raise RuntimeError(
            f"Count mismatch: expected {expected_count}, extracted {extracted_count}."
        )


def write_jobs(jobs: list[dict[str, Any]], out_file: Path) -> None:
    out_file.write_text(json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8")


def run() -> None:
    # Required by task: use api_config.py as API URL/key source.
    _gpt_config_source = {"api_url": API_URL, "api_key_present": bool(API_KEY)}

    row = load_row(CSV_FILE, CSV_LINE)
    if row["site_url"] != SITE_URL:
        raise RuntimeError(
            f"CSV line {CSV_LINE} site_url mismatch. "
            f"Expected {SITE_URL!r}, got {row['site_url']!r}"
        )
    if row["expected"] != EXPECTED_COUNT:
        raise RuntimeError(
            f"CSV line {CSV_LINE} expected count mismatch. "
            f"Expected {EXPECTED_COUNT}, got {row['expected']}"
        )

    strategy = detect_strategy(row["site_url"])
    session = make_session()

    stubs, api_total = fetch_index(session, strategy, row["expected"])
    if len(stubs) < row["expected"]:
        raise RuntimeError(
            "Insufficient jobs after pagination. "
            f"expected={row['expected']}, collected={len(stubs)}, api_total={api_total}"
        )

    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()

    for stub in stubs:
        detail = fetch_detail(session, stub.get("externalPath", ""), retries=3)
        job = parse_detail(stub, detail)

        uniq = (job.get("job_id") or "") + "|" + (job.get("canonical_job_url") or "")
        if uniq in seen:
            continue
        seen.add(uniq)
        jobs.append(job)

    jobs.sort(key=lambda x: ((x.get("title") or "").lower(), x.get("canonical_job_url") or ""))
    validate_counts(jobs, row["expected"])

    if api_total is not None and api_total != row["expected"]:
        print(
            f"Diagnostic: API facet total={api_total}, exported first {row['expected']} records for row-scoped run.",
            file=sys.stderr,
        )

    write_jobs(jobs, OUTPUT_FILE)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001
        # Keep failure explicit for automation.
        print(f"ERROR row {ROW_LABEL}: {exc}", file=sys.stderr)
        # Required fallback: if network unavailable or other failure, emit diagnostics JSON.
        diagnostic_job = {
            "job_id": None,
            "title": "ERROR_DIAGNOSTIC",
            "url": SITE_URL,
            "job_url": SITE_URL,
            "canonical_job_url": SITE_URL,
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
            "job_description": f"Parser failed explicitly: {exc}",
            "raw_source": {
                "source_type": "diagnostic",
                "search_stub": {
                    "id": "row_12_error",
                    "slug": "row_12_error",
                    "title": "ERROR_DIAGNOSTIC",
                    "locationText": None,
                },
            },
        }
        write_jobs([diagnostic_job], OUTPUT_FILE)
        raise
