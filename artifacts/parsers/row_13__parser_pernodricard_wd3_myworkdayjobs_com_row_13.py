#!/usr/bin/env python3
"""
Row-scoped parser for CSV line 14 (row label 13):
https://pernodricard.wd3.myworkdayjobs.com/en-US/pernod-ricard?locations=49fded0e3803019b6b5f55027f0e76b0&locationCountry=6cb77610a8a543aea2d6bc10457e35d4

Writes:
- jobs_pernodricard_wd3_myworkdayjobs_com_row_13.json
"""

from __future__ import annotations

import csv
import html
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
CSV_LINE = 14
ROW_LABEL = 13
SITE_URL = (
    "https://pernodricard.wd3.myworkdayjobs.com/en-US/pernod-ricard"
    "?locations=49fded0e3803019b6b5f55027f0e76b0&locationCountry=6cb77610a8a543aea2d6bc10457e35d4"
)
EXPECTED_COUNT = 5
OUTPUT_FILE = Path("jobs_pernodricard_wd3_myworkdayjobs_com_row_13.json")

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

    return {
        "expected": int(str(row[0]).strip()),
        "site_url": str(row[1]).strip(),
    }


def detect_strategy(site_url: str) -> dict[str, Any]:
    parsed = urlparse(site_url)
    query = parse_qs(parsed.query)

    applied_facets: dict[str, list[str]] = {}
    for key, values in query.items():
        cleaned = [v.strip() for v in values if isinstance(v, str) and v.strip()]
        if cleaned:
            applied_facets[key] = cleaned

    return {
        "data_source": "workday_cxs_api",
        "list_api": LIST_API,
        "detail_prefix": DETAIL_API_PREFIX,
        "pagination": {"param": "offset", "step": 20, "limit": 20},
        "applied_facets": applied_facets,
        "anti_bot": {"rate_limit_seconds": 0.2, "retries": 3, "timeout_seconds": 30},
    }


def make_session(site_url: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": WORKDAY_BASE,
            "Referer": site_url,
        }
    )
    return session


def fetch_index(
    session: requests.Session,
    strategy: dict[str, Any],
    expected_count: int,
) -> tuple[list[dict[str, Any]], int | None]:
    limit = int(strategy["pagination"]["limit"])
    offset = 0
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
            total_value = data.get("total")
            api_total = int(total_value) if isinstance(total_value, int) else None

        postings = data.get("jobPostings") or []
        print(f"page offset={offset} fetched -> {len(postings)} jobs found", file=sys.stderr)

        if not postings:
            break

        for stub in postings:
            path = stub.get("externalPath")
            if not isinstance(path, str) or not path.strip() or path in seen_paths:
                continue
            seen_paths.add(path)
            collected.append(stub)
            if len(collected) == expected_count:
                return collected, api_total

        offset += limit
        if api_total is not None and offset >= api_total:
            break
        time.sleep(float(strategy["anti_bot"]["rate_limit_seconds"]))

    return collected, api_total


def fetch_detail(session: requests.Session, external_path: str, retries: int = 3) -> dict[str, Any]:
    if not external_path:
        raise ValueError("external_path is required for detail fetch")

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
                raise ValueError(f"jobPostingInfo missing for {external_path}")
            return info
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt == retries:
                break
            time.sleep(wait)
            wait *= 2

    raise RuntimeError(f"Failed detail fetch for {external_path}: {last_err}")


def parse_slug(external_path: str | None) -> str | None:
    if not external_path:
        return None
    slug = external_path.rstrip("/").split("/")[-1]
    return slug or None


def html_to_text(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"<br\\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|li|h[1-6]|ul|ol)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\r", "", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def parse_city_state(location: str | None) -> tuple[str | None, str | None]:
    if not location:
        return None, None
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return location.strip(), None


def normalize_country(value: Any) -> str | None:
    if isinstance(value, dict):
        descriptor = value.get("descriptor")
        return descriptor.strip() if isinstance(descriptor, str) and descriptor.strip() else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def parse_detail(stub: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    external_path = stub.get("externalPath")
    fallback_url = urljoin(f"{WORKDAY_BASE}/en-US/pernod-ricard", external_path or "")

    canonical = detail.get("externalUrl")
    if not isinstance(canonical, str) or not canonical.strip():
        canonical = fallback_url
    canonical = canonical.strip()

    location = detail.get("location") or stub.get("locationsText")
    location = location.strip() if isinstance(location, str) and location.strip() else None

    city, state = parse_city_state(location)

    item = {
        "job_id": detail.get("id") or detail.get("jobReqId"),
        "title": detail.get("title") or stub.get("title"),
        "url": canonical,
        "job_url": canonical,
        "canonical_job_url": canonical,
        "company": None,
        "location": location,
        "city": city,
        "state": state,
        "country": normalize_country(detail.get("country")),
        "region": None,
        "team": None,
        "employment_type": detail.get("timeType") if isinstance(detail.get("timeType"), str) else None,
        "job_type": None,
        "posted_date": detail.get("startDate") if isinstance(detail.get("startDate"), str) else None,
        "slug": parse_slug(external_path if isinstance(external_path, str) else None),
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
    return {k: item.get(k, None) for k in ALLOWED_KEYS}


def validate_counts(
    jobs: list[dict[str, Any]],
    expected_count: int,
    before_dedupe_count: int,
) -> dict[str, Any]:
    extracted_count = len(jobs)
    duplicates_removed = before_dedupe_count - extracted_count
    missing_summary = {
        key: sum(1 for job in jobs if job.get(key) in (None, ""))
        for key in ["job_id", "title", "job_url", "location", "posted_date", "job_description"]
    }

    if extracted_count != expected_count:
        raise RuntimeError(
            "Count mismatch: "
            f"expected={expected_count}, extracted={extracted_count}, duplicates_removed={duplicates_removed}"
        )

    return {
        "expected_job_count": expected_count,
        "extracted_job_count": extracted_count,
        "duplicates_removed": duplicates_removed,
        "missing_fields_summary": missing_summary,
    }


def write_jobs(jobs: list[dict[str, Any]], output_file: Path) -> None:
    output_file.write_text(json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8")


def run() -> None:
    # Required by task instructions: use api_config.py as API URL/key source.
    _gpt_source = {"api_url": API_URL, "api_key_present": bool(API_KEY)}

    row = load_row(CSV_FILE, CSV_LINE)
    if row["site_url"] != SITE_URL:
        raise RuntimeError(
            f"CSV line {CSV_LINE} site_url mismatch: expected {SITE_URL!r}, got {row['site_url']!r}"
        )
    if row["expected"] != EXPECTED_COUNT:
        raise RuntimeError(
            f"CSV line {CSV_LINE} expected mismatch: expected {EXPECTED_COUNT}, got {row['expected']}"
        )

    strategy = detect_strategy(SITE_URL)
    session = make_session(SITE_URL)

    stubs, api_total = fetch_index(session, strategy, EXPECTED_COUNT)
    if len(stubs) != EXPECTED_COUNT:
        raise RuntimeError(
            "Pagination ended with mismatched count. "
            f"expected={EXPECTED_COUNT}, collected={len(stubs)}, api_total={api_total}, "
            f"appliedFacets={strategy['applied_facets']}"
        )

    jobs_pre_dedupe: list[dict[str, Any]] = []
    for stub in stubs:
        external_path = stub.get("externalPath")
        if not isinstance(external_path, str) or not external_path.strip():
            continue
        detail = fetch_detail(session, external_path, retries=3)
        jobs_pre_dedupe.append(parse_detail(stub, detail))

    deduped: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for job in jobs_pre_dedupe:
        unique_key = f"{job.get('job_id') or ''}|{job.get('canonical_job_url') or ''}"
        if unique_key in seen_keys:
            continue
        seen_keys.add(unique_key)
        deduped.append(job)

    deduped.sort(key=lambda x: ((x.get("title") or "").lower(), x.get("canonical_job_url") or ""))
    _validation = validate_counts(deduped, EXPECTED_COUNT, len(jobs_pre_dedupe))
    write_jobs(deduped, OUTPUT_FILE)


def write_diagnostic(error: Exception) -> None:
    diagnostic = {
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
        "job_description": (
            f"Parser failed explicitly for row {ROW_LABEL}. "
            f"Reason: {type(error).__name__}: {error}"
        ),
        "raw_source": {
            "source_type": "diagnostic",
            "search_stub": {
                "id": f"row_{ROW_LABEL}_error",
                "slug": f"row_{ROW_LABEL}_error",
                "title": "ERROR_DIAGNOSTIC",
                "locationText": None,
            },
        },
    }
    write_jobs([diagnostic], OUTPUT_FILE)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR row {ROW_LABEL}: {exc}", file=sys.stderr)
        write_diagnostic(exc)
        raise
