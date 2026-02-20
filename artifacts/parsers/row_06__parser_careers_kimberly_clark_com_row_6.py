#!/usr/bin/env python3
"""Site-specific parser for careers.kimberly-clark.com (Book2.csv line 7 / row 6)."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from api_config import API_KEY, API_URL


CSV_FILE = "Book2.csv"
CSV_LINE = 7
ROW_ID = 6
OUTPUT_JOBS = Path("jobs_careers_kimberly_clark_com_row_6.json")
TARGET_SITE = (
    "https://careers.kimberly-clark.com/en/job-search"
    "?q=hong%20kong&first=0&numberOfResults=10&sort=jobposteddate"
)
WORKDAY_BASE = "https://kimberlyclark.wd1.myworkdayjobs.com"
INDEX_ENDPOINT = f"{WORKDAY_BASE}/wday/cxs/kimberlyclark/GLOBAL/jobs"
COMPANY_NAME = "Kimberly-Clark"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_BASE = 1.5


class ParserError(RuntimeError):
    """Base parser error."""


class CountMismatchError(ParserError):
    """Raised when extracted count != expected count."""


@dataclass(frozen=True)
class TargetConfig:
    expected_count: int
    site_url: str
    search_text: str
    first: int
    page_size: int


def _request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    json_payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(
                method=method,
                url=url,
                json=json_payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == MAX_RETRIES:
                break
            time.sleep(BACKOFF_BASE ** attempt)
    raise ParserError(f"Request failed after retries: {method} {url} :: {last_exc}")


def load_target_from_csv(csv_path: str = CSV_FILE, csv_line: int = CSV_LINE) -> TargetConfig:
    path = Path(csv_path)
    if not path.exists():
        raise ParserError(f"CSV file not found: {csv_path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    row_index = csv_line - 2  # line 1 header, line 2 => index 0
    if row_index < 0 or row_index >= len(rows):
        raise ParserError(f"csv_line {csv_line} out of range; total data rows={len(rows)}")

    row = rows[row_index]
    expected_raw = row.get("true_open_jobsLstings")
    site_url = (row.get("site_url") or "").strip()

    if not expected_raw:
        raise ParserError("Missing true_open_jobsLstings in CSV row")
    expected = int(expected_raw)
    if not site_url:
        raise ParserError("Missing site_url in CSV row")

    parsed = urlparse(site_url)
    qs = parse_qs(parsed.query)
    search_text = (qs.get("q") or [""])[0]
    first = int((qs.get("first") or ["0"])[0])
    page_size = int((qs.get("numberOfResults") or ["10"])[0])

    return TargetConfig(
        expected_count=expected,
        site_url=site_url,
        search_text=search_text,
        first=first,
        page_size=page_size,
    )


def detect_strategy(config: TargetConfig) -> dict[str, Any]:
    return {
        "source": "Workday CXS JSON API",
        "index_endpoint": INDEX_ENDPOINT,
        "detail_endpoint_pattern": f"{WORKDAY_BASE}/wday/cxs/kimberlyclark/GLOBAL{{externalPath}}",
        "pagination": {
            "type": "offset+limit",
            "offset_start": config.first,
            "limit": config.page_size,
            "stop_condition": "offset >= total OR empty jobPostings",
        },
        "identifier": "externalPath (fallback jobReqId)",
        "anti_bot": {
            "notes": "Use direct API endpoint, low request rate, retries with exponential backoff.",
            "retry_count": MAX_RETRIES,
        },
        "gpt_config_source": {
            "api_url": API_URL,
            "api_key_loaded": bool(API_KEY),
            "usage": "optional description cleaning only",
        },
    }


def fetch_index(session: requests.Session, config: TargetConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    offset = config.first
    page_size = config.page_size
    dedup: dict[str, dict[str, Any]] = {}
    page_logs: list[dict[str, Any]] = []
    declared_total: int | None = None

    while True:
        payload = {
            "appliedFacets": {},
            "limit": page_size,
            "offset": offset,
            "searchText": config.search_text,
        }
        resp = _request_with_retries(
            session,
            "POST",
            INDEX_ENDPOINT,
            json_payload=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        data = resp.json()

        if declared_total is None:
            declared_total = int(data.get("total") or 0)

        postings = data.get("jobPostings") or []
        page_logs.append({"offset": offset, "fetched": len(postings)})

        for post in postings:
            key = (post.get("externalPath") or "").strip() or "|".join(post.get("bulletFields") or [])
            if not key:
                key = hashlib.sha256(json.dumps(post, sort_keys=True).encode("utf-8")).hexdigest()
            if key not in dedup:
                dedup[key] = post

        if not postings:
            break

        offset += page_size
        if declared_total is not None and offset >= declared_total:
            break

    records = list(dedup.values())
    diagnostics = {
        "declared_total": declared_total,
        "page_logs": page_logs,
        "unique_index_records": len(records),
    }
    return records, diagnostics


def fetch_detail(session: requests.Session, external_path: str) -> dict[str, Any]:
    detail_url = f"{WORKDAY_BASE}/wday/cxs/kimberlyclark/GLOBAL{external_path}"
    resp = _request_with_retries(
        session,
        "GET",
        detail_url,
        headers={"Accept": "application/json"},
    )
    return resp.json()


def html_to_text(html: str | None) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text("\n")
    text = unescape(text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()


def clean_description_with_gpt(html: str | None) -> str:
    """Optional GPT cleaning path; source credentials loaded from api_config.py."""
    # Kept optional and disabled by default to avoid external dependency during parsing.
    _ = API_URL, API_KEY
    return html_to_text(html)


def parse_detail(index_record: dict[str, Any], detail_payload: dict[str, Any]) -> dict[str, Any]:
    info = detail_payload.get("jobPostingInfo") or {}

    external_path = (index_record.get("externalPath") or "").strip()
    external_url = (info.get("externalUrl") or "").strip()
    canonical_job_url = external_url or (f"{WORKDAY_BASE}/GLOBAL{external_path}" if external_path else None)

    job_req_id = info.get("jobReqId")
    job_id = str(job_req_id).strip() if job_req_id else None
    if not job_id:
        stable_input = f"{canonical_job_url}|{info.get('title')}|{info.get('location')}"
        job_id = hashlib.sha256(stable_input.encode("utf-8")).hexdigest()[:16]

    description_html = info.get("jobDescription")
    job_description = clean_description_with_gpt(description_html)

    country_descriptor = None
    country_obj = info.get("country") or {}
    if isinstance(country_obj, dict):
        country_descriptor = country_obj.get("descriptor")

    job = {
        "job_id": job_id,
        "title": info.get("title") or index_record.get("title") or None,
        "url": canonical_job_url,
        "job_url": canonical_job_url,
        "canonical_job_url": canonical_job_url,
        "company": COMPANY_NAME,
        "location": info.get("location") or index_record.get("locationsText") or None,
        "country": country_descriptor,
        "employment_type": info.get("timeType") or None,
        "posted_date": info.get("startDate") or None,
        "job_requisition_id": str(job_req_id) if job_req_id else None,
        "job_description": job_description or None,
        "raw_source": {
            "source_type": "api",
            "index_endpoint": INDEX_ENDPOINT,
            "detail_endpoint": f"{WORKDAY_BASE}/wday/cxs/kimberlyclark/GLOBAL{external_path}",
            "index_record": index_record,
            "jobPostingInfo": info,
        },
    }

    for key, value in list(job.items()):
        if isinstance(value, str):
            job[key] = value.strip() or None

    return job


def validate_counts(
    jobs: list[dict[str, Any]],
    expected_count: int,
    index_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    extracted_count = len(jobs)
    duplicates_removed = index_diagnostics.get("unique_index_records", 0) - extracted_count
    if duplicates_removed < 0:
        duplicates_removed = 0

    missing_fields_summary: dict[str, int] = {}
    track_fields = [
        "job_id",
        "title",
        "job_url",
        "company",
        "location",
        "posted_date",
        "employment_type",
        "job_description",
    ]
    for field in track_fields:
        missing_fields_summary[field] = sum(1 for job in jobs if not job.get(field))

    result = {
        "expected_job_count": expected_count,
        "extracted_job_count": extracted_count,
        "duplicates_removed": duplicates_removed,
        "missing_fields_summary": missing_fields_summary,
        "index_diagnostics": index_diagnostics,
        "valid": extracted_count == expected_count,
    }
    if extracted_count != expected_count:
        raise CountMismatchError(
            json.dumps(
                {
                    "error": "COUNT_MISMATCH",
                    "message": (
                        f"Expected {expected_count} jobs but extracted {extracted_count}. "
                        "Stopping per strict count rule."
                    ),
                    "diagnostics": result,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return result


def run() -> None:
    config = load_target_from_csv(CSV_FILE, CSV_LINE)
    if config.site_url != TARGET_SITE:
        raise ParserError(
            f"CSV row site URL mismatch. expected={TARGET_SITE} got={config.site_url}"
        )

    strategy = detect_strategy(config)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Origin": WORKDAY_BASE,
            "Referer": config.site_url,
        }
    )

    index_records, index_diagnostics = fetch_index(session, config)

    jobs: list[dict[str, Any]] = []
    for record in sorted(index_records, key=lambda x: (x.get("externalPath") or "", x.get("title") or "")):
        external_path = (record.get("externalPath") or "").strip()
        if not external_path:
            continue
        detail_payload = fetch_detail(session, external_path)
        jobs.append(parse_detail(record, detail_payload))
        time.sleep(0.25)

    jobs = sorted(jobs, key=lambda x: (str(x.get("posted_date") or ""), str(x.get("job_id") or ""), str(x.get("job_url") or "")))
    _ = validate_counts(jobs, config.expected_count, index_diagnostics)

    OUTPUT_JOBS.write_text(json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001
        diagnostics_payload = [
            {
                "title": "PARSER_ERROR",
                "url": TARGET_SITE,
                "job_url": TARGET_SITE,
                "error": str(exc),
            }
        ]
        OUTPUT_JOBS.write_text(
            json.dumps(diagnostics_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        raise
