#!/usr/bin/env python3
"""
Site-specific parser for https://www.ajinomotocareers.com (row 9 / csv line 10).

Outputs (row-scoped):
- jobs_ajinomotocareers_com_row_9.json

Design goals:
- Evidence-only extraction from site JSON APIs.
- Exact count validation against true_open_jobs.
- Deterministic output ordering.
- Explicit diagnostics on failures.
"""

from __future__ import annotations

import csv
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

try:
    # Required by task instructions: use api_config.py as API URL/key source.
    from api_config import API_KEY, API_URL  # noqa: F401
except Exception:  # pragma: no cover
    API_URL = None
    API_KEY = None


ROW_INDEX = 9
CSV_LINE = 10
DEFAULT_CSV_FILE = "Book2.csv"
OUTPUT_FILE = f"jobs_ajinomotocareers_com_row_{ROW_INDEX}.json"

DEFAULT_SITE_URL = (
    "https://www.ajinomotocareers.com/careers-home/jobs?location=Hong%20Kong,"
    "%20Central%20and%20Western%20District,%20Hong%20Kong&woe=7&regionCode=HK"
    "&stretchUnit=MILES&stretch=10&page=1"
)
DEFAULT_TRUE_OPEN_JOBS = 1

BASE_DOMAIN = "https://www.ajinomotocareers.com"
INDEX_ENDPOINT = f"{BASE_DOMAIN}/api/jobs"
DETAIL_ENDPOINT_TEMPLATE = f"{BASE_DOMAIN}/api/jobs/{{slug}}/{{lang}}"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_SECONDS = 1.5


class ParserError(RuntimeError):
    """Controlled parser failure with explicit diagnostics."""


@dataclass(frozen=True)
class TargetConfig:
    site_url: str
    true_open_jobs: int
    csv_file: str
    csv_line: int


@dataclass(frozen=True)
class Strategy:
    index_endpoint: str
    detail_endpoint_template: str
    query_params: dict[str, str]
    analysis: dict[str, Any]


def log(message: str) -> None:
    print(f"[row {ROW_INDEX}] {message}")


def load_target_from_csv(csv_file: str = DEFAULT_CSV_FILE, csv_line: int = CSV_LINE) -> TargetConfig:
    path = Path(csv_file)
    if not path.exists():
        log(f"CSV not found at {csv_file}; using row-scoped fallback constants.")
        return TargetConfig(DEFAULT_SITE_URL, DEFAULT_TRUE_OPEN_JOBS, csv_file, csv_line)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))

    if not rows:
        raise ParserError("CSV is empty.")
    if csv_line - 1 >= len(rows):
        raise ParserError(f"csv_line={csv_line} is out of range. Total lines={len(rows)}")

    header = [h.strip() for h in rows[0]]
    row = rows[csv_line - 1]
    if len(row) < 2:
        raise ParserError(f"CSV line {csv_line} does not have expected 2 columns.")

    open_jobs_col = 0
    site_url_col = 1
    # Header can vary (e.g., true_open_jobsLstings typo in this file).
    for i, name in enumerate(header):
        n = name.lower().strip()
        if "true_open" in n or "open_jobs" in n:
            open_jobs_col = i
        if "site_url" in n or n == "url":
            site_url_col = i

    site_url = row[site_url_col].strip().strip('"')
    expected_raw = row[open_jobs_col].strip()
    try:
        true_open_jobs = int(float(expected_raw))
    except ValueError as exc:
        raise ParserError(f"Cannot parse true_open_jobs from '{expected_raw}'") from exc

    return TargetConfig(
        site_url=site_url,
        true_open_jobs=true_open_jobs,
        csv_file=str(path),
        csv_line=csv_line,
    )


def parse_query_params(site_url: str) -> dict[str, str]:
    parsed = urlparse(site_url)
    raw = parse_qs(parsed.query, keep_blank_values=True)
    params = {k: v[-1] for k, v in raw.items()}
    if "page" not in params:
        params["page"] = "1"
    return params


def request_json(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, str] | None = None,
    context: str = "request",
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            wait = BACKOFF_SECONDS ** attempt
            log(f"{context} failed (attempt {attempt}/{MAX_RETRIES}): {exc}; retrying in {wait:.1f}s")
            time.sleep(wait)
    raise ParserError(f"{context} failed after {MAX_RETRIES} attempts: {last_error}")


def detect_strategy(session: requests.Session, site_url: str) -> Strategy:
    params = parse_query_params(site_url)
    probe = request_json(session, INDEX_ENDPOINT, params=params, context="index probe")

    if not isinstance(probe, dict) or "jobs" not in probe:
        raise ParserError("Unexpected index response shape: expected JSON object with 'jobs'.")

    analysis = {
        "data_source": "XHR JSON API",
        "index_endpoint": INDEX_ENDPOINT,
        "detail_endpoint": DETAIL_ENDPOINT_TEMPLATE,
        "pagination": {
            "parameter": "page",
            "start": int(params.get("page", "1")),
            "stop_condition": "no jobs returned OR unique jobs >= totalCount",
            "total_count_field": "totalCount",
        },
        "unique_identifier": "data.slug (fallback: data.req_id)",
        "anti_bot": {
            "approach": "polite requests session with retries/backoff; no intrusive bypassing",
            "request_timeout_seconds": REQUEST_TIMEOUT,
            "max_retries": MAX_RETRIES,
        },
        "gpt_api_source": {
            "api_url_from_api_config": bool(API_URL),
            "api_key_from_api_config": bool(API_KEY),
        },
    }

    return Strategy(
        index_endpoint=INDEX_ENDPOINT,
        detail_endpoint_template=DETAIL_ENDPOINT_TEMPLATE,
        query_params=params,
        analysis=analysis,
    )


def normalize_job_stub(raw_job: dict[str, Any]) -> dict[str, Any] | None:
    data = raw_job.get("data") if isinstance(raw_job, dict) else None
    if not isinstance(data, dict):
        return None
    slug = str(data.get("slug") or "").strip()
    req_id = str(data.get("req_id") or "").strip()
    if not slug and not req_id:
        return None
    lang = str(data.get("language") or "en-us").strip() or "en-us"
    unique_id = slug or req_id
    return {
        "unique_id": unique_id,
        "slug": slug or req_id,
        "req_id": req_id or slug,
        "lang": lang,
        "title": data.get("title"),
        "location": data.get("full_location") or data.get("location_name"),
        "listing_data": data,
    }


def fetch_index(session: requests.Session, strategy: Strategy, expected_count: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params = dict(strategy.query_params)
    start_page = int(params.get("page", "1"))

    unique_stubs: dict[str, dict[str, Any]] = {}
    duplicates_removed = 0
    pages_fetched = 0
    reported_total_count = None

    page = start_page
    while True:
        params["page"] = str(page)
        payload = request_json(
            session,
            strategy.index_endpoint,
            params=params,
            context=f"index page {page}",
        )

        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            raise ParserError(f"Index page {page} response missing 'jobs' list.")

        pages_fetched += 1
        if isinstance(payload.get("totalCount"), int):
            reported_total_count = payload.get("totalCount")

        page_new = 0
        for raw in jobs:
            stub = normalize_job_stub(raw)
            if not stub:
                continue
            key = stub["unique_id"]
            if key in unique_stubs:
                duplicates_removed += 1
                continue
            unique_stubs[key] = stub
            page_new += 1

        log(f"page {page} fetched -> {len(jobs)} jobs found ({page_new} new, {len(unique_stubs)} unique total)")

        # End conditions
        if len(jobs) == 0:
            break
        if reported_total_count is not None and len(unique_stubs) >= reported_total_count:
            break
        if page_new == 0:
            break

        page += 1
        if page - start_page > 200:
            raise ParserError("Safety stop triggered: pagination exceeded 200 pages.")

    stubs = sorted(unique_stubs.values(), key=lambda x: (str(x.get("slug") or ""), str(x.get("title") or "")))

    diagnostics = {
        "pages_fetched": pages_fetched,
        "reported_total_count": reported_total_count,
        "unique_collected": len(stubs),
        "duplicates_removed": duplicates_removed,
        "query_params": dict(params),
    }

    if len(stubs) != expected_count:
        raise ParserError(
            "Count mismatch after index pagination. "
            f"expected_job_count={expected_count}, extracted_job_count={len(stubs)}, "
            f"reported_total_count={reported_total_count}, pages_fetched={pages_fetched}."
        )

    return stubs, diagnostics


def fetch_detail(session: requests.Session, slug: str, lang: str) -> dict[str, Any]:
    detail_url = DETAIL_ENDPOINT_TEMPLATE.format(slug=slug, lang=lang)
    return request_json(session, detail_url, context=f"detail slug={slug}")


def clean_html_to_text(html: str | None) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    text = unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = text.strip()
    return text or None


def build_canonical_job_url(slug: str) -> str:
    return f"{BASE_DOMAIN}/careers-home/jobs/{slug}"


def parse_detail(stub: dict[str, Any], detail: dict[str, Any], strategy: Strategy) -> dict[str, Any]:
    slug = str(detail.get("slug") or stub.get("slug") or "").strip()
    req_id = str(detail.get("req_id") or stub.get("req_id") or slug).strip()
    lang = str(detail.get("language") or stub.get("lang") or "en-us").strip() or "en-us"

    description_html = detail.get("description")
    responsibilities_html = detail.get("responsibilities")
    qualifications_html = detail.get("qualifications")

    description_blocks = [b for b in [description_html, responsibilities_html, qualifications_html] if isinstance(b, str) and b.strip()]
    merged_description_html = "\n<hr/>\n".join(description_blocks) if description_blocks else None

    canonical = build_canonical_job_url(slug)

    def first_tag_value(tag_key: str) -> str | None:
        val = detail.get(tag_key)
        if isinstance(val, list) and val:
            return str(val[0])
        if isinstance(val, str):
            return val
        return None

    job = {
        "job_id": req_id or slug,
        "job_url": canonical,
        "canonical_job_url": canonical,
        "url": canonical,
        "title": detail.get("title") or stub.get("title"),
        "company": detail.get("hiring_organization"),
        "location": detail.get("full_location") or detail.get("location_name") or stub.get("location"),
        "country": detail.get("country"),
        "country_code": detail.get("country_code"),
        "employment_type": detail.get("employment_type"),
        "category": detail.get("category") if detail.get("category") is not None else None,
        "team": first_tag_value("tags1"),
        "shift": first_tag_value("tags2"),
        "job_type_label": first_tag_value("tags3"),
        "remote_eligible": first_tag_value("tags4"),
        "posted_date": detail.get("posted_date"),
        "update_date": detail.get("update_date"),
        "create_date": detail.get("create_date"),
        "apply_url": detail.get("apply_url"),
        "language": lang,
        "job_description": clean_html_to_text(merged_description_html),
        "job_description_html": merged_description_html,
        "raw_source": {
            "source_type": "api",
            "index_endpoint": strategy.index_endpoint,
            "detail_endpoint": DETAIL_ENDPOINT_TEMPLATE.format(slug=slug, lang=lang),
            "query_params": strategy.query_params,
            "listing_data": stub.get("listing_data"),
            "detail_data": detail,
        },
    }

    # Evidence-only discipline: keep absent values as None.
    for key, value in list(job.items()):
        if value == "":
            job[key] = None

    return job


def validate_counts(jobs: list[dict[str, Any]], expected_count: int, duplicates_removed: int) -> dict[str, Any]:
    extracted_count = len(jobs)
    missing_fields_summary: dict[str, int] = {}

    tracked_fields = [
        "job_id",
        "job_url",
        "title",
        "company",
        "location",
        "posted_date",
        "job_description",
    ]

    for field in tracked_fields:
        missing_fields_summary[field] = sum(1 for j in jobs if not j.get(field))

    report = {
        "expected_job_count": expected_count,
        "extracted_job_count": extracted_count,
        "duplicates_removed": duplicates_removed,
        "missing_fields_summary": missing_fields_summary,
    }

    if extracted_count != expected_count:
        raise ParserError(
            "Validation failed: extracted_job_count does not match expected_job_count. "
            f"expected={expected_count}, extracted={extracted_count}"
        )

    return report


def write_jobs_output(records: list[dict[str, Any]]) -> None:
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def run() -> None:
    target = load_target_from_csv(DEFAULT_CSV_FILE, CSV_LINE)

    # Guardrail: this row-scoped parser is only for ajinomotocareers.com
    if "ajinomotocareers.com" not in target.site_url:
        raise ParserError(
            f"Target URL mismatch for row {ROW_INDEX}. Expected ajinomotocareers.com, got: {target.site_url}"
        )

    log(f"target site_url: {target.site_url}")
    log(f"expected true_open_jobs: {target.true_open_jobs}")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": target.site_url,
        }
    )

    try:
        strategy = detect_strategy(session, target.site_url)
        stubs, index_diag = fetch_index(session, strategy, target.true_open_jobs)

        jobs: list[dict[str, Any]] = []
        for idx, stub in enumerate(stubs, start=1):
            detail = fetch_detail(session, stub["slug"], stub["lang"])
            job = parse_detail(stub, detail, strategy)
            jobs.append(job)
            log(f"detail {idx}/{len(stubs)} parsed -> {job.get('job_id')}")

        jobs = sorted(jobs, key=lambda j: (str(j.get("job_id") or ""), str(j.get("job_url") or "")))

        validation = validate_counts(
            jobs,
            expected_count=target.true_open_jobs,
            duplicates_removed=index_diag.get("duplicates_removed", 0),
        )

        # keep row-scoped output strictly as a job list; include traceability in each job object.
        write_jobs_output(jobs)

        log(
            "success: "
            f"expected={validation['expected_job_count']} extracted={validation['extracted_job_count']} "
            f"duplicates_removed={validation['duplicates_removed']}"
        )

    except Exception as exc:  # noqa: BLE001
        # Explicit failure diagnostics in required jobs file when extraction fails.
        diagnostics_record = {
            "title": "PARSER_ERROR",
            "url": target.site_url,
            "job_url": target.site_url,
            "canonical_job_url": target.site_url,
            "error": str(exc),
            "expected_job_count": target.true_open_jobs,
            "extracted_job_count": 0,
            "diagnostics": {
                "site": "ajinomotocareers.com",
                "index_endpoint": INDEX_ENDPOINT,
                "detail_endpoint_template": DETAIL_ENDPOINT_TEMPLATE,
                "note": "Extraction failed explicitly; see error for details.",
            },
        }
        write_jobs_output([diagnostics_record])
        log(f"FAILED: {exc}")
        raise


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)
