#!/usr/bin/env python3
"""Site-specific parser for careers.coty.com (CSV line 9 / row_8)."""

from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

from api_config import API_KEY, API_URL


CSV_FILE = "Book2.csv"
CSV_LINE = 9
SITE_URL = (
    "https://careers.coty.com/search/?createNewAlert=false&q=&"
    "optionsFacetsDD_department=&optionsFacetsDD_country=HK&"
    "optionsFacetsDD_city=&optionsFacetsDD_customfield5="
)
TRUE_OPEN_JOBS = 8
OUTPUT_JOBS_FILE = Path("jobs_careers_coty_com_row_8.json")
TIMEOUT = 30


@dataclass(frozen=True)
class TargetConfig:
    expected_jobs: int
    site_url: str


def normalize_url(url: str) -> str:
    parts = urlsplit(url)
    cleaned = parts._replace(query="", fragment="")
    return urlunsplit(cleaned)


def load_target_from_csv(csv_file: str, csv_line: int) -> TargetConfig:
    with open(csv_file, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if csv_line < 1 or csv_line > len(rows):
        raise ValueError(f"csv_line {csv_line} out of range (1..{len(rows)})")

    row = rows[csv_line - 1]
    if len(row) < 2:
        raise ValueError(f"CSV line {csv_line} has insufficient columns: {row}")

    expected = int(row[0].strip())
    site_url = row[1].strip()
    return TargetConfig(expected_jobs=expected, site_url=site_url)


def detect_strategy(session: requests.Session, site_url: str) -> dict[str, Any]:
    r = session.get(site_url, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    job_links = [a.get("href", "") for a in soup.select("a[href*='/job/']")]
    has_server_rendered_jobs = any(job_links)

    return {
        "data_source": "server_rendered_html",
        "job_links_detected": len(job_links),
        "has_server_rendered_jobs": has_server_rendered_jobs,
        "pagination": "single_page_for_HK_filter",
        "anti_bot": {
            "request_rate": "serial requests with small delay",
            "retries": "3 with exponential backoff",
            "user_agent": "static desktop UA",
        },
        "api_config_source": {
            "api_url": API_URL,
            "api_key_present": bool(API_KEY),
        },
    }


def fetch_index(session: requests.Session, site_url: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    r = session.get(site_url, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    stubs: list[dict[str, Any]] = []
    seen: set[str] = set()
    raw_count = 0

    for a in soup.select("a[href*='/job/']"):
        href = a.get("href", "").strip()
        if not href:
            continue
        raw_count += 1
        job_url = normalize_url(urljoin(site_url, href))
        if job_url in seen:
            continue
        seen.add(job_url)
        title = a.get_text(" ", strip=True) or None
        stubs.append(
            {
                "job_url": job_url,
                "title_from_index": title,
                "raw_source": {
                    "source_type": "search_html_anchor",
                    "href": href,
                    "anchor_text": title,
                },
            }
        )

    diagnostics = {
        "page": 1,
        "raw_links_found": raw_count,
        "unique_jobs_found": len(stubs),
        "duplicates_removed": raw_count - len(stubs),
    }
    print(f"page 1 fetched -> {len(stubs)} unique jobs found")
    return stubs, diagnostics


def fetch_detail(session: requests.Session, job_url: str, retries: int = 3) -> tuple[str, str]:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(job_url, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text, normalize_url(r.url)
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < retries:
                time.sleep(0.8 * (2 ** (attempt - 1)))
    raise RuntimeError(f"detail fetch failed for {job_url}: {last_err}")


def html_to_clean_text(html_fragment: str) -> str:
    soup = BeautifulSoup(html_fragment or "", "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines()]
    return "\n".join(line for line in lines if line)


def parse_detail(detail_html: str, canonical_url: str, stub: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(detail_html, "html.parser")

    prop_map: dict[str, str] = {}
    for node in soup.select("[data-careersite-propertyid]"):
        key = (node.get("data-careersite-propertyid") or "").strip()
        if not key:
            continue
        value = node.get_text(" ", strip=True)
        if value:
            prop_map[key] = value

    desc_node = soup.select_one("span.jobdescription")
    description_html = str(desc_node) if desc_node else ""

    url_job_id_match = re.search(r"/(\d{6,})/?$", canonical_url)
    url_job_id = url_job_id_match.group(1) if url_job_id_match else None

    job = {
        "job_id": prop_map.get("customfield2") or url_job_id,
        "job_url": stub["job_url"],
        "canonical_job_url": canonical_url,
        "title": prop_map.get("title") or stub.get("title_from_index"),
        "company": "Coty",
        "location": prop_map.get("location"),
        "city": prop_map.get("city"),
        "country": prop_map.get("country"),
        "department": prop_map.get("department"),
        "posted_date": prop_map.get("date"),
        "job_description": html_to_clean_text(description_html),
        "raw_source": {
            "source_type": "detail_html",
            "detail_url": canonical_url,
            "property_fields": prop_map,
        },
    }

    # Explicit nulls for missing values.
    for key, value in list(job.items()):
        if value == "":
            job[key] = None

    return job


def validate_counts(
    jobs: list[dict[str, Any]],
    expected_count: int,
    index_diag: dict[str, Any],
    site_analysis: dict[str, Any],
) -> None:
    extracted_count = len(jobs)
    missing_title = sum(1 for j in jobs if not j.get("title"))
    missing_url = sum(1 for j in jobs if not j.get("job_url"))

    diagnostics = {
        "expected_job_count": expected_count,
        "extracted_job_count": extracted_count,
        "index_diagnostics": index_diag,
        "missing_fields_summary": {
            "missing_title": missing_title,
            "missing_job_url": missing_url,
        },
        "site_analysis": site_analysis,
    }

    if extracted_count != expected_count:
        raise RuntimeError(
            "COUNT_MISMATCH: "
            + json.dumps(diagnostics, ensure_ascii=False, indent=2)
        )
    if missing_title or missing_url:
        raise RuntimeError(
            "REQUIRED_FIELDS_MISSING: "
            + json.dumps(diagnostics, ensure_ascii=False, indent=2)
        )


def run() -> list[dict[str, Any]]:
    target = load_target_from_csv(CSV_FILE, CSV_LINE)

    # Hard guard for row-scoped deterministic target.
    if target.site_url != SITE_URL:
        raise RuntimeError(f"Unexpected site_url at CSV line {CSV_LINE}: {target.site_url}")
    if target.expected_jobs != TRUE_OPEN_JOBS:
        raise RuntimeError(
            f"Unexpected true_open_jobs at CSV line {CSV_LINE}: {target.expected_jobs}"
        )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    site_analysis = detect_strategy(session, target.site_url)
    stubs, index_diag = fetch_index(session, target.site_url)

    jobs: list[dict[str, Any]] = []
    for stub in stubs:
        detail_html, canonical = fetch_detail(session, stub["job_url"])
        jobs.append(parse_detail(detail_html, canonical, stub))
        time.sleep(0.2)

    validate_counts(jobs, target.expected_jobs, index_diag, site_analysis)

    jobs_sorted = sorted(jobs, key=lambda x: (x.get("job_id") or "", x.get("job_url") or ""))
    OUTPUT_JOBS_FILE.write_text(
        json.dumps(jobs_sorted, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Saved {len(jobs_sorted)} jobs to {OUTPUT_JOBS_FILE}")
    return jobs_sorted


def write_network_failure_diagnostics(error: Exception) -> None:
    diag = [
        {
            "title": "NETWORK_OR_RUNTIME_ERROR",
            "job_url": SITE_URL,
            "canonical_job_url": SITE_URL,
            "job_id": None,
            "company": "Coty",
            "location": None,
            "job_description": None,
            "error": str(error),
            "diagnostics": {
                "expected_job_count": TRUE_OPEN_JOBS,
                "extracted_job_count": 0,
                "reason": "network/dns unavailable or parser runtime failure",
            },
        }
    ]
    OUTPUT_JOBS_FILE.write_text(json.dumps(diag, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001
        write_network_failure_diagnostics(exc)
        raise
