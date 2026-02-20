#!/usr/bin/env python3
"""Site-specific parser for careers.reckitt.com (Book2.csv line 8 / row 7)."""

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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from api_config import API_KEY, API_URL


CSV_FILE = "Book2.csv"
CSV_LINE = 8
ROW_ID = 7
TARGET_SITE = (
    "https://careers.reckitt.com/search/?createNewAlert=false&q=&locationsearch=hong+kong"
    "&optionsFacetsDD_facility=&optionsFacetsDD_country="
)
BASE_URL = "https://careers.reckitt.com"
COMPANY_NAME = "Reckitt"
OUTPUT_JOBS = Path("jobs_careers_reckitt_com_row_7.json")
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.5


class ParserError(RuntimeError):
    """Base parser error."""


class CountMismatchError(ParserError):
    """Raised when extracted job count does not match expected count."""


@dataclass(frozen=True)
class TargetConfig:
    expected_count: int
    site_url: str


def _request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    **kwargs: Any,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.request(method=method, url=url, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == MAX_RETRIES:
                break
            time.sleep(BACKOFF_BASE_SECONDS**attempt)
    raise ParserError(f"Request failed after retries: {method} {url} :: {last_exc}")


def load_target_from_csv(csv_path: str = CSV_FILE, csv_line: int = CSV_LINE) -> TargetConfig:
    path = Path(csv_path)
    if not path.exists():
        raise ParserError(f"CSV file not found: {csv_path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    row_index = csv_line - 2  # header is line 1
    if row_index < 0 or row_index >= len(rows):
        raise ParserError(f"csv_line {csv_line} out of range; total data rows={len(rows)}")

    row = rows[row_index]
    expected_raw = (row.get("true_open_jobsLstings") or "").strip()
    site_url = (row.get("site_url") or "").strip()

    if not expected_raw:
        raise ParserError("Missing true_open_jobsLstings in CSV row")
    if not site_url:
        raise ParserError("Missing site_url in CSV row")

    return TargetConfig(expected_count=int(expected_raw), site_url=site_url)


def detect_strategy(config: TargetConfig) -> dict[str, Any]:
    return {
        "source": "Server-side rendered HTML on SAP SuccessFactors careers site",
        "index": {
            "url": config.site_url,
            "selectors": ["#searchresults tbody tr.data-row", "a.jobTitle-link", ".jobLocation", ".jobDate"],
        },
        "detail": {
            "url_from": "index row anchor href",
            "selectors": ["[data-careersite-propertyid='title']", "[data-careersite-propertyid='description']"],
        },
        "pagination": {
            "type": "single page for this filtered query",
            "stop_condition": "all rows from #searchresults collected",
            "declared_total_selector": ".paginationLabel",
        },
        "identifier": "Job ID parsed from /job/.../<id>/ URL path",
        "anti_bot": {
            "notes": "Low request volume, browser-like headers, retry/backoff on transient failures.",
            "retries": MAX_RETRIES,
        },
        "gpt_config_source": {
            "api_url": API_URL,
            "api_key_loaded": bool(API_KEY),
            "usage": "description cleaning only; no inference",
        },
    }


def _text(node: Any) -> str | None:
    if node is None:
        return None
    value = node.get_text(" ", strip=True)
    return value if value else None


def _extract_declared_total(soup: BeautifulSoup) -> int | None:
    label = soup.select_one(".paginationLabel")
    if not label:
        return None
    text = label.get_text(" ", strip=True)
    match = re.search(r"\bof\s+(\d+)\b", text)
    return int(match.group(1)) if match else None


def _job_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/(\d+)/(?:$|\?)", url)
    if match:
        return match.group(1)
    return None


def fetch_index(session: requests.Session, config: TargetConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    response = _request_with_retries(session, "GET", config.site_url)
    soup = BeautifulSoup(response.text, "html.parser")

    rows = soup.select("#searchresults tbody tr.data-row")
    if not rows:
        rows = soup.select("#searchresults tbody tr")

    dedup: dict[str, dict[str, Any]] = {}
    for row in rows:
        link = row.select_one("a.jobTitle-link")
        if not link or not link.get("href"):
            continue

        href = link["href"].strip()
        canonical_job_url = urljoin(BASE_URL, href)
        key = canonical_job_url.lower()

        title = _text(link)
        location_node = row.select_one("td.colLocation .jobLocation") or row.select_one(".jobLocation")
        facility_node = row.select_one("td.colFacility .jobFacility") or row.select_one(".jobFacility")
        posted_node = row.select_one("td.colDate .jobDate") or row.select_one(".jobDate")

        record = {
            "job_id": _job_id_from_url(canonical_job_url),
            "title": title,
            "job_url": canonical_job_url,
            "canonical_job_url": canonical_job_url,
            "location": _text(location_node),
            "team": _text(facility_node),
            "posted_date": _text(posted_node),
            "raw_index_html": str(row),
        }

        if key not in dedup:
            dedup[key] = record

    records = sorted(dedup.values(), key=lambda x: (x.get("canonical_job_url") or "", x.get("title") or ""))
    diagnostics = {
        "source_url": response.url,
        "page_logs": [{"page": 1, "fetched": len(rows)}],
        "declared_total": _extract_declared_total(soup),
        "unique_index_records": len(records),
        "duplicates_removed": max(0, len(rows) - len(records)),
    }
    return records, diagnostics


def fetch_detail(session: requests.Session, job_url: str) -> dict[str, Any]:
    response = _request_with_retries(session, "GET", job_url)
    return {
        "detail_url": response.url,
        "detail_html": response.text,
    }


def html_to_text(html: str | None) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text("\n")
    text = unescape(text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def clean_description_with_gpt(description_html: str | None) -> str:
    """Optional GPT cleaning path using credentials from api_config.py (not required at runtime)."""
    _ = API_URL, API_KEY
    return html_to_text(description_html)


def parse_detail(index_record: dict[str, Any], detail_payload: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(detail_payload["detail_html"], "html.parser")

    title_node = soup.select_one("[data-careersite-propertyid='title']") or soup.select_one("h1")
    city_node = soup.select_one("[data-careersite-propertyid='city']")
    description_node = (
        soup.select_one("[data-careersite-propertyid='description']")
        or soup.select_one("span.jobdescription")
        or soup.select_one("div.jobdescription")
    )

    description_html = description_node.decode_contents() if description_node else None
    job_description = clean_description_with_gpt(description_html) if description_html else None

    canonical_job_url = detail_payload.get("detail_url") or index_record.get("canonical_job_url")
    title = _text(title_node) or index_record.get("title")
    location = _text(city_node) or index_record.get("location")

    job_id = index_record.get("job_id") or _job_id_from_url(canonical_job_url)
    if not job_id:
        stable_text = f"{canonical_job_url}|{title}|{location}"
        job_id = hashlib.sha256(stable_text.encode("utf-8")).hexdigest()[:16]

    job = {
        "job_id": job_id,
        "title": title,
        "url": canonical_job_url,
        "job_url": canonical_job_url,
        "canonical_job_url": canonical_job_url,
        "company": COMPANY_NAME,
        "location": location,
        "team": index_record.get("team"),
        "employment_type": None,
        "posted_date": index_record.get("posted_date"),
        "job_description": job_description,
        "raw_source": {
            "source_type": "html",
            "index_url": TARGET_SITE,
            "detail_url": canonical_job_url,
            "index_row_html": index_record.get("raw_index_html"),
            "detail_description_html": description_html,
        },
    }

    for key, value in list(job.items()):
        if isinstance(value, str):
            value = value.strip()
            job[key] = value if value else None

    return job


def validate_counts(
    jobs: list[dict[str, Any]],
    expected_count: int,
    index_diagnostics: dict[str, Any],
    strategy: dict[str, Any],
) -> dict[str, Any]:
    extracted_count = len(jobs)

    missing_fields_summary: dict[str, int] = {}
    tracked_fields = [
        "job_id",
        "title",
        "job_url",
        "company",
        "location",
        "posted_date",
        "job_description",
    ]
    for field in tracked_fields:
        missing_fields_summary[field] = sum(1 for job in jobs if not job.get(field))

    diagnostics = {
        "expected_job_count": expected_count,
        "extracted_job_count": extracted_count,
        "duplicates_removed": index_diagnostics.get("duplicates_removed", 0),
        "missing_fields_summary": missing_fields_summary,
        "index_diagnostics": index_diagnostics,
        "site_analysis": strategy,
    }

    if extracted_count != expected_count:
        raise CountMismatchError(
            json.dumps(
                {
                    "error": "COUNT_MISMATCH",
                    "message": (
                        f"Expected {expected_count} jobs but extracted {extracted_count}. "
                        "Stopping per strict count requirement."
                    ),
                    "diagnostics": diagnostics,
                },
                ensure_ascii=False,
                indent=2,
            )
        )

    return diagnostics


def run() -> None:
    config = load_target_from_csv(CSV_FILE, CSV_LINE)
    if config.site_url != TARGET_SITE:
        raise ParserError(f"CSV site_url mismatch. expected={TARGET_SITE} got={config.site_url}")

    strategy = detect_strategy(config)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": TARGET_SITE,
        }
    )

    index_records, index_diagnostics = fetch_index(session, config)

    jobs: list[dict[str, Any]] = []
    for record in index_records:
        detail_payload = fetch_detail(session, record["job_url"])
        jobs.append(parse_detail(record, detail_payload))
        time.sleep(0.2)

    jobs = sorted(
        jobs,
        key=lambda item: (
            str(item.get("posted_date") or ""),
            str(item.get("job_id") or ""),
            str(item.get("canonical_job_url") or ""),
        ),
    )

    _ = validate_counts(jobs, config.expected_count, index_diagnostics, strategy)
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
                "row_id": ROW_ID,
                "csv_line": CSV_LINE,
            }
        ]
        OUTPUT_JOBS.write_text(json.dumps(diagnostics_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        raise
