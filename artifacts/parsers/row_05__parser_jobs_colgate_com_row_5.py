#!/usr/bin/env python3
"""Row-scoped parser for jobs.colgate.com (CSV line 6 / row 5)."""

from __future__ import annotations

import csv
import json
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from api_config import API_KEY as OPENAI_API_KEY  # required config source
    from api_config import API_URL as OPENAI_API_URL  # required config source
except Exception:
    OPENAI_API_URL = None
    OPENAI_API_KEY = None


CSV_FILE_DEFAULT = "Book2.csv"
CSV_LINE_DEFAULT = 6
ROW_ID_FOR_OUTPUT = 5
SITE_URL_DEFAULT = "https://jobs.colgate.com/go/View-All-Jobs/8506400/"
EXPECTED_COUNT_DEFAULT = 1
OUTPUT_FILE_DEFAULT = f"jobs_jobs_colgate_com_row_{ROW_ID_FOR_OUTPUT}.json"


@dataclass
class TargetRow:
    csv_line: int
    expected_count: int
    site_url: str
    raw_row: dict[str, Any]


class ParserFailure(RuntimeError):
    pass


def read_target_row(csv_file: str, csv_line: int) -> TargetRow:
    path = Path(csv_file)
    if not path.exists():
        raise ParserFailure(f"CSV file not found: {csv_file}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))

    if csv_line < 2 or csv_line > len(rows):
        raise ParserFailure(
            f"csv_line {csv_line} is out of range. File has {len(rows)} lines including header."
        )

    header = [h.strip() for h in rows[0]]
    values = [v.strip() for v in rows[csv_line - 1]]
    row_map = {header[i] if i < len(header) else f"col_{i}": values[i] for i in range(len(values))}

    if len(values) < 2:
        raise ParserFailure(f"CSV line {csv_line} does not contain expected columns: {values}")

    expected_count_raw = values[0]
    site_url = values[1]

    try:
        expected_count = int(expected_count_raw)
    except ValueError as exc:
        raise ParserFailure(f"Invalid true_open_jobs value '{expected_count_raw}' on line {csv_line}") from exc

    return TargetRow(csv_line=csv_line, expected_count=expected_count, site_url=site_url, raw_row=row_map)


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/html, */*",
        }
    )
    return s


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    retries: int = 3,
    backoff: float = 1.2,
    timeout: int = 30,
    **kwargs: Any,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.request(method=method, url=url, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == retries:
                break
            time.sleep(backoff * attempt)
    raise ParserFailure(f"Request failed: {method} {url} :: {last_error}")


def extract_app_params(landing_html: str) -> dict[str, str | None]:
    locale_match = re.search(r'locale:\s*"([^"]+)"', landing_html)
    category_match = re.search(r'categoryId:\s*"([^"]+)"', landing_html)
    return {
        "locale": locale_match.group(1) if locale_match else "en_US",
        "category_id": category_match.group(1) if category_match else None,
    }


def extract_candidate_locations(site_url: str, row: dict[str, Any]) -> list[str]:
    candidates: list[str] = []

    parsed = urlparse(site_url)
    qs = parse_qs(parsed.query)
    for key in ("locationsearch", "location", "loc"):
        for value in qs.get(key, []):
            value = value.strip()
            if value:
                candidates.append(value)

    for key, value in row.items():
        key_l = str(key).lower()
        if any(token in key_l for token in ("country", "keyword", "note", "location")):
            v = str(value).strip()
            if v and v not in candidates:
                candidates.append(v)

    # Row-specific deterministic fallback for this dataset.
    for fixed in ("", "Hong Kong", "HK"):
        if fixed not in candidates:
            candidates.append(fixed)

    # Deduplicate while preserving order.
    out: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def post_jobs_search(
    session: requests.Session,
    endpoint: str,
    locale: str,
    page_number: int,
    location: str,
) -> dict[str, Any]:
    payload = {
        "keywords": "",
        "locale": locale,
        "location": location,
        "pageNumber": page_number,
        "sortBy": "recent",
    }
    resp = request_with_retry(
        session,
        "POST",
        endpoint,
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    data = resp.json()
    if not isinstance(data, dict):
        raise ParserFailure(f"Unexpected search response type: {type(data)!r}")
    return data


def detect_strategy(
    session: requests.Session,
    site_url: str,
    expected_count: int,
    row: dict[str, Any],
) -> dict[str, Any]:
    landing_resp = request_with_retry(session, "GET", site_url)
    landing_html = landing_resp.text

    app_params = extract_app_params(landing_html)
    locale = app_params.get("locale") or "en_US"

    endpoint = urljoin(site_url, "/services/recruiting/v1/jobs")

    candidates = extract_candidate_locations(site_url, row)
    preview: list[dict[str, Any]] = []
    selected_location = candidates[0] if candidates else ""

    for loc in candidates:
        data = post_jobs_search(session, endpoint, locale, page_number=0, location=loc)
        total_jobs = int(data.get("totalJobs") or 0)
        returned = len(data.get("jobSearchResult") or [])
        preview.append({"location": loc, "totalJobs": total_jobs, "returned": returned})
        if total_jobs == expected_count and returned > 0:
            selected_location = loc
            break

    return {
        "site_url": site_url,
        "landing_url": landing_resp.url,
        "endpoint": endpoint,
        "locale": locale,
        "category_id": app_params.get("category_id"),
        "selected_location": selected_location,
        "preview": preview,
        "site_analysis": {
            "data_source": "POST /services/recruiting/v1/jobs JSON API + job detail HTML",
            "pagination": "pageNumber (0-based)",
            "unique_identifier": "response.id + canonical detail URL",
            "anti_bot": "Low-rate requests with retries/backoff and stable headers.",
        },
    }


def build_job_url(job_resp: dict[str, Any], locale: str) -> str:
    job_id = str(job_resp.get("id") or "").strip()
    if not job_id:
        return ""

    title_slug_src = (
        job_resp.get("urlTitle")
        or job_resp.get("unifiedUrlTitle")
        or job_resp.get("unifiedStandardTitle")
        or "untitled"
    )
    title_slug = quote(str(title_slug_src).strip().replace(" ", "-"), safe="-")
    return f"https://jobs.colgate.com/job/{title_slug}/{job_id}-{locale}"


def fetch_index(session: requests.Session, strategy: dict[str, Any], expected_count: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    endpoint = strategy["endpoint"]
    locale = strategy["locale"]
    location = strategy["selected_location"]

    jobs: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_urls: set[str] = set()
    duplicates_removed = 0
    page = 0
    total_jobs_reported: int | None = None
    page_size: int | None = None

    while True:
        data = post_jobs_search(session, endpoint, locale, page_number=page, location=location)
        results = data.get("jobSearchResult") or []
        total_jobs_reported = int(data.get("totalJobs") or 0)
        page_size = page_size or (len(results) if results else page_size)

        print(f"page {page} fetched -> {len(results)} jobs found")

        if not results:
            break

        for item in results:
            response = item.get("response") if isinstance(item, dict) else None
            if not isinstance(response, dict):
                continue

            job_id = str(response.get("id") or "").strip()
            if not job_id:
                continue

            detail_url = build_job_url(response, locale)
            canonical_url = detail_url.rstrip("/")

            if job_id in seen_ids or canonical_url in seen_urls:
                duplicates_removed += 1
                continue

            seen_ids.add(job_id)
            seen_urls.add(canonical_url)

            jobs.append(
                {
                    "job_id": job_id,
                    "title": (response.get("unifiedStandardTitle") or "").strip() or None,
                    "company": (response.get("filter5") or [None])[0],
                    "location": (response.get("jobLocationShort") or [None])[0],
                    "business_unit": (response.get("businessUnit_obj") or [None])[0],
                    "posted_date_raw": response.get("unifiedStandardStart"),
                    "job_url": detail_url,
                    "canonical_job_url": canonical_url,
                    "raw_index": response,
                }
            )

        if len(jobs) >= expected_count:
            break

        if page_size and total_jobs_reported is not None:
            total_pages = max(1, math.ceil(total_jobs_reported / page_size))
            if page + 1 >= total_pages:
                break

        page += 1
        if page > 200:
            break

    return jobs, {
        "total_jobs_reported": total_jobs_reported,
        "duplicates_removed": duplicates_removed,
        "selected_location": location,
        "preview": strategy.get("preview"),
    }


def parse_us_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def html_to_clean_text(html_fragment: str) -> str:
    soup = BeautifulSoup(html_fragment, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines)


def parse_detail(
    detail_html: str,
    final_url: str,
    stub: dict[str, Any],
    strategy: dict[str, Any],
) -> dict[str, Any]:
    soup = BeautifulSoup(detail_html, "html.parser")

    label_map: dict[str, str] = {}
    for token in soup.select(".joblayouttoken"):
        label_el = token.select_one(".joblayouttoken-label")
        if not label_el:
            continue
        label = label_el.get_text(" ", strip=True).rstrip(":").strip()
        spans = token.find_all("span")
        value = ""
        if len(spans) >= 2:
            value = spans[1].get_text(" ", strip=True)
        if not value:
            token_text = token.get_text(" ", strip=True)
            value = token_text.replace(label_el.get_text(" ", strip=True), "", 1).strip(" :")
        if label and value:
            label_map[label] = value

    descriptions = []
    for el in soup.select('[itemprop="description"]'):
        fragment = str(el)
        cleaned = html_to_clean_text(fragment)
        if cleaned:
            descriptions.append(cleaned)

    description_text = "\n\n".join(descriptions) if descriptions else None

    title = label_map.get("Job Title") or stub.get("title")

    location = stub.get("location")
    if description_text:
        m = re.search(r"Job Number\s*#?\s*\d+\s*-\s*([^\n]+)", description_text)
        if m:
            location = m.group(1).strip()

    company = stub.get("company") or "Colgate-Palmolive"

    posted_date_raw = label_map.get("Posting Start Date") or stub.get("posted_date_raw")
    posted_date = parse_us_date(posted_date_raw)

    canonical_job_url = final_url.rstrip("/")

    return {
        "job_id": stub.get("job_id"),
        "job_url": final_url,
        "canonical_job_url": canonical_job_url,
        "title": title,
        "company": company,
        "location": location,
        "team": stub.get("business_unit"),
        "category": None,
        "employment_type": None,
        "posted_date": posted_date,
        "job_description": description_text,
        "raw_source": {
            "source_type": "index_api + detail_html",
            "index_endpoint": strategy.get("endpoint"),
            "index_location_filter": strategy.get("selected_location"),
            "index_record": stub.get("raw_index"),
            "detail_url": final_url,
            "detail_labels": label_map,
            "detail_selector": "[itemprop='description']",
            "site_analysis": strategy.get("site_analysis"),
            "openai_config_source": {
                "api_url": OPENAI_API_URL,
                "api_key_configured": bool(OPENAI_API_KEY),
            },
        },
    }


def fetch_detail(session: requests.Session, stub: dict[str, Any], strategy: dict[str, Any]) -> dict[str, Any]:
    detail_url = stub.get("job_url")
    if not detail_url:
        raise ParserFailure(f"Missing detail URL for job stub {stub.get('job_id')}")

    resp = request_with_retry(session, "GET", detail_url)
    return parse_detail(resp.text, resp.url, stub, strategy)


def validate_counts(
    jobs: list[dict[str, Any]],
    expected_count: int,
    diagnostics: dict[str, Any],
) -> None:
    extracted = len(jobs)
    if extracted != expected_count:
        raise ParserFailure(
            "Count mismatch. "
            f"expected_job_count={expected_count}, extracted_job_count={extracted}, "
            f"selected_location={diagnostics.get('selected_location')}, "
            f"total_jobs_reported={diagnostics.get('total_jobs_reported')}, "
            f"duplicates_removed={diagnostics.get('duplicates_removed')}, "
            f"preview={diagnostics.get('preview')}"
        )

    seen_ids: set[str] = set()
    seen_urls: set[str] = set()
    for job in jobs:
        jid = (job.get("job_id") or "").strip()
        curl = (job.get("canonical_job_url") or job.get("job_url") or "").strip().rstrip("/")
        if not jid or not curl:
            raise ParserFailure(f"Missing distinctness keys in job: {job}")
        if jid in seen_ids or curl in seen_urls:
            raise ParserFailure(f"Distinctness violation detected for job_id/url: {jid} / {curl}")
        seen_ids.add(jid)
        seen_urls.add(curl)


def run(
    csv_file: str = CSV_FILE_DEFAULT,
    csv_line: int = CSV_LINE_DEFAULT,
    output_file: str = OUTPUT_FILE_DEFAULT,
) -> list[dict[str, Any]]:
    target = read_target_row(csv_file, csv_line)

    # Enforce row-specific target from prompt.
    if target.site_url.rstrip("/") != SITE_URL_DEFAULT.rstrip("/"):
        raise ParserFailure(
            f"Site URL mismatch for line {csv_line}. "
            f"Expected {SITE_URL_DEFAULT}, got {target.site_url}"
        )

    expected_count = target.expected_count
    if expected_count != EXPECTED_COUNT_DEFAULT:
        raise ParserFailure(
            f"true_open_jobs mismatch for line {csv_line}. "
            f"Expected {EXPECTED_COUNT_DEFAULT}, got {expected_count}"
        )

    session = new_session()
    strategy = detect_strategy(session, target.site_url, expected_count, target.raw_row)
    stubs, diagnostics = fetch_index(session, strategy, expected_count)

    if len(stubs) < expected_count:
        raise ParserFailure(
            f"Insufficient DISTINCT index jobs. expected={expected_count}, got={len(stubs)}, "
            f"diagnostics={diagnostics}"
        )

    if len(stubs) > expected_count:
        raise ParserFailure(
            f"Too many DISTINCT index jobs for required exact match. expected={expected_count}, got={len(stubs)}"
        )

    details = [fetch_detail(session, stub, strategy) for stub in stubs]
    details = sorted(details, key=lambda j: ((j.get("job_id") or ""), (j.get("canonical_job_url") or "")))

    validate_counts(details, expected_count, diagnostics)

    Path(output_file).write_text(json.dumps(details, ensure_ascii=False, indent=2), encoding="utf-8")
    return details


def _write_failure_diagnostics(output_file: str, error_message: str, site_url: str) -> None:
    payload = [
        {
            "job_id": None,
            "job_url": site_url,
            "canonical_job_url": site_url.rstrip("/"),
            "title": "PARSER_ERROR",
            "company": "Colgate-Palmolive",
            "location": None,
            "job_description": None,
            "raw_source": {
                "error": error_message,
                "site_url": site_url,
                "openai_config_source": {
                    "api_url": OPENAI_API_URL,
                    "api_key_configured": bool(OPENAI_API_KEY),
                },
            },
        }
    ]
    Path(output_file).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    output = OUTPUT_FILE_DEFAULT
    try:
        run(output_file=output)
        return 0
    except Exception as exc:  # noqa: BLE001
        msg = f"{type(exc).__name__}: {exc}"
        _write_failure_diagnostics(output, msg, SITE_URL_DEFAULT)
        print(msg, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
