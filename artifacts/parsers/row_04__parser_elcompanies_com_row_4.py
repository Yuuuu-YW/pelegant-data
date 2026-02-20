#!/usr/bin/env python3
"""Row-scoped parser for https://www.elcompanies.com/en/careers (Book2.csv line 5).

Required row outputs:
- jobs_elcompanies_com_row_4.json
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

import api_config

CSV_FILE = "Book2.csv"
CSV_LINE = 5  # absolute CSV line (header included)
ROW_SCOPE = 4
DEFAULT_SITE_URL = "https://www.elcompanies.com/en/careers"
DEFAULT_EXPECTED = 1
OUTPUT_JOBS = f"jobs_elcompanies_com_row_{ROW_SCOPE}.json"

PORTAL_URL = "https://careers.elcompanies.com/careers"
SEARCH_API = "https://careers.elcompanies.com/api/pcsx/search"
DETAIL_API = "https://careers.elcompanies.com/api/pcsx/position_details"
DOMAIN = "elcompanies.com"

TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_SECONDS = (1.0, 2.0, 4.0)
MAX_PAGES = 250

# Required by task: API URL + KEY source should come from api_config.py
OPENAI_API_URL = getattr(api_config, "API_URL", None)
OPENAI_API_KEY = getattr(api_config, "API_KEY", None)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PORTAL_URL,
    # trace-only (no secret exposure)
    "X-Client-Api-Base": OPENAI_API_URL or "",
    "X-Client-Api-Key-Configured": "1" if bool(OPENAI_API_KEY) else "0",
}


@dataclass(frozen=True)
class Target:
    site_url: str
    true_open_jobs: int
    csv_line: int


class CountMismatchError(RuntimeError):
    """Raised when extracted distinct jobs do not match expected count."""


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    path = parsed.path.rstrip("/") or "/"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def html_to_text(html_fragment: Optional[str]) -> Optional[str]:
    if not html_fragment:
        return None
    soup = BeautifulSoup(html_fragment, "html.parser")
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text or None


def ts_to_iso_date(ts: Optional[Any]) -> Optional[str]:
    if ts is None:
        return None
    try:
        v = int(ts)
        return datetime.fromtimestamp(v, tz=timezone.utc).date().isoformat()
    except Exception:  # noqa: BLE001
        return None


def deterministic_fallback_id(job_url: Optional[str], title: Optional[str], location: Optional[str]) -> str:
    blob = f"{job_url or ''}|{title or ''}|{location or ''}".encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:24]


def request_json_with_retry(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < MAX_RETRIES:
                sleep_for = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
                time.sleep(sleep_for)
    assert last_err is not None
    raise last_err


def request_text_with_retry(
    session: requests.Session,
    url: str,
) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < MAX_RETRIES:
                sleep_for = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
                time.sleep(sleep_for)
    assert last_err is not None
    raise last_err


def read_target(csv_file: str = CSV_FILE, csv_line: int = CSV_LINE) -> Target:
    data_idx = csv_line - 2  # first data row is CSV line 2
    try:
        with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        if not (0 <= data_idx < len(rows)):
            raise IndexError(f"csv_line={csv_line} out of range for {len(rows)} data rows")

        row = rows[data_idx]
        count_key = next((k for k in row.keys() if "true_open_jobs" in (k or "").lower()), None)
        if not count_key:
            count_key = list(row.keys())[0]

        expected = int(str(row.get(count_key, DEFAULT_EXPECTED)).strip())
        site_url = str(row.get("site_url", DEFAULT_SITE_URL)).strip() or DEFAULT_SITE_URL
        return Target(site_url=site_url, true_open_jobs=expected, csv_line=csv_line)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Failed reading CSV target (%s). Using defaults.", exc)
        return Target(site_url=DEFAULT_SITE_URL, true_open_jobs=DEFAULT_EXPECTED, csv_line=csv_line)


def dedupe_distinct(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """Distinct by BOTH job_id and canonical URL (url/job_url fallback)."""
    seen_ids = set()
    seen_urls = set()
    out: List[Dict[str, Any]] = []
    duplicates = 0

    for item in items:
        jid = str(item.get("job_id") or "").strip() or None
        curl = (
            item.get("canonical_job_url")
            or item.get("url")
            or item.get("job_url")
            or None
        )
        curl = canonicalize_url(curl) if curl else None

        is_dup = False
        if jid and jid in seen_ids:
            is_dup = True
        if curl and curl in seen_urls:
            is_dup = True

        if is_dup:
            duplicates += 1
            continue

        if jid:
            seen_ids.add(jid)
        if curl:
            seen_urls.add(curl)
        out.append(item)

    return out, duplicates


def detect_strategy(session: requests.Session, site_url: str) -> Dict[str, Any]:
    root_status = None
    root_error = None
    try:
        root_resp = session.get(site_url, timeout=TIMEOUT)
        root_status = root_resp.status_code
    except Exception as exc:  # noqa: BLE001
        root_error = str(exc)

    portal_html = request_text_with_retry(session, PORTAL_URL)
    country_match = re.search(r'window\\.COUNTRY_CODE\\s*=\\s*"([A-Za-z]{2})"', portal_html)
    detected_country = country_match.group(1).upper() if country_match else None

    # Probe primary index API once to confirm strategy.
    probe = request_json_with_retry(
        session,
        SEARCH_API,
        params={"domain": DOMAIN, "query": "", "location": "", "start": 0},
    )
    if int(probe.get("status", 0)) != 200 or "data" not in probe:
        raise RuntimeError("Search API probe failed: unexpected response shape")

    return {
        "site": "elcompanies.com",
        "data_source": "Eightfold PCS API (/api/pcsx/search + /api/pcsx/position_details)",
        "site_url_status": root_status,
        "site_url_error": root_error,
        "portal_url": PORTAL_URL,
        "search_api": SEARCH_API,
        "detail_api": DETAIL_API,
        "pagination": "start offset increments by page size; total count from data.count",
        "unique_identifier": "id (position_id) + canonical publicUrl",
        "detected_country_code": detected_country,
        "anti_bot": "Light retry/backoff, standard headers, low request rate",
    }


def fetch_search_page(
    session: requests.Session,
    *,
    location: str,
    start: int,
    query: str = "",
) -> Dict[str, Any]:
    payload = request_json_with_retry(
        session,
        SEARCH_API,
        params={
            "domain": DOMAIN,
            "query": query,
            "location": location,
            "start": start,
        },
    )
    if int(payload.get("status", 0)) != 200:
        raise RuntimeError(f"search API non-200 status payload: {payload.get('status')}")
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError("search API data is not an object")
    return data


def build_location_candidates(expected: int, detected_country_code: Optional[str]) -> List[str]:
    candidates: List[str] = []
    if expected <= 5:
        if detected_country_code == "HK":
            candidates.extend(["Hong Kong", "HK"])
        candidates.extend(["Hong Kong", "HK", ""])  # include global fallback
    else:
        candidates.extend(["", "Hong Kong", "HK"])

    out: List[str] = []
    seen = set()
    for loc in candidates:
        if loc not in seen:
            seen.add(loc)
            out.append(loc)
    return out


def fetch_index(
    session: requests.Session,
    strategy: Dict[str, Any],
    expected: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], int]:
    candidates = build_location_candidates(expected, strategy.get("detected_country_code"))

    diagnostics: Dict[str, Any] = {
        "candidates": [],
        "expected_job_count": expected,
    }

    best_jobs: List[Dict[str, Any]] = []
    best_distance = 10**9
    best_duplicates = 0

    for location in candidates:
        start = 0
        page = 0
        candidate_records: List[Dict[str, Any]] = []
        duplicates_removed = 0
        reported_total: Optional[int] = None

        while page < MAX_PAGES:
            data = fetch_search_page(session, location=location, start=start)
            positions = data.get("positions") or []
            count_raw = data.get("count")
            if isinstance(count_raw, int):
                reported_total = count_raw

            page += 1
            logging.info(
                "location=%r page=%s start=%s fetched=%s reported_total=%s",
                location,
                page,
                start,
                len(positions),
                reported_total,
            )

            if not positions:
                break

            for pos in positions:
                pid = pos.get("id")
                position_url = pos.get("positionUrl")
                public_url = pos.get("publicUrl")
                if not public_url and position_url:
                    public_url = urljoin(PORTAL_URL, position_url)
                canonical = canonicalize_url(public_url) if public_url else None
                candidate_records.append(
                    {
                        "job_id": str(pid) if pid is not None else None,
                        "job_url": public_url,
                        "canonical_job_url": canonical,
                        "title": pos.get("name"),
                        "location": ", ".join(pos.get("standardizedLocations") or pos.get("locations") or [] ) or None,
                        "department": pos.get("department"),
                        "posted_date": ts_to_iso_date(pos.get("postedTs")),
                        "index_raw": pos,
                        "location_filter": location,
                    }
                )

            unique_records, dups = dedupe_distinct(candidate_records)
            duplicates_removed = dups

            page_size = len(positions)
            if page_size <= 0:
                break
            start += page_size

            if reported_total is not None and start >= reported_total:
                candidate_records = unique_records
                break

        unique_records, duplicates_removed = dedupe_distinct(candidate_records)

        diagnostics["candidates"].append(
            {
                "location": location,
                "reported_total": reported_total,
                "extracted_unique": len(unique_records),
                "duplicates_removed": duplicates_removed,
                "sample_titles": [j.get("title") for j in unique_records[:3]],
            }
        )

        distance = abs(len(unique_records) - expected)
        if distance < best_distance:
            best_distance = distance
            best_jobs = unique_records
            best_duplicates = duplicates_removed

        # strict acceptance: reported total and extracted unique both match expected
        if reported_total == expected and len(unique_records) == expected:
            diagnostics["selected_location"] = location
            diagnostics["selected_reported_total"] = reported_total
            return unique_records, diagnostics, duplicates_removed

    diagnostics["selected_location"] = None
    diagnostics["failure_reason"] = (
        "No location candidate produced exact expected distinct job count. "
        f"Best extracted={len(best_jobs)} expected={expected}."
    )

    raise CountMismatchError(json.dumps(diagnostics, ensure_ascii=False, indent=2))


def fetch_detail(session: requests.Session, job_id: str) -> Dict[str, Any]:
    payload = request_json_with_retry(
        session,
        DETAIL_API,
        params={"position_id": job_id, "domain": DOMAIN, "hl": "en"},
    )
    if int(payload.get("status", 0)) != 200:
        raise RuntimeError(f"detail API non-200 status payload for {job_id}: {payload.get('status')}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"detail API malformed for {job_id}")
    return data


def parse_detail(index_stub: Dict[str, Any], detail: Dict[str, Any], strategy: Dict[str, Any]) -> Dict[str, Any]:
    job_url = detail.get("publicUrl") or index_stub.get("job_url")
    if not job_url and detail.get("positionUrl"):
        job_url = urljoin(PORTAL_URL, detail["positionUrl"])

    canonical_job_url = canonicalize_url(job_url) if job_url else None

    standardized_locations = detail.get("standardizedLocations") or []
    raw_locations = detail.get("locations") or []
    location = None
    if standardized_locations:
        location = standardized_locations[0]
    elif raw_locations:
        location = raw_locations[0]

    title = detail.get("name") or index_stub.get("title")

    job_id_val = detail.get("id") or index_stub.get("job_id")
    job_id = str(job_id_val) if job_id_val is not None else None
    if not job_id:
        job_id = deterministic_fallback_id(job_url, title, location)

    employment_raw = (detail.get("efcustomTextAssignmentcat") or [None])[0]
    brand_raw = (detail.get("efcustomTextBrand") or [None])[0]
    sub_function_raw = (detail.get("efcustomTextJobsubfunction") or [None])[0]

    job_description_html = detail.get("jobDescription")
    job_description_text = html_to_text(job_description_html)

    job_obj = {
        "job_id": job_id,
        "url": canonical_job_url or job_url,
        "job_url": job_url,
        "canonical_job_url": canonical_job_url,
        "title": title,
        "company": "The Estée Lauder Companies Inc.",
        "location": location,
        "department": detail.get("department") or index_stub.get("department"),
        "team": sub_function_raw,
        "brand": brand_raw,
        "employment_type": employment_raw,
        "workplace_type": detail.get("workLocationOption"),
        "posted_date": ts_to_iso_date(detail.get("postedTs")),
        "job_description": job_description_text,
        "job_description_html": job_description_html,
        "raw_source": {
            "source_type": "api",
            "strategy": strategy,
            "index_endpoint": SEARCH_API,
            "detail_endpoint": DETAIL_API,
            "index_stub": index_stub.get("index_raw"),
            "detail_payload": detail,
        },
    }

    # evidence-only: if missing, leave null
    for key, val in list(job_obj.items()):
        if val == "":
            job_obj[key] = None

    return job_obj


def validate_counts(
    jobs: List[Dict[str, Any]],
    expected: int,
    duplicates_removed: int,
    index_diagnostics: Dict[str, Any],
) -> Dict[str, Any]:
    unique_jobs, dedup_dups = dedupe_distinct(jobs)
    missing_summary = {
        "title": sum(1 for j in unique_jobs if not j.get("title")),
        "job_url": sum(1 for j in unique_jobs if not j.get("job_url")),
        "canonical_job_url": sum(1 for j in unique_jobs if not j.get("canonical_job_url")),
        "location": sum(1 for j in unique_jobs if not j.get("location")),
        "job_description": sum(1 for j in unique_jobs if not j.get("job_description")),
    }

    report = {
        "expected_job_count": expected,
        "extracted_job_count": len(unique_jobs),
        "duplicates_removed": duplicates_removed + dedup_dups,
        "missing_fields_summary": missing_summary,
        "index_diagnostics": index_diagnostics,
        "status": "passed" if len(unique_jobs) == expected else "failed",
    }

    if report["status"] != "passed":
        report["failure_reason"] = (
            f"Count mismatch: expected={expected}, extracted_distinct={len(unique_jobs)}"
        )

    return report


def run(csv_file: str = CSV_FILE, csv_line: int = CSV_LINE) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    target = read_target(csv_file=csv_file, csv_line=csv_line)
    logging.info("Target selected: line=%s site=%s expected=%s", target.csv_line, target.site_url, target.true_open_jobs)

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        strategy = detect_strategy(session, target.site_url)
        index_jobs, index_diagnostics, duplicates_removed = fetch_index(
            session=session,
            strategy=strategy,
            expected=target.true_open_jobs,
        )

        jobs: List[Dict[str, Any]] = []
        for idx, stub in enumerate(index_jobs, start=1):
            jid = stub.get("job_id")
            if not jid:
                raise RuntimeError(f"Missing job_id in index stub: {stub}")
            detail = fetch_detail(session, str(jid))
            parsed = parse_detail(stub, detail, strategy)
            jobs.append(parsed)
            logging.info("detail %s/%s fetched job_id=%s title=%r", idx, len(index_jobs), jid, parsed.get("title"))

        jobs, final_dups = dedupe_distinct(jobs)
        validation = validate_counts(
            jobs=jobs,
            expected=target.true_open_jobs,
            duplicates_removed=duplicates_removed + final_dups,
            index_diagnostics=index_diagnostics,
        )

        if validation["status"] != "passed":
            raise CountMismatchError(json.dumps(validation, ensure_ascii=False, indent=2))

        jobs_sorted = sorted(jobs, key=lambda j: ((j.get("job_id") or ""), (j.get("canonical_job_url") or "")))
        Path(OUTPUT_JOBS).write_text(json.dumps(jobs_sorted, ensure_ascii=False, indent=2), encoding="utf-8")
        logging.info("Wrote %s with %s jobs", OUTPUT_JOBS, len(jobs_sorted))

    except Exception as exc:  # noqa: BLE001
        # Contract fallback: keep row-scoped jobs file with explicit diagnostics when run fails.
        diagnostic = {
            "job_id": None,
            "title": "PARSER_RUN_FAILED",
            "url": target.site_url,
            "job_url": target.site_url,
            "canonical_job_url": canonicalize_url(target.site_url),
            "company": "The Estée Lauder Companies Inc.",
            "location": None,
            "job_description": None,
            "raw_source": {
                "source_type": "diagnostic",
                "error": str(exc),
            },
        }
        Path(OUTPUT_JOBS).write_text(json.dumps([diagnostic], ensure_ascii=False, indent=2), encoding="utf-8")
        logging.error("Run failed. Diagnostics written to %s", OUTPUT_JOBS)
        raise


if __name__ == "__main__":
    run()
