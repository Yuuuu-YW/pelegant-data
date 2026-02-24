#!/usr/bin/env python3
"""
Row-scoped parser for https://jobs.redbull.com/int-en (Book2.csv data row 10).
Writes exactly one row-scoped output file: jobs_jobs_redbull_com_row_10.json
"""

from __future__ import annotations

import csv
import hashlib
import html as html_lib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

try:
    from api_config import API_KEY, API_URL
except Exception:
    API_KEY = ""
    API_URL = ""


CSV_FILE = "Book2.csv"
TARGET_DATA_ROW_NUMBER = 10  # 1-based among data rows (header excluded)
TARGET_SITE_URL = "https://jobs.redbull.com/int-en"
OUTPUT_FILE = "jobs_jobs_redbull_com_row_10.json"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)


@dataclass
class TargetRow:
    row_number: int
    true_open_jobs: int
    site_url: str
    country: Optional[str] = None
    keyword: Optional[str] = None
    notes: Optional[str] = None


class ParserError(RuntimeError):
    pass


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}" if path else f"{parsed.scheme}://{parsed.netloc}"


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; redbull-row10-parser/1.0)",
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        }
    )
    return session


def request_json(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(0.8 * attempt)
    raise ParserError(f"GET JSON failed: {url} params={params} error={last_error}")


def request_text(session: requests.Session, url: str) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(0.8 * attempt)
    raise ParserError(f"GET HTML failed: {url} error={last_error}")


def extract_next_data(html: str) -> Dict[str, Any]:
    match = NEXT_DATA_RE.search(html)
    if not match:
        raise ParserError("__NEXT_DATA__ not found")
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise ParserError(f"Failed to decode __NEXT_DATA__: {exc}") from exc


def clean_html_to_text(raw_html: Optional[str]) -> Optional[str]:
    if not raw_html:
        return None
    text = raw_html
    text = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", text)
    text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\\s*>", "\n", text)
    text = re.sub(r"(?i)<li\\b[^>]*>", "- ", text)
    text = re.sub(r"(?i)</li\\s*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = "\n".join(line for line in text.splitlines() if line)
    return text.strip() or None


def read_target_row(csv_file: str, data_row_number: int) -> TargetRow:
    csv_path = Path(csv_file)
    if not csv_path.exists():
        raise ParserError(f"CSV not found: {csv_file}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            raise ParserError("CSV header missing")

        for idx, row in enumerate(reader, start=1):
            if idx != data_row_number:
                continue
            if len(row) < 2:
                raise ParserError(f"Row {data_row_number} malformed: {row}")

            true_open_jobs = int(str(row[0]).strip())
            site_url = str(row[1]).strip().strip('"')

            country = str(row[2]).strip() if len(row) > 2 and str(row[2]).strip() else None
            keyword = str(row[3]).strip() if len(row) > 3 and str(row[3]).strip() else None
            notes = str(row[4]).strip() if len(row) > 4 and str(row[4]).strip() else None

            return TargetRow(
                row_number=data_row_number,
                true_open_jobs=true_open_jobs,
                site_url=site_url,
                country=country,
                keyword=keyword,
                notes=notes,
            )

    raise ParserError(f"Data row {data_row_number} not found in {csv_file}")


def detect_strategy(session: requests.Session, site_url: str) -> Dict[str, Any]:
    html = request_text(session, site_url)
    next_data = extract_next_data(html)

    page_props = (
        next_data.get("props", {})
        .get("pageProps", {})
    )
    app_state = page_props.get("appState", {})

    parsed = urlparse(site_url)
    locale_path = parsed.path.strip("/") or "int-en"
    base_host = f"{parsed.scheme}://{parsed.netloc}"

    strategy = {
        "source": "nextjs_api_and_next_data",
        "search_endpoint": f"{base_host}/api/search",
        "locations_endpoint": f"{base_host}/api/locations",
        "base_host": base_host,
        "locale_path": locale_path,
        "language": app_state.get("language") or "en",
        "country": app_state.get("country") or "int",
        "anti_bot": "polite retries, low request volume, default browser-like headers",
        "gpt_config_source": "api_config.py",
        "gpt_api_url_present": bool(API_URL),
        "gpt_api_key_present": bool(API_KEY),
    }
    return strategy


def _job_stub_key(stub: Dict[str, Any]) -> Tuple[str, str]:
    return str(stub.get("id") or ""), str(stub.get("slug") or "")


def fetch_index(
    session: requests.Session,
    strategy: Dict[str, Any],
    expected_count: int,
    target_row: TargetRow,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    base_params = {
        "locale": strategy["language"],
        "country": strategy["country"],
        "pageSize": max(expected_count, 10),
    }

    diagnostics: Dict[str, Any] = {
        "expected_count": expected_count,
        "pagination": "single call with pageSize>=expected_count (site uses cumulative pageSize load-more)",
        "attempts": [],
    }

    # Attempt 1: direct search (no location filter)
    initial = request_json(session, strategy["search_endpoint"], params=base_params)
    init_result_size = int(initial.get("resultSize") or 0)
    diagnostics["attempts"].append(
        {
            "type": "unfiltered",
            "params": dict(base_params),
            "resultSize": init_result_size,
            "count": int(initial.get("count") or 0),
        }
    )

    if init_result_size == expected_count:
        stubs = initial.get("jobs") or []
        unique = []
        seen = set()
        for s in stubs:
            key = _job_stub_key(s)
            if key in seen:
                continue
            seen.add(key)
            unique.append(s)
        if len(unique) >= expected_count:
            diagnostics["selected_filter"] = "none"
            return unique[:expected_count], diagnostics

    # Attempt 2: location narrowing. We prioritize Hong Kong hints because this CSV batch is HK-focused.
    hints = [target_row.country, target_row.keyword, target_row.notes, "hong kong", "hong"]
    hints = [h.strip() for h in hints if h and h.strip()]

    candidates: Dict[str, str] = {}
    for hint in hints:
        locs = request_json(
            session,
            strategy["locations_endpoint"],
            params={
                "searchText": hint,
                "locale": strategy["language"],
                "country": strategy["country"],
            },
        )
        if isinstance(locs, list):
            for item in locs:
                loc_id = str(item.get("id") or "").strip()
                loc_name = str(item.get("name") or "").strip()
                if loc_id:
                    candidates[loc_id] = loc_name

    # deterministic order by numeric location id when possible
    def _sort_loc_key(loc_id: str) -> Tuple[int, str]:
        return (int(loc_id), loc_id) if loc_id.isdigit() else (10**12, loc_id)

    for loc_id in sorted(candidates.keys(), key=_sort_loc_key):
        params = dict(base_params)
        params["locations"] = loc_id
        res = request_json(session, strategy["search_endpoint"], params=params)
        result_size = int(res.get("resultSize") or 0)
        count = int(res.get("count") or 0)

        diagnostics["attempts"].append(
            {
                "type": "location-filter",
                "location_id": loc_id,
                "location_name": candidates.get(loc_id),
                "params": params,
                "resultSize": result_size,
                "count": count,
            }
        )

        if result_size != expected_count:
            continue

        stubs = res.get("jobs") or []
        unique = []
        seen = set()
        for s in stubs:
            key = _job_stub_key(s)
            if key in seen:
                continue
            seen.add(key)
            unique.append(s)
        if len(unique) >= expected_count:
            diagnostics["selected_filter"] = {
                "location_id": loc_id,
                "location_name": candidates.get(loc_id),
            }
            return unique[:expected_count], diagnostics

    raise ParserError(
        "Could not match true_open_jobs exactly from /api/search. "
        f"Expected {expected_count}, observed attempts: {json.dumps(diagnostics, ensure_ascii=False)}"
    )


def fetch_detail(session: requests.Session, strategy: Dict[str, Any], slug: str) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    detail_url = f"{strategy['base_host']}/{strategy['locale_path']}/{slug}".rstrip("/")
    html = request_text(session, detail_url)
    next_data = extract_next_data(html)

    detail_root = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("pageProps", {})
    )
    if not detail_root or not isinstance(detail_root, dict):
        raise ParserError(f"Detail payload missing pageProps for {detail_url}")

    job = detail_root.get("job")
    if not isinstance(job, dict):
        raise ParserError(f"Detail payload missing job for {detail_url}")

    return detail_url, detail_root, job


def _pick_location(components: List[Dict[str, Any]], target_type: str) -> Optional[str]:
    for comp in components or []:
        if str(comp.get("type") or "").upper() == target_type.upper():
            name = comp.get("name")
            if name is not None:
                return str(name)
    return None


def parse_detail(
    job_stub: Dict[str, Any],
    detail_url: str,
    detail_root: Dict[str, Any],
    detail_job: Dict[str, Any],
    strategy: Dict[str, Any],
) -> Dict[str, Any]:
    job_id = str(detail_job.get("id") or job_stub.get("id") or "").strip()
    title = (detail_job.get("title") or job_stub.get("title") or "").strip()
    slug = str(detail_job.get("slug") or job_stub.get("slug") or "").strip()

    if not job_id:
        seed = f"{detail_url}|{title}|{job_stub.get('locationText') or ''}"
        job_id = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]

    metadata = detail_root.get("metadata") or {}
    canonical_meta = metadata.get("canonical") if isinstance(metadata, dict) else None
    if isinstance(canonical_meta, dict) and canonical_meta.get("slug"):
        canonical_locale = canonical_meta.get("locale") or strategy["locale_path"]
        canonical_slug = canonical_meta.get("slug")
        canonical_job_url = canonicalize_url(f"{strategy['base_host']}/{canonical_locale}/{canonical_slug}")
    else:
        canonical_job_url = canonicalize_url(detail_url)

    organization = detail_job.get("organization") if isinstance(detail_job.get("organization"), dict) else {}
    company = organization.get("name") or "Red Bull"

    locations = detail_job.get("locations") if isinstance(detail_job.get("locations"), list) else []
    city = _pick_location(locations, "CITY")
    state = _pick_location(locations, "STATE")
    country = _pick_location(locations, "COUNTRY")
    region = _pick_location(locations, "REGION")

    location_text = job_stub.get("locationText")
    if not location_text:
        location_text = ", ".join([x for x in [city, state, country] if x]) or None

    function = detail_job.get("function") if isinstance(detail_job.get("function"), dict) else {}
    team = function.get("name")

    job_description_html = detail_job.get("description")
    job_description = clean_html_to_text(job_description_html)

    result = {
        "job_id": job_id,
        "title": title or None,
        "url": canonicalize_url(detail_url),
        "job_url": canonicalize_url(detail_url),
        "canonical_job_url": canonical_job_url,
        "company": company,
        "location": location_text,
        "city": city,
        "state": state,
        "country": country,
        "region": region,
        "team": team,
        "employment_type": detail_job.get("employmentType"),
        "job_type": detail_job.get("jobType"),
        "posted_date": detail_job.get("createdAt"),
        "slug": slug or None,
        "job_description": job_description,
        "raw_source": {
            "source_type": "redbull_next_data",
            "search_stub": {
                "id": job_stub.get("id"),
                "slug": job_stub.get("slug"),
                "title": job_stub.get("title"),
                "locationText": job_stub.get("locationText"),
            },
            "detail_url": detail_url,
            "detail_payload_keys": sorted(detail_job.keys()),
            "job_description_html": job_description_html,
        },
    }
    return result


def validate_counts(jobs: List[Dict[str, Any]], expected_count: int) -> Dict[str, Any]:
    unique_jobs: List[Dict[str, Any]] = []
    seen_ids = set()
    seen_urls = set()
    duplicates_removed = 0

    for job in jobs:
        job_id = str(job.get("job_id") or "").strip()
        canonical = str(job.get("canonical_job_url") or job.get("url") or job.get("job_url") or "").strip()
        if not job_id or not canonical:
            continue
        if job_id in seen_ids or canonical in seen_urls:
            duplicates_removed += 1
            continue
        seen_ids.add(job_id)
        seen_urls.add(canonical)
        unique_jobs.append(job)

    unique_jobs.sort(key=lambda j: (str(j.get("job_id") or ""), str(j.get("canonical_job_url") or j.get("job_url") or "")))

    validation = {
        "expected_job_count": expected_count,
        "extracted_job_count": len(unique_jobs),
        "duplicates_removed": duplicates_removed,
        "distinct_job_ids": len({str(j.get("job_id")) for j in unique_jobs}),
        "distinct_canonical_urls": len({str(j.get("canonical_job_url") or j.get("job_url")) for j in unique_jobs}),
    }

    if validation["extracted_job_count"] != expected_count:
        raise ParserError(
            "Validation failed: extracted count does not match expected count. "
            f"Details: {json.dumps(validation, ensure_ascii=False)}"
        )

    if validation["distinct_job_ids"] != expected_count or validation["distinct_canonical_urls"] != expected_count:
        raise ParserError(
            "Validation failed: jobs are not distinct by both job_id and canonical URL. "
            f"Details: {json.dumps(validation, ensure_ascii=False)}"
        )

    return {"validation": validation, "jobs": unique_jobs}


def run() -> None:
    target = read_target_row(CSV_FILE, TARGET_DATA_ROW_NUMBER)
    if canonicalize_url(target.site_url) != canonicalize_url(TARGET_SITE_URL):
        raise ParserError(
            f"Unexpected site_url at row {TARGET_DATA_ROW_NUMBER}: {target.site_url} (expected {TARGET_SITE_URL})"
        )

    session = get_session()

    strategy = detect_strategy(session, target.site_url)
    stubs, index_diagnostics = fetch_index(
        session=session,
        strategy=strategy,
        expected_count=target.true_open_jobs,
        target_row=target,
    )

    detailed_jobs: List[Dict[str, Any]] = []
    for stub in stubs:
        slug = str(stub.get("slug") or "").strip()
        if not slug:
            raise ParserError(f"Missing slug in job stub: {stub}")

        detail_url, detail_root, detail_job = fetch_detail(session, strategy, slug)
        parsed = parse_detail(stub, detail_url, detail_root, detail_job, strategy)
        parsed["raw_source"]["index_diagnostics"] = index_diagnostics
        detailed_jobs.append(parsed)

    validated = validate_counts(detailed_jobs, target.true_open_jobs)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(validated["jobs"], f, ensure_ascii=False, indent=2)

    print(
        f"OK: wrote {len(validated['jobs'])} jobs to {OUTPUT_FILE} "
        f"(expected {target.true_open_jobs}, row {TARGET_DATA_ROW_NUMBER})"
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001
        # Keep this row-scoped output file present with explicit diagnostics in case of network/DNS failure.
        diagnostics_payload = [
            {
                "job_id": None,
                "title": "PARSER_ERROR",
                "url": TARGET_SITE_URL,
                "job_url": TARGET_SITE_URL,
                "canonical_job_url": TARGET_SITE_URL,
                "error": str(exc),
            }
        ]
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(diagnostics_payload, f, ensure_ascii=False, indent=2)
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
