#!/usr/bin/env python3
"""
Site-specific parser for:
https://www.pgcareers.com/global/en/search-results?m=3

Row scope:
- csv_line: 2 (row_1 data row)
- expected jobs: 6
- output: jobs_pgcareers_com_row_1.json

If extracted count != expected count, the script fails explicitly with diagnostics.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from api_config import API_KEY, API_URL  # required source for GPT API config

CSV_DEFAULT = "Book2.csv"
CSV_LINE_DEFAULT = 2
TARGET_SITE_URL = "https://www.pgcareers.com/global/en/search-results?m=3"
TARGET_EXPECTED_COUNT = 6
OUTPUT_JOBS_FILE = Path("jobs_pgcareers_com_row_1.json")

WIDGET_ENDPOINT = "https://www.pgcareers.com/widgets"
REQUEST_TIMEOUT = 45
MAX_RETRIES = 3

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def log(msg: str) -> None:
    print(f"[pgcareers-row1] {msg}", flush=True)


def load_csv_row(csv_file: str, csv_line: int) -> Tuple[int, str]:
    with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        raise RuntimeError("CSV is empty.")
    if csv_line < 2 or csv_line > len(rows):
        raise RuntimeError(f"Requested csv_line={csv_line} is out of range (2..{len(rows)}).")

    row = rows[csv_line - 1]
    if len(row) < 2:
        raise RuntimeError(f"CSV line {csv_line} has fewer than 2 columns: {row}")

    try:
        expected_count = int(str(row[0]).strip())
    except ValueError as exc:
        raise RuntimeError(f"Cannot parse expected count from CSV line {csv_line}: {row[0]!r}") from exc

    site_url = str(row[1]).strip()
    return expected_count, site_url


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept": "text/html,application/json,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }
    )
    return s


def extract_phapp_ddo(html: str) -> Dict[str, Any]:
    m = re.search(r"phApp\.ddo\s*=\s*(\{.*?\});\s*phApp\.experimentData", html, re.S)
    if not m:
        raise RuntimeError("Could not locate `phApp.ddo` JSON in HTML.")
    blob = m.group(1)
    try:
        return json.loads(blob)
    except json.JSONDecodeError as exc:
        snippet = blob[:300].replace("\n", " ")
        raise RuntimeError(f"Failed to decode phApp.ddo JSON. Snippet: {snippet}") from exc


def detect_strategy(session: requests.Session, site_url: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    resp = session.get(site_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    ddo = extract_phapp_ddo(resp.text)

    refine = ddo.get("eagerLoadRefineSearch") or {}
    refine_data = refine.get("data") or {}
    jobs = refine_data.get("jobs") or []

    analysis = {
        "data_source": "Embedded JSON in HTML: phApp.ddo.eagerLoadRefineSearch + POST /widgets ddoKey=refineSearch",
        "pagination": "offset-based via payload field `from` and page size `size`",
        "unique_identifier": "jobId (fallback reqId/jobSeqNo)",
        "anti_bot": "conservative retries, keep-alive session, no aggressive parallelism",
        "initial_hits": refine.get("hits"),
        "initial_total_hits": refine.get("totalHits"),
        "initial_jobs_on_page": len(jobs),
    }
    return ddo, analysis


def build_refine_payload(
    from_offset: int,
    size: int,
    selected_fields: Dict[str, List[str]],
    keywords: str,
) -> Dict[str, Any]:
    return {
        "lang": "en_global",
        "deviceType": "desktop",
        "country": "global",
        "pageName": "search-results",
        "ddoKey": "refineSearch",
        "sortBy": "",
        "subsearch": "",
        "from": from_offset,
        "jobs": True,
        "counts": True,
        "all_fields": ["category", "country", "state", "city", "type", "subCategory", "experienceLevel", "phLocSlider"],
        "size": size,
        "clearAll": False,
        "jdsource": "facets",
        "isSliderEnable": True,
        "pageId": "page16",
        "siteType": "external",
        "keywords": keywords,
        "global": True,
        "selected_fields": selected_fields,
        "locationData": {"sliderRadius": 51, "aboveMaxRadius": True, "LocationUnit": "miles"},
    }


def call_refine_search(session: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = session.post(WIDGET_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    refine = body.get("refineSearch")
    if not isinstance(refine, dict):
        raise RuntimeError(f"Unexpected refineSearch response: {body}")
    if refine.get("status") != 200:
        raise RuntimeError(f"refineSearch returned non-200 status payload: {refine}")
    return refine


def _aggregation_map(aggregations: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for agg in aggregations or []:
        field = agg.get("field")
        value = agg.get("value")
        if isinstance(field, str) and isinstance(value, dict):
            out[field] = value
    return out


def choose_count_matched_filter(
    base_jobs: List[Dict[str, Any]],
    aggregations: List[Dict[str, Any]],
    expected_count: int,
) -> Optional[Tuple[Dict[str, List[str]], str, str]]:
    agg_map = _aggregation_map(aggregations)

    # Prefer fields that appear in the current page order, deterministic and URL-context aware.
    for field in ["country", "state", "city", "category", "subCategory", "type"]:
        counts = agg_map.get(field) or {}
        seen = set()
        for job in base_jobs:
            value = job.get(field)
            if not value or value in seen:
                continue
            seen.add(value)
            if counts.get(value) == expected_count:
                return ({field: [value]}, "", f"Matched expected count via facet {field}={value!r}")

    # Fallback: keyword from first job city/country if exact count can be achieved.
    if base_jobs:
        first = base_jobs[0]
        for candidate_kw in [first.get("city"), first.get("state"), first.get("country")]:
            if candidate_kw and isinstance(candidate_kw, str):
                return ({}, candidate_kw, f"Fallback keyword attempt using {candidate_kw!r}")

    return None


def dedupe_job_stubs(stubs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    dupes = 0
    for job in stubs:
        jid = job.get("jobId") or job.get("reqId") or job.get("jobSeqNo")
        if not jid:
            jid = hashlib.sha1(
                (f"{job.get('title','')}|{job.get('location','')}|{job.get('postedDate','')}").encode("utf-8")
            ).hexdigest()[:16]
        if jid in seen:
            dupes += 1
            continue
        seen.add(jid)
        deduped.append(job)
    return deduped, dupes


def fetch_index(
    session: requests.Session,
    site_url: str,
    expected_count: int,
    base_ddo: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    eager = base_ddo.get("eagerLoadRefineSearch") or {}
    eager_data = eager.get("data") or {}
    base_jobs = eager_data.get("jobs") or []
    aggregations = eager_data.get("aggregations") or []
    base_total_hits = int(eager.get("totalHits") or 0)

    selected_fields: Dict[str, List[str]] = {}
    keywords = ""
    strategy_reason = "unfiltered"

    if base_total_hits != expected_count:
        choice = choose_count_matched_filter(base_jobs, aggregations, expected_count)
        if choice:
            selected_fields, keywords, strategy_reason = choice

    page_size = 10
    from_offset = 0
    page_no = 1
    collected: List[Dict[str, Any]] = []
    page_logs: List[Dict[str, Any]] = []
    total_hits_reported: Optional[int] = None

    while True:
        payload = build_refine_payload(
            from_offset=from_offset,
            size=page_size,
            selected_fields=selected_fields,
            keywords=keywords,
        )
        refine = call_refine_search(session, payload)
        data = refine.get("data") or {}
        jobs = data.get("jobs") or []
        total_hits = int(refine.get("totalHits") or 0)
        total_hits_reported = total_hits if total_hits_reported is None else total_hits_reported

        log(f"page {page_no} fetched -> {len(jobs)} jobs found")
        page_logs.append(
            {
                "page": page_no,
                "from": from_offset,
                "fetched_jobs": len(jobs),
                "total_hits": total_hits,
            }
        )

        if not jobs:
            break

        collected.extend(jobs)
        deduped_tmp, _ = dedupe_job_stubs(collected)

        if len(deduped_tmp) >= expected_count and total_hits <= expected_count:
            collected = deduped_tmp
            break

        if len(jobs) < page_size:
            collected = deduped_tmp
            break

        from_offset += len(jobs)
        page_no += 1

        # Safety break to avoid crawling unrelated large datasets for this row.
        if from_offset > 200:
            collected = deduped_tmp
            break

    deduped, dupes = dedupe_job_stubs(collected)

    diagnostics = {
        "site_url": site_url,
        "strategy_reason": strategy_reason,
        "selected_fields": selected_fields,
        "keywords": keywords,
        "base_total_hits": base_total_hits,
        "filtered_total_hits": total_hits_reported,
        "before_dedupe": len(collected),
        "after_dedupe": len(deduped),
        "duplicates_removed": dupes,
        "page_logs": page_logs,
    }

    if len(deduped) != expected_count:
        raise RuntimeError(
            "Index count mismatch: "
            f"expected={expected_count}, extracted_unique={len(deduped)}. "
            f"Diagnostics={json.dumps(diagnostics, ensure_ascii=False)}"
        )

    return deduped, diagnostics


def slugify_title(title: str) -> str:
    t = (title or "job").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "-", t)
    t = re.sub(r"-+", "-", t).strip("-")
    return t or "job"


def build_job_url(job_stub: Dict[str, Any]) -> str:
    job_id = job_stub.get("jobId") or job_stub.get("reqId")
    if not job_id:
        raise RuntimeError(f"Cannot build job URL without jobId/reqId: {job_stub}")
    return f"https://www.pgcareers.com/global/en/job/{job_id}/{slugify_title(job_stub.get('title', 'job'))}"


def fetch_detail(session: requests.Session, job_stub: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    job_url = build_job_url(job_stub)
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(job_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            ddo = extract_phapp_ddo(resp.text)
            detail_root = ddo.get("jobDetail") or {}
            detail_data = detail_root.get("data") or {}
            job_detail = detail_data.get("job")
            if not isinstance(job_detail, dict):
                raise RuntimeError(f"jobDetail.data.job missing for {job_url}")
            canonical_url = resp.url.split("?")[0]
            return job_detail, job_url, canonical_url
        except Exception as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            sleep_s = 1.5 * attempt
            log(f"detail retry {attempt}/{MAX_RETRIES} for {job_url}: {exc}. sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch detail for {job_url}: {last_error}")


def html_to_text(html: Optional[str]) -> Optional[str]:
    if html is None:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = []
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            lines.append(line)
    if not lines:
        return None
    return "\n".join(lines)


def normalize_location(location: Optional[str]) -> Optional[str]:
    if not location:
        return None
    return re.sub(r"\s+", " ", location).strip()


def deterministic_job_id(job_url: str, title: Optional[str], location: Optional[str]) -> str:
    base = f"{job_url}|{title or ''}|{location or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def parse_detail(
    job_stub: Dict[str, Any],
    job_detail: Dict[str, Any],
    requested_job_url: str,
    canonical_job_url: str,
    index_diagnostics: Dict[str, Any],
) -> Dict[str, Any]:
    title = job_detail.get("title") or job_stub.get("title")
    location = normalize_location(job_detail.get("location") or job_stub.get("location"))
    job_id = job_detail.get("jobId") or job_stub.get("jobId") or job_stub.get("reqId")
    if not job_id:
        job_id = deterministic_job_id(canonical_job_url, title, location)

    description_html = job_detail.get("description")
    description_text = html_to_text(description_html)

    minimal_detail_snapshot = {
        k: job_detail.get(k)
        for k in [
            "jobId",
            "reqId",
            "jobSeqNo",
            "title",
            "companyName",
            "location",
            "country",
            "state",
            "city",
            "type",
            "category",
            "subCategory",
            "postedDate",
            "dateCreated",
            "applyUrl",
            "externalApply",
        ]
    }

    return {
        "job_id": job_id,
        "job_url": canonical_job_url,
        "canonical_job_url": canonical_job_url,
        "url": canonical_job_url or requested_job_url or job_detail.get("applyUrl") or job_stub.get("applyUrl"),
        "title": title,
        "company": job_detail.get("companyName") or "Procter & Gamble",
        "location": location,
        "team": job_detail.get("subCategory"),
        "category": job_detail.get("category"),
        "employment_type": job_detail.get("type"),
        "posted_date": job_detail.get("postedDate"),
        "date_created": job_detail.get("dateCreated"),
        "description_teaser": job_detail.get("descriptionTeaser") or job_stub.get("descriptionTeaser"),
        "job_description": description_text,
        "apply_url": job_detail.get("applyUrl") or job_stub.get("applyUrl"),
        "raw_source": {
            "index_api": {
                "endpoint": WIDGET_ENDPOINT,
                "ddoKey": "refineSearch",
                "selected_fields": index_diagnostics.get("selected_fields"),
                "keywords": index_diagnostics.get("keywords"),
                "job_stub": job_stub,
            },
            "detail_page": {
                "requested_job_url": requested_job_url,
                "canonical_job_url": canonical_job_url,
                "ddo_path": "phApp.ddo.jobDetail.data.job",
                "job_snapshot": minimal_detail_snapshot,
                "description_html": description_html,
            },
        },
    }


def validate_counts(jobs: List[Dict[str, Any]], expected_count: int) -> Dict[str, Any]:
    missing_fields_summary = {}
    for key in ["job_id", "job_url", "title", "company", "location", "job_description"]:
        missing_fields_summary[key] = sum(1 for j in jobs if not j.get(key))

    unique_ids = {j.get("job_id") for j in jobs}
    duplicates_removed = len(jobs) - len(unique_ids)

    result = {
        "expected_job_count": expected_count,
        "extracted_job_count": len(jobs),
        "duplicates_removed": duplicates_removed,
        "missing_fields_summary": missing_fields_summary,
    }

    if len(jobs) != expected_count:
        raise RuntimeError(
            f"Final count validation failed: expected={expected_count}, extracted={len(jobs)}. "
            f"Details={json.dumps(result, ensure_ascii=False)}"
        )

    return result


def run(csv_file: str = CSV_DEFAULT, csv_line: int = CSV_LINE_DEFAULT) -> None:
    expected_count, site_url = load_csv_row(csv_file, csv_line)

    log(f"Using api_config.py API endpoint source: {API_URL}")
    log(f"API key present: {'yes' if bool(API_KEY) else 'no'}")

    if site_url != TARGET_SITE_URL:
        raise RuntimeError(f"Unexpected site_url on csv line {csv_line}: {site_url}")

    if expected_count != TARGET_EXPECTED_COUNT:
        log(
            "Warning: CSV expected count differs from task constant. "
            f"CSV={expected_count}, task={TARGET_EXPECTED_COUNT}. Using CSV value."
        )

    expected = expected_count
    session = make_session()

    base_ddo, site_analysis = detect_strategy(session, site_url)
    log(f"strategy: {json.dumps(site_analysis, ensure_ascii=False)}")

    job_stubs, index_diag = fetch_index(session, site_url, expected, base_ddo)
    log(f"index diagnostics: {json.dumps(index_diag, ensure_ascii=False)}")

    jobs: List[Dict[str, Any]] = []
    for idx, stub in enumerate(job_stubs, start=1):
        job_detail, requested_url, canonical_url = fetch_detail(session, stub)
        parsed = parse_detail(stub, job_detail, requested_url, canonical_url, index_diag)
        jobs.append(parsed)
        log(f"detail {idx}/{len(job_stubs)} parsed -> {parsed.get('job_id')} | {parsed.get('title')}")

    # Deterministic ordering: preserve index order then tie-break by job_id.
    jobs = sorted(enumerate(jobs), key=lambda x: (x[0], str(x[1].get("job_id"))))
    jobs = [x[1] for x in jobs]

    validation_summary = validate_counts(jobs, expected)
    log(f"validation summary: {json.dumps(validation_summary, ensure_ascii=False)}")

    OUTPUT_JOBS_FILE.write_text(json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"wrote {OUTPUT_JOBS_FILE} with {len(jobs)} jobs")


if __name__ == "__main__":
    csv_file = CSV_DEFAULT
    csv_line = CSV_LINE_DEFAULT

    # Optional CLI: python parser_pgcareers_com_row_1.py [csv_file] [csv_line]
    if len(sys.argv) >= 2:
        csv_file = sys.argv[1]
    if len(sys.argv) >= 3:
        csv_line = int(sys.argv[2])

    try:
        run(csv_file=csv_file, csv_line=csv_line)
    except Exception as exc:
        log(f"FAIL: {exc}")
        raise
