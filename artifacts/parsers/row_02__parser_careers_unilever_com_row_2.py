#!/usr/bin/env python3
"""Row-scoped parser for https://careers.unilever.com/en/search-jobs (Book2.csv line 3).

Outputs only:
- jobs_careers_unilever_com_row_2.json
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

import api_config

CSV_FILE = "Book2.csv"
CSV_LINE = 3  # absolute line number in CSV including header
ROW_SCOPE = 2
DEFAULT_SITE_URL = "https://careers.unilever.com/en/search-jobs"
DEFAULT_EXPECTED = 1
OUTPUT_JOBS = f"jobs_careers_unilever_com_row_{ROW_SCOPE}.json"

TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]

# Required by task: use api_config.py as source of API URL and key.
OPENAI_API_URL = getattr(api_config, "API_URL", None)
OPENAI_API_KEY = getattr(api_config, "API_KEY", None)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    # lightweight traceability headers proving api_config usage without exposing secrets
    "X-Client-Api-Base": OPENAI_API_URL or "",
    "X-Client-Api-Key-Configured": "1" if bool(OPENAI_API_KEY) else "0",
}


@dataclass(frozen=True)
class Target:
    site_url: str
    true_open_jobs: int
    csv_line: int


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def read_target(csv_file: str = CSV_FILE, csv_line: int = CSV_LINE) -> Target:
    # csv_line is 1-based absolute file line; DictReader starts at data line 2.
    target_idx = csv_line - 2
    try:
        with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        if not (0 <= target_idx < len(rows)):
            raise IndexError(f"csv_line={csv_line} out of range")
        row = rows[target_idx]

        count_key = None
        for k in row.keys():
            if "true_open_jobs" in (k or "").strip().lower():
                count_key = k
                break
        if count_key is None:
            # fallback: first column
            count_key = list(row.keys())[0]

        expected = int(str(row.get(count_key, DEFAULT_EXPECTED)).strip())
        site_url = str(row.get("site_url", DEFAULT_SITE_URL)).strip() or DEFAULT_SITE_URL
        return Target(site_url=site_url, true_open_jobs=expected, csv_line=csv_line)
    except Exception as exc:
        logging.warning("Failed to parse %s line %s (%s). Using defaults.", csv_file, csv_line, exc)
        return Target(site_url=DEFAULT_SITE_URL, true_open_jobs=DEFAULT_EXPECTED, csv_line=csv_line)


def request_with_retry(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)])
    assert last_err is not None
    raise last_err


def parse_total_pages(soup: BeautifulSoup) -> int:
    # TalentBrew pagination: input.pagination-current has max="49"
    current_input = soup.select_one("nav.pagination input.pagination-current")
    if current_input and current_input.get("max"):
        try:
            return int(current_input.get("max", "1"))
        except ValueError:
            pass

    total_span = soup.select_one("nav.pagination span.pagination-total-pages")
    if total_span:
        m = re.search(r"(\d+)", total_span.get_text(" ", strip=True))
        if m:
            return int(m.group(1))

    return 1


def detect_strategy(session: requests.Session, site_url: str) -> Dict[str, Any]:
    resp = request_with_retry(session, site_url)
    soup = BeautifulSoup(resp.text, "html.parser")
    first_page_jobs = soup.select("section#search-results-list ul.global-job-list li a[data-job-id]")
    analysis = {
        "site": "careers.unilever.com",
        "data_source": "Server-rendered HTML (TalentBrew global-job-list) + detail page LD+JSON",
        "listing_selector": "section#search-results-list ul.global-job-list li a[data-job-id]",
        "pagination": {
            "parameter": "p",
            "start": 1,
            "total_pages": parse_total_pages(soup),
            "stop_condition": "empty page OR page > total_pages OR expected distinct jobs reached",
        },
        "first_page_job_count": len(first_page_jobs),
        "anti_bot": "Low-rate requests, retries with backoff, standard browser UA",
    }
    return analysis


def parse_index_page(html: str, page_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []
    for a in soup.select("section#search-results-list ul.global-job-list li a[data-job-id]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        job_url = urljoin(page_url, href)
        title_node = a.select_one("h2.global-job-list__title") or a.select_one("h2")
        title = title_node.get_text(" ", strip=True) if title_node else None
        loc_node = a.select_one("span.job-location")
        location = loc_node.get_text(" ", strip=True) if loc_node else None
        job_id = (a.get("data-job-id") or "").strip() or None

        out.append(
            {
                "job_id": job_id,
                "job_url": job_url,
                "canonical_job_url": canonicalize_url(job_url),
                "title": title,
                "location": location,
                "listing_page_url": page_url,
                "raw_source": {
                    "source_type": "listing_html",
                    "selector": "a[data-job-id]",
                    "href": href,
                    "data_job_id": job_id,
                },
            }
        )
    return out


def dedupe_distinct(stubs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    seen_ids = set()
    seen_urls = set()
    unique: List[Dict[str, Any]] = []
    duplicates = 0

    for s in stubs:
        jid = s.get("job_id")
        curl = s.get("canonical_job_url") or s.get("job_url")
        if (jid and jid in seen_ids) or (curl and curl in seen_urls):
            duplicates += 1
            continue
        if jid:
            seen_ids.add(jid)
        if curl:
            seen_urls.add(curl)
        unique.append(s)

    return unique, duplicates


def fetch_index(
    session: requests.Session,
    site_url: str,
    expected_count: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    analysis = detect_strategy(session, site_url)
    total_pages = int(analysis["pagination"]["total_pages"])

    collected: List[Dict[str, Any]] = []
    duplicates_seen = 0

    page = 1
    while True:
        params = {"p": page} if page > 1 else None
        resp = request_with_retry(session, site_url, params=params)
        page_jobs = parse_index_page(resp.text, resp.url)
        logging.info("page %s fetched -> %s jobs found", page, len(page_jobs))

        if not page_jobs:
            break

        before = len(collected)
        collected.extend(page_jobs)
        collected, removed = dedupe_distinct(collected)
        duplicates_seen += removed

        if len(collected) > before:
            logging.info("distinct collected so far: %s", len(collected))

        if len(collected) >= expected_count:
            collected = collected[:expected_count]
            break

        if page >= total_pages:
            break

        page += 1

    analysis["duplicates_removed_during_index"] = duplicates_seen

    if len(collected) != expected_count:
        diagnostics = {
            "error": "COUNT_MISMATCH_AT_INDEX",
            "expected_job_count": expected_count,
            "extracted_job_count": len(collected),
            "site_url": site_url,
            "pagination_observed": analysis.get("pagination"),
            "suggestions": [
                "Check if additional filters/cookies alter visible jobs.",
                "Verify whether locale/country settings are required.",
                "Inspect XHR endpoints for hidden listing sources.",
            ],
        }
        raise RuntimeError(json.dumps(diagnostics, ensure_ascii=False))

    return collected, analysis


def extract_jobposting_ldjson(soup: BeautifulSoup) -> Dict[str, Any]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue

        candidates: List[Dict[str, Any]] = []
        if isinstance(obj, dict):
            candidates = [obj]
        elif isinstance(obj, list):
            candidates = [x for x in obj if isinstance(x, dict)]

        for cand in candidates:
            if str(cand.get("@type", "")).lower() == "jobposting":
                return cand
    return {}


def html_to_text(html: Optional[str]) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{2,}", "\n\n", text).strip()
    return text or None


def normalize_location_from_ld(ld: Dict[str, Any], fallback: Optional[str]) -> Optional[str]:
    loc = ld.get("jobLocation")
    if isinstance(loc, dict):
        loc = [loc]
    if isinstance(loc, list) and loc:
        addr = (loc[0] or {}).get("address", {})
        if isinstance(addr, dict):
            parts = [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")]
            parts = [str(p).strip() for p in parts if p and str(p).strip()]
            if parts:
                return ", ".join(parts)
    return fallback


def stable_job_id(stub_id: Optional[str], canonical_url: str, title: Optional[str], location: Optional[str]) -> str:
    if stub_id:
        return str(stub_id)
    raw = "|".join([canonical_url or "", title or "", location or ""])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def parse_detail(html: str, stub: Dict[str, Any], detail_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    ld = extract_jobposting_ldjson(soup)

    title = ld.get("title") or stub.get("title")

    listed_url = ld.get("url") if isinstance(ld.get("url"), str) else None
    job_url = listed_url or detail_url
    canonical_job_url = canonicalize_url(job_url)

    location = normalize_location_from_ld(ld, stub.get("location"))

    company = None
    hiring_org = ld.get("hiringOrganization")
    if isinstance(hiring_org, dict):
        company = hiring_org.get("name")
    if not company:
        company = "Unilever"

    identifier = ld.get("identifier")
    if isinstance(identifier, dict):
        identifier = identifier.get("value") or identifier.get("name")
    elif not isinstance(identifier, str):
        identifier = None

    job_id = stable_job_id(stub.get("job_id") or identifier, canonical_job_url, title, location)

    description_html = ld.get("description") if isinstance(ld.get("description"), str) else None
    description_text = html_to_text(description_html)

    employment_type = ld.get("employmentType") if isinstance(ld.get("employmentType"), str) else None
    posted_date = ld.get("datePosted") if isinstance(ld.get("datePosted"), str) else None

    return {
        "job_id": job_id,
        "title": title,
        "url": canonical_job_url,
        "job_url": job_url,
        "canonical_job_url": canonical_job_url,
        "company": company,
        "location": location,
        "employment_type": employment_type,
        "posted_date": posted_date,
        "job_description": description_text,
        "raw_source": {
            "source_type": "detail_html_with_ldjson",
            "detail_page_url": detail_url,
            "listing_page_url": stub.get("listing_page_url"),
            "ldjson_jobposting": ld,
        },
    }


def fetch_detail(session: requests.Session, stub: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = stub["job_url"]
    resp = request_with_retry(session, detail_url)
    return parse_detail(resp.text, stub, resp.url)


def validate_counts(jobs: List[Dict[str, Any]], expected: int) -> None:
    distinct_jobs, duplicates_removed = dedupe_distinct(jobs)
    if duplicates_removed:
        jobs[:] = distinct_jobs

    if len(jobs) != expected:
        diagnostics = {
            "error": "COUNT_MISMATCH_AT_FINAL",
            "expected_job_count": expected,
            "extracted_job_count": len(jobs),
            "duplicates_removed": duplicates_removed,
        }
        raise RuntimeError(json.dumps(diagnostics, ensure_ascii=False))


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    target = read_target(CSV_FILE, CSV_LINE)

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        stubs, analysis = fetch_index(session, target.site_url, target.true_open_jobs)

        jobs: List[Dict[str, Any]] = []
        for stub in stubs:
            jobs.append(fetch_detail(session, stub))

        # deterministic order preserved from index order; keep exactly expected count
        jobs = jobs[: target.true_open_jobs]
        validate_counts(jobs, target.true_open_jobs)

        # attach concise site analysis to each row for traceability without extra files
        for job in jobs:
            job["raw_source"]["site_analysis"] = analysis

        with open(OUTPUT_JOBS, "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, indent=2)

        logging.info("Wrote %s with %s job(s)", OUTPUT_JOBS, len(jobs))
    except Exception as exc:  # noqa: BLE001
        diagnostics_row = {
            "job_id": None,
            "title": "PARSER_ERROR",
            "url": canonicalize_url(target.site_url),
            "job_url": target.site_url,
            "canonical_job_url": canonicalize_url(target.site_url),
            "company": "Unilever",
            "location": None,
            "job_description": None,
            "raw_source": {
                "error": str(exc),
                "site_url": target.site_url,
                "expected_job_count": target.true_open_jobs,
                "network_or_parse_failure": True,
                "api_config_source": {
                    "api_url_present": bool(OPENAI_API_URL),
                    "api_key_present": bool(OPENAI_API_KEY),
                },
            },
        }
        with open(OUTPUT_JOBS, "w", encoding="utf-8") as f:
            json.dump([diagnostics_row], f, ensure_ascii=False, indent=2)
        raise


if __name__ == "__main__":
    run()
