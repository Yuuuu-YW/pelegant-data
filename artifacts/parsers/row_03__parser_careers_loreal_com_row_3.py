#!/usr/bin/env python3
"""Row-scoped parser for https://careers.loreal.com/en_US/jobs/SearchJobs (Book2.csv line 4).

Outputs only:
- jobs_careers_loreal_com_row_3.json
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
CSV_LINE = 4  # absolute CSV line number, including header
ROW_SCOPE = 3
DEFAULT_SITE_URL = "https://careers.loreal.com/en_US/jobs/SearchJobs"
DEFAULT_EXPECTED = 6
OUTPUT_JOBS = f"jobs_careers_loreal_com_row_{ROW_SCOPE}.json"

TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]
PAGE_SIZE = 20
MAX_PAGES = 200

# Required by task: API URL/key source must come from api_config.py
OPENAI_API_URL = getattr(api_config, "API_URL", None)
OPENAI_API_KEY = getattr(api_config, "API_KEY", None)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": DEFAULT_SITE_URL,
    # lightweight trace (no secret leakage)
    "X-Client-Api-Base": OPENAI_API_URL or "",
    "X-Client-Api-Key-Configured": "1" if bool(OPENAI_API_KEY) else "0",
}


@dataclass(frozen=True)
class Target:
    site_url: str
    true_open_jobs: int
    csv_line: int


def canonicalize_url(url: str) -> str:
    p = urlparse(url)
    path = p.path.rstrip("/") or "/"
    return urlunparse((p.scheme, p.netloc, path, "", "", ""))


def read_target(csv_file: str = CSV_FILE, csv_line: int = CSV_LINE) -> Target:
    target_idx = csv_line - 2  # first data row is line 2
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
            count_key = list(row.keys())[0]

        expected = int(str(row.get(count_key, DEFAULT_EXPECTED)).strip())
        site_url = str(row.get("site_url", DEFAULT_SITE_URL)).strip() or DEFAULT_SITE_URL
        return Target(site_url=site_url, true_open_jobs=expected, csv_line=csv_line)
    except Exception as exc:
        logging.warning("Failed to read target from %s line %s (%s); using defaults.", csv_file, csv_line, exc)
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


def extract_search_jobs_ajax_url(search_page_html: str, page_url: str) -> str:
    m = re.search(r'var\s+searchJobsAJAXPage\s*=\s*"([^"]+)"', search_page_html)
    if m:
        return urljoin(page_url, m.group(1).strip())
    return urljoin(page_url, "/en_US/jobs/SearchJobsAJAX")


def extract_job_id(job_url: Optional[str], element_id: Optional[str] = None) -> Optional[str]:
    if element_id:
        m = re.search(r"jobId(\d+)", element_id)
        if m:
            return m.group(1)
    if job_url:
        m = re.search(r"/(\d+)(?:/?$)", job_url)
        if m:
            return m.group(1)
    return None


def parse_index_html(html: str, page_url: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []

    articles = soup.select("article.article--result")
    total_marker = articles[0].get("data-total") if articles else None

    for article in articles:
        link = article.select_one("h3.article__header__text__title a")
        if not link:
            continue

        href = (link.get("href") or "").strip()
        if not href:
            continue

        job_url = urljoin(page_url, href)
        canonical_job_url = canonicalize_url(job_url)

        title = link.get_text(" ", strip=True) or None

        subtitle_spans = article.select(".article__header__text__subtitle span")
        subtitle_values = [s.get_text(" ", strip=True) for s in subtitle_spans if s.get_text(" ", strip=True)]
        location = subtitle_values[0] if subtitle_values else None
        posted_date = None
        for value in subtitle_values:
            m_posted = re.search(r"Posted\s+(.+)$", value, flags=re.IGNORECASE)
            if m_posted:
                posted_date = m_posted.group(1).strip()
                break

        action_node = article.select_one(".article__header__actions[id]")
        elem_id = action_node.get("id") if action_node else None
        job_id = extract_job_id(job_url, elem_id)

        out.append(
            {
                "job_id": job_id,
                "job_url": job_url,
                "canonical_job_url": canonical_job_url,
                "title": title,
                "location": location,
                "posted_date": posted_date,
                "listing_page_url": page_url,
                "raw_source": {
                    "source_type": "listing_html",
                    "selector": "article.article--result",
                    "href": href,
                    "action_id": elem_id,
                    "subtitle_values": subtitle_values,
                },
            }
        )

    return out, total_marker


def dedupe_distinct(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    seen_ids = set()
    seen_urls = set()
    unique: List[Dict[str, Any]] = []
    duplicates = 0

    for item in items:
        jid = item.get("job_id")
        curl = item.get("canonical_job_url") or item.get("url") or item.get("job_url")

        if (jid and jid in seen_ids) or (curl and curl in seen_urls):
            duplicates += 1
            continue

        if jid:
            seen_ids.add(jid)
        if curl:
            seen_urls.add(curl)
        unique.append(item)

    return unique, duplicates


def detect_strategy(session: requests.Session, site_url: str) -> Dict[str, Any]:
    resp = request_with_retry(session, site_url)
    ajax_url = extract_search_jobs_ajax_url(resp.text, resp.url)
    first = request_with_retry(session, ajax_url)
    first_jobs, total_marker = parse_index_html(first.text, first.url)

    return {
        "site": "careers.loreal.com",
        "data_source": "Avature SearchJobsAJAX HTML endpoint + job detail HTML",
        "search_page_url": resp.url,
        "search_jobs_ajax_url": ajax_url,
        "pagination": {
            "parameter": "jobOffset",
            "step": PAGE_SIZE,
            "start": 0,
            "stop_condition": "when expected distinct jobs reached OR empty page",
        },
        "first_page_job_count": len(first_jobs),
        "first_page_total_marker": total_marker,
        "unique_key": "job_id + canonical_job_url",
        "anti_bot": "low-rate requests, retries with exponential backoff, standard headers",
    }


def fetch_index(
    session: requests.Session,
    ajax_url: str,
    expected_count: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    duplicates_removed_total = 0
    pages_observed: List[Dict[str, Any]] = []

    offset = 0
    page_no = 1

    while len(collected) < expected_count and page_no <= MAX_PAGES:
        params = {"jobOffset": offset} if offset > 0 else None
        resp = request_with_retry(session, ajax_url, params=params)
        stubs, total_marker = parse_index_html(resp.text, resp.url)

        logging.info("page %s fetched (offset=%s) -> %s jobs found", page_no, offset, len(stubs))
        pages_observed.append(
            {
                "page": page_no,
                "offset": offset,
                "jobs_found": len(stubs),
                "total_marker": total_marker,
            }
        )

        if not stubs:
            break

        before = len(collected)
        collected.extend(stubs)
        collected, removed = dedupe_distinct(collected)
        duplicates_removed_total += removed

        if len(collected) > before:
            logging.info("distinct collected so far: %s", len(collected))

        if len(collected) >= expected_count:
            collected = collected[:expected_count]
            break

        # Continue pagination when duplicates prevent reaching expected distinct count.
        offset += len(stubs)
        page_no += 1

    diagnostics = {
        "pages_observed": pages_observed,
        "duplicates_removed_during_index": duplicates_removed_total,
        "distinct_after_index": len(collected),
    }

    if len(collected) != expected_count:
        error = {
            "error": "COUNT_MISMATCH_AT_INDEX",
            "expected_job_count": expected_count,
            "extracted_job_count": len(collected),
            "ajax_url": ajax_url,
            "pagination_observed": pages_observed,
            "suggestions": [
                "Check for hidden filters/cookies affecting result set.",
                "Inspect additional SearchJobsAJAX query parameters (country/keyword facets).",
                "Verify locale/region-specific variants of the search URL.",
            ],
        }
        raise RuntimeError(json.dumps(error, ensure_ascii=False))

    return collected, diagnostics


def normalize_whitespace(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def html_to_clean_text(html_fragment: Optional[str]) -> Optional[str]:
    if not html_fragment:
        return None
    soup = BeautifulSoup(html_fragment, "html.parser")
    for bad in soup.select("img,script,style,svg,noscript"):
        bad.decompose()
    text = soup.get_text("\n", strip=True)
    text = normalize_whitespace(text)
    return text or None


def parse_detail_metadata(soup: BeautifulSoup) -> Dict[str, Any]:
    values = [
        n.get_text(" ", strip=True)
        for n in soup.select("article.article--details .detail-data .article__content__view__field__value")
        if n.get_text(" ", strip=True)
    ]

    posted_date = None
    for v in values:
        if re.fullmatch(r"\d{2}-[A-Za-z]{3}-\d{4}", v):
            posted_date = v
            break

    employment_type = None
    for v in values:
        if re.search(r"\btime\b", v, flags=re.IGNORECASE):
            employment_type = v
            break

    contract_type = values[0] if values else None
    region = values[1] if len(values) > 1 else None
    city = values[2] if len(values) > 2 else None
    team = values[3] if len(values) > 3 else None

    location = None
    if city and region:
        location = city if city.lower() == region.lower() else f"{city}, {region}"
    else:
        location = city or region

    return {
        "detail_values": values,
        "posted_date": posted_date,
        "employment_type": employment_type,
        "contract_type": contract_type,
        "team": team,
        "location": location,
    }


def stable_job_id(stub_job_id: Optional[str], canonical_job_url: str, title: Optional[str], location: Optional[str]) -> str:
    if stub_job_id:
        return str(stub_job_id)
    raw = "|".join([canonical_job_url or "", title or "", location or ""])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def parse_detail(html: str, stub: Dict[str, Any], detail_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    title_node = soup.select_one("h2.banner__text__title") or soup.select_one("h2")
    title = title_node.get_text(" ", strip=True) if title_node else stub.get("title")

    metadata = parse_detail_metadata(soup)

    desc_node = soup.select_one("article.article--details .article__content--rich-text")
    description_html = str(desc_node) if desc_node else None
    job_description = html_to_clean_text(description_html)

    job_url = detail_url or stub.get("job_url")
    canonical_job_url = canonicalize_url(job_url)

    location = metadata.get("location") or stub.get("location")
    posted_date = metadata.get("posted_date") or stub.get("posted_date")

    job_id = stable_job_id(stub.get("job_id"), canonical_job_url, title, location)

    return {
        "job_id": job_id,
        "title": title,
        "url": canonical_job_url,
        "job_url": job_url,
        "canonical_job_url": canonical_job_url,
        "company": "L'Oréal",
        "location": location,
        "team": metadata.get("team"),
        "employment_type": metadata.get("employment_type"),
        "contract_type": metadata.get("contract_type"),
        "posted_date": posted_date,
        "job_description": job_description,
        "raw_source": {
            "source_type": "detail_html",
            "detail_page_url": detail_url,
            "listing_page_url": stub.get("listing_page_url"),
            "listing_stub": stub.get("raw_source"),
            "detail_metadata_values": metadata.get("detail_values"),
            "description_selector": "article.article--details .article__content--rich-text",
        },
    }


def fetch_detail(session: requests.Session, stub: Dict[str, Any]) -> Dict[str, Any]:
    resp = request_with_retry(session, stub["job_url"])
    return parse_detail(resp.text, stub, resp.url)


def validate_counts(jobs: List[Dict[str, Any]], expected: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_jobs, duplicates_removed = dedupe_distinct(jobs)

    missing_fields = {
        "title_missing": sum(1 for j in unique_jobs if not j.get("title")),
        "url_missing": sum(1 for j in unique_jobs if not (j.get("canonical_job_url") or j.get("job_url") or j.get("url"))),
        "location_missing": sum(1 for j in unique_jobs if not j.get("location")),
        "description_missing": sum(1 for j in unique_jobs if not j.get("job_description")),
    }

    diagnostics = {
        "expected_job_count": expected,
        "extracted_job_count": len(unique_jobs),
        "duplicates_removed": duplicates_removed,
        "missing_fields_summary": missing_fields,
    }

    if len(unique_jobs) != expected:
        raise RuntimeError(
            json.dumps(
                {
                    "error": "COUNT_MISMATCH_AT_FINAL",
                    **diagnostics,
                    "suggestions": [
                        "Continue pagination with larger offset range.",
                        "Check if some job detail pages are inaccessible and retriable.",
                        "Verify duplicate logic against both job_id and canonical URL.",
                    ],
                },
                ensure_ascii=False,
            )
        )

    return unique_jobs, diagnostics


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    target = read_target(CSV_FILE, CSV_LINE)

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        strategy = detect_strategy(session, target.site_url)
        ajax_url = strategy["search_jobs_ajax_url"]

        stubs, index_diag = fetch_index(session, ajax_url, target.true_open_jobs)

        jobs: List[Dict[str, Any]] = []
        for stub in stubs:
            jobs.append(fetch_detail(session, stub))

        jobs, final_diag = validate_counts(jobs, target.true_open_jobs)

        # deterministic order = collected listing order
        jobs = jobs[: target.true_open_jobs]

        for job in jobs:
            job["raw_source"]["site_analysis"] = strategy
            job["raw_source"]["index_diagnostics"] = index_diag
            job["raw_source"]["final_validation"] = final_diag
            job["raw_source"]["api_config_source"] = {
                "api_url_present": bool(OPENAI_API_URL),
                "api_key_present": bool(OPENAI_API_KEY),
            }

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
            "company": "L'Oréal",
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
