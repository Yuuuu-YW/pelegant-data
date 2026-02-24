#!/usr/bin/env python3
"""
Run Codex repeatedly with a file-backed model instructions prompt.

This script does not use the OpenAI API directly. It shells out to `codex exec`
for each iteration and records an auditable trail for every run.

Each run can be mapped to exactly one website from a CSV file.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_text_file(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"{label} not found: {path}") from exc


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def site_slug(site_url: str) -> str:
    parsed = urlparse(site_url)
    host = (parsed.netloc or parsed.path or "site").lower()
    host = host.split("@")[-1].split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    slug = re.sub(r"[^a-z0-9]+", "_", host).strip("_")
    return slug or "site"


def expected_artifacts(row_number: int, site_url: str) -> dict[str, Path]:
    slug = site_slug(site_url)
    return {
        "parser_py": Path(f"parser_{slug}_row_{row_number}.py"),
        "jobs_json": Path(f"jobs_{slug}_row_{row_number}.json"),
    }


def remove_stale_artifacts(artifacts: dict[str, Path]) -> None:
    for path in artifacts.values():
        if path.exists() and path.is_file():
            path.unlink()


def cleanup_row_extras(row_number: int, site_url: str) -> None:
    slug = site_slug(site_url)
    extra_files = [
        Path(f"validation_{slug}_row_{row_number}.json"),
    ]
    for path in extra_files:
        if path.exists() and path.is_file():
            path.unlink()


def _to_expected_int(expected_count: Any) -> int | None:
    s = str(expected_count).strip()
    if s.isdigit():
        return int(s)
    return None


def validate_artifacts(artifacts: dict[str, Path], expected_count: Any) -> dict[str, Any]:
    parser_path = artifacts["parser_py"]
    jobs_path = artifacts["jobs_json"]
    expected_int = _to_expected_int(expected_count)

    parser_exists = parser_path.is_file() and parser_path.stat().st_size > 0
    parser_syntax_ok = False
    parser_error = ""
    if parser_exists:
        parser_check = subprocess.run(
            [sys.executable, "-m", "py_compile", str(parser_path)],
            capture_output=True,
            text=True,
        )
        parser_syntax_ok = parser_check.returncode == 0
        parser_error = (parser_check.stderr or "").strip()

    jobs_exists = jobs_path.is_file() and jobs_path.stat().st_size > 0
    jobs_json_valid = False
    jobs_is_list = False
    jobs_count = 0
    jobs_schema_ok = False
    jobs_error = ""
    if jobs_exists:
        try:
            jobs_data = json.loads(jobs_path.read_text(encoding="utf-8"))
            jobs_json_valid = True
            jobs_is_list = isinstance(jobs_data, list)
            if jobs_is_list:
                jobs_count = len(jobs_data)
                # Basic schema sanity: require at least title + URL-ish field.
                jobs_schema_ok = all(
                    isinstance(item, dict)
                    and bool(str(item.get("title", "")).strip())
                    and (
                        bool(str(item.get("job_url", "")).strip())
                        or bool(str(item.get("canonical_job_url", "")).strip())
                        or bool(str(item.get("url", "")).strip())
                        or bool(str(item.get("apply_url", "")).strip())
                        or bool(str(item.get("link", "")).strip())
                    )
                    for item in jobs_data
                )
        except Exception as exc:
            jobs_error = str(exc)

    issues: list[str] = []
    if not parser_exists:
        issues.append(f"Missing file: {parser_path}")
    elif not parser_syntax_ok:
        issues.append(f"Python syntax invalid: {parser_path} ({parser_error[:200]})")

    if not jobs_exists:
        issues.append(f"Missing file: {jobs_path}")
    elif not jobs_json_valid:
        issues.append(f"Invalid JSON: {jobs_path} ({jobs_error[:200]})")
    elif not jobs_is_list:
        issues.append(f"jobs JSON must be a list: {jobs_path}")
    elif jobs_count == 0:
        issues.append(f"jobs JSON is empty: {jobs_path}")
    elif not jobs_schema_ok:
        issues.append(f"jobs JSON schema check failed: {jobs_path}")

    if expected_int is not None and jobs_json_valid and jobs_is_list and jobs_count != expected_int:
        issues.append(
            f"jobs count mismatch: expected {expected_int}, got {jobs_count} in {jobs_path}"
        )

    return {
        "success": len(issues) == 0,
        "parser_exists": parser_exists,
        "parser_syntax_ok": parser_syntax_ok,
        "parser_error": parser_error,
        "jobs_exists": jobs_exists,
        "jobs_json_valid": jobs_json_valid,
        "jobs_is_list": jobs_is_list,
        "jobs_count": jobs_count,
        "jobs_schema_ok": jobs_schema_ok,
        "jobs_error": jobs_error,
        "expected_count": expected_int,
        "issues": issues,
        "artifact_paths": {k: str(v) for k, v in artifacts.items()},
    }


def build_attempt_prompt(
    base_prompt: str,
    row_number: int,
    attempt: int,
    max_attempts: int,
    artifacts: dict[str, Path],
    previous_issues: list[str] | None = None,
) -> str:
    artifact_contract = (
        "Execution contract for this attempt:\n"
        f"- Required files in workspace root: {artifacts['parser_py']} and {artifacts['jobs_json']}\n"
        "- Write these files first before long exploration.\n"
        "- jobs JSON must be a non-empty list of real jobs with title and URL fields.\n"
        "- extracted job count must match true_open_jobs exactly.\n"
        "- If network/DNS is unavailable, still create parser + jobs JSON diagnostics with explicit error reasons.\n"
        "- Do not switch to other websites.\n"
        "- Do not create additional project-root files beyond the two required files.\n"
        "- Prefer local file edits and finish quickly."
    )

    retry_note = ""
    if previous_issues:
        joined = "\n".join(f"- {issue}" for issue in previous_issues)
        retry_note = (
            f"\n\nPrevious attempt issues (attempt {attempt - 1}):\n"
            f"{joined}\n"
            "Fix only these issues now."
        )

    return (
        f"{base_prompt}\n\n"
        f"{artifact_contract}{retry_note}\n\n"
        f"Attempt {attempt}/{max_attempts}. "
        f"Reply exactly: DONE row {row_number} attempt {attempt}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Loop Codex calls using system_prompt.txt as model instructions. "
            "By default, each child run is assigned one website from a CSV."
        )
    )
    parser.add_argument(
        "--system-prompt-file",
        default="system_prompt.txt",
        help="Path to the file used as Codex model instructions.",
    )
    parser.add_argument(
        "--csv-file",
        default="Book2.csv",
        help="CSV file containing website targets.",
    )
    parser.add_argument(
        "--site-column",
        help="Column name for website URLs. Auto-detected if omitted.",
    )
    parser.add_argument(
        "--count-column",
        help="Column name for expected job count. Auto-detected if omitted.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="1-based index into detected website rows.",
    )
    parser.add_argument(
        "--user-prompt",
        default=(
            "Run {iteration}/{total}. Work on exactly one website.\n"
            "- csv_file: {csv_file}\n"
            "- csv_line: {csv_line}\n"
            "- site_url: {site_url}\n"
            "- true_open_jobs: {expected_count}\n\n"
            "Rules:\n"
            "1) Build parser code only for this website.\n"
            "2) Do not switch to other websites.\n"
            "3) Use api_config.py for API URL and key source.\n"
            "4) Produce row-scoped outputs to avoid overwrite, e.g. jobs_row_{row_number}.json.\n"
            "5) If extracted count != true_open_jobs, fail explicitly with diagnostics."
        ),
        help=(
            "User prompt template. Supported placeholders: "
            "{iteration}, {total}, {csv_file}, {site_url}, {expected_count}, "
            "{site_slug}, {row_number}, {csv_line}, {site_column}, {count_column}"
        ),
    )
    parser.add_argument(
        "--user-prompt-file",
        help="Optional file containing user prompt template. Overrides --user-prompt.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="How many Codex calls to run.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        help="Sleep between iterations.",
    )
    parser.add_argument(
        "--iteration-timeout-seconds",
        type=int,
        default=600,
        help="Timeout for each child Codex run in seconds (default: 600).",
    )
    parser.add_argument(
        "--max-attempts-per-target",
        type=int,
        default=3,
        help="Retry count per website until required artifacts are produced (default: 3).",
    )
    parser.add_argument(
        "--model",
        help="Optional Codex model override, e.g. gpt-5.3-codex.",
    )
    parser.add_argument(
        "--codex-bin",
        default="codex",
        help="Path to codex binary.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: /tmp/codex_runs/<timestamp>).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue remaining iterations if one iteration fails.",
    )
    parser.add_argument(
        "--aggregate-file",
        default="result.json",
        help="Combined result JSON file that accumulates jobs from each successful row.",
    )
    parser.add_argument(
        "--no-aggregate",
        action="store_true",
        help="Disable appending successful row jobs to aggregate file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only write rendered prompts and metadata; do not call codex.",
    )
    parser.add_argument(
        "--sandbox-mode",
        default="workspace-write",
        help="Child Codex sandbox mode (default: workspace-write).",
    )
    parser.add_argument(
        "--approval-policy",
        default="never",
        help="Child Codex approval policy (default: never).",
    )
    parser.add_argument(
        "--ephemeral",
        action="store_true",
        default=True,
        help="Run each child Codex session as ephemeral (default: true).",
    )
    parser.add_argument(
        "--no-ephemeral",
        dest="ephemeral",
        action="store_false",
        help="Disable ephemeral mode and keep child session files.",
    )
    parser.add_argument(
        "--disable-web-search",
        dest="disable_web_search",
        action="store_true",
        default=True,
        help="Disable child web_search tool to prioritize artifact generation (default: true).",
    )
    parser.add_argument(
        "--allow-web-search",
        dest="disable_web_search",
        action="store_false",
        help="Allow child web_search tool.",
    )
    return parser.parse_args()


def render_prompt(template: str, variables: dict[str, Any]) -> str:
    try:
        return template.format(**variables)
    except KeyError as exc:
        raise SystemExit(
            f"Invalid template placeholder: {exc}. Allowed placeholders: "
            "{iteration}, {total}, {csv_file}, {site_url}, {expected_count}, "
            "{site_slug}, {row_number}, {csv_line}, {site_column}, {count_column}"
        ) from exc


def ensure_dirs(root: Path) -> dict[str, Path]:
    paths = {
        "root": root,
        "prompts": root / "prompts",
        "responses": root / "responses",
        "events": root / "events",
        "stderr": root / "stderr",
        "meta": root / "meta",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def _normalize_colname(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    return normalized.strip("_")


def _resolve_column(
    fieldnames: list[str],
    explicit_name: str | None,
    aliases: list[str],
    label: str,
) -> str:
    if not fieldnames:
        raise SystemExit("CSV has no header row")

    by_normalized = {_normalize_colname(name): name for name in fieldnames}

    if explicit_name:
        if explicit_name in fieldnames:
            return explicit_name
        normalized_explicit = _normalize_colname(explicit_name)
        if normalized_explicit in by_normalized:
            return by_normalized[normalized_explicit]
        raise SystemExit(f"{label} column '{explicit_name}' not found in CSV header: {fieldnames}")

    for alias in aliases:
        if alias in by_normalized:
            return by_normalized[alias]

    # Heuristic fallback for noisy/typo headers.
    normalized_names = list(by_normalized.keys())
    if label == "site":
        for name in normalized_names:
            if "url" in name:
                return by_normalized[name]
    if label == "count":
        for name in normalized_names:
            if "count" in name:
                return by_normalized[name]
        for name in normalized_names:
            tokens = set(name.split("_"))
            if {"true", "open"}.issubset(tokens) and ("jobs" in tokens or "job" in tokens):
                return by_normalized[name]
            if ("job" in tokens or "jobs" in tokens) and ("listing" in tokens or "listings" in tokens):
                return by_normalized[name]
            # e.g. "true_open_jobsLstings" -> "true_open_jobslstings"
            if "true" in name and "open" in name and "job" in name:
                return by_normalized[name]
            if "job" in name and "list" in name:
                return by_normalized[name]

    raise SystemExit(
        f"Could not auto-detect {label} column. CSV header: {fieldnames}. "
        f"Use --{label}-column to specify explicitly."
    )


def load_targets(
    csv_file: Path,
    site_column: str | None,
    count_column: str | None,
) -> tuple[list[dict[str, Any]], str, str]:
    try:
        with csv_file.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            fieldnames = reader.fieldnames or []
            resolved_site_column = _resolve_column(
                fieldnames=fieldnames,
                explicit_name=site_column,
                aliases=["site_url", "url", "urls", "website_url", "website", "career_url"],
                label="site",
            )
            resolved_count_column = _resolve_column(
                fieldnames=fieldnames,
                explicit_name=count_column,
                aliases=[
                    "true_open_jobs",
                    "expected_job_count",
                    "hong_kong_job_listings_count",
                    "job_count",
                    "open_jobs",
                    "count",
                ],
                label="count",
            )

            targets: list[dict[str, Any]] = []
            for row_number, row in enumerate(reader, start=1):
                site_url = str(row.get(resolved_site_column, "") or "").strip()
                if not site_url:
                    continue
                expected_count = str(row.get(resolved_count_column, "") or "").strip()
                slug = site_slug(site_url)
                targets.append(
                    {
                        "row_number": row_number,
                        "csv_line": row_number + 1,
                        "site_url": site_url,
                        "site_slug": slug,
                        "expected_count": expected_count or "N/A",
                    }
                )
    except FileNotFoundError as exc:
        raise SystemExit(f"CSV file not found: {csv_file}") from exc

    if not targets:
        raise SystemExit(f"No website URLs found in CSV column '{resolved_site_column}'")

    return targets, resolved_site_column, resolved_count_column


def load_json_list(path: Path) -> list[Any]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, list):
        raise RuntimeError(f"Expected top-level JSON list in {path}")
    return payload


def _job_identity(item: Any) -> str:
    if not isinstance(item, dict):
        return sha256_hex(json.dumps(item, sort_keys=True, ensure_ascii=False).encode("utf-8"))
    for key in ("job_id", "reqId", "id"):
        value = str(item.get(key, "")).strip()
        if value:
            return f"id:{value}"
    for key in ("job_url", "canonical_job_url", "url", "apply_url", "link"):
        value = str(item.get(key, "")).strip()
        if value:
            return f"url:{value}"
    fallback = json.dumps(item, sort_keys=True, ensure_ascii=False)
    return f"hash:{sha256_hex(fallback.encode('utf-8'))}"


def append_jobs_to_aggregate(aggregate_path: Path, jobs_path: Path) -> dict[str, int]:
    aggregate_jobs = load_json_list(aggregate_path)
    new_jobs = load_json_list(jobs_path)

    seen = {_job_identity(job) for job in aggregate_jobs}
    added = 0
    skipped = 0
    for job in new_jobs:
        identity = _job_identity(job)
        if identity in seen:
            skipped += 1
            continue
        seen.add(identity)
        aggregate_jobs.append(job)
        added += 1

    aggregate_path.write_text(
        json.dumps(aggregate_jobs, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {"added": added, "skipped_duplicates": skipped, "aggregate_total": len(aggregate_jobs)}


def build_command(
    codex_bin: str,
    response_file: Path,
    system_prompt_file: Path,
    model: str | None,
    sandbox_mode: str,
    approval_policy: str,
    ephemeral: bool,
    disable_web_search: bool,
) -> list[str]:
    command = [
        codex_bin,
        "exec",
        "--skip-git-repo-check",
        "--json",
        "-o",
        str(response_file),
        "-c",
        f"model_instructions_file={json.dumps(str(system_prompt_file.resolve()))}",
        "-c",
        f"sandbox_mode={json.dumps(sandbox_mode)}",
        "-c",
        f"approval_policy={json.dumps(approval_policy)}",
    ]
    if disable_web_search:
        command.extend(["-c", 'disabled_tools=["web_search"]'])
    if ephemeral:
        command.append("--ephemeral")
    if model:
        command.extend(["-m", model])
    command.append("-")
    return command


def main() -> int:
    args = parse_args()
    if args.iterations <= 0:
        raise SystemExit("--iterations must be >= 1")
    if args.start_index <= 0:
        raise SystemExit("--start-index must be >= 1")
    if args.delay_seconds < 0:
        raise SystemExit("--delay-seconds must be >= 0")
    if args.iteration_timeout_seconds <= 0:
        raise SystemExit("--iteration-timeout-seconds must be >= 1")
    if args.max_attempts_per_target <= 0:
        raise SystemExit("--max-attempts-per-target must be >= 1")
    aggregate_path = Path(args.aggregate_file)

    system_prompt_path = Path(args.system_prompt_file)
    system_prompt_text = read_text_file(system_prompt_path, "System prompt file")
    if not system_prompt_text.strip():
        raise SystemExit(f"System prompt file is empty: {system_prompt_path}")
    baseline_system_hash = sha256_hex(system_prompt_text.encode("utf-8"))

    if args.user_prompt_file:
        user_prompt_template = read_text_file(Path(args.user_prompt_file), "User prompt file")
    else:
        user_prompt_template = args.user_prompt

    csv_path = Path(args.csv_file)
    targets, resolved_site_column, resolved_count_column = load_targets(
        csv_file=csv_path,
        site_column=args.site_column,
        count_column=args.count_column,
    )
    selected_targets = targets[args.start_index - 1 : args.start_index - 1 + args.iterations]
    if not selected_targets:
        raise SystemExit(
            f"No targets selected. start_index={args.start_index}, total_targets={len(targets)}"
        )

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = Path(args.output_dir) if args.output_dir else Path("/tmp/codex_runs") / run_id
    paths = ensure_dirs(root)

    manifest = {
        "started_at_utc": utc_now_iso(),
        "run_id": run_id,
        "cwd": str(Path.cwd()),
        "codex_bin": args.codex_bin,
        "model": args.model or "default-from-codex-config",
        "system_prompt_file": str(system_prompt_path.resolve()),
        "system_prompt_sha256": baseline_system_hash,
        "iterations_requested": args.iterations,
        "iterations_planned": len(selected_targets),
        "delay_seconds": args.delay_seconds,
        "iteration_timeout_seconds": args.iteration_timeout_seconds,
        "max_attempts_per_target": args.max_attempts_per_target,
        "dry_run": args.dry_run,
        "continue_on_error": args.continue_on_error,
        "csv_file": str(csv_path.resolve()),
        "site_column": resolved_site_column,
        "count_column": resolved_count_column,
        "start_index": args.start_index,
        "sandbox_mode": args.sandbox_mode,
        "approval_policy": args.approval_policy,
        "ephemeral": args.ephemeral,
        "disable_web_search": args.disable_web_search,
        "aggregate_file": str(aggregate_path.resolve()),
        "aggregate_enabled": not args.no_aggregate,
        "results": [],
    }
    (paths["root"] / "targets.json").write_text(
        json.dumps(selected_targets, indent=2),
        encoding="utf-8",
    )

    failures = 0

    for iteration, target in enumerate(selected_targets, start=1):
        print(
            f"[{iteration}/{len(selected_targets)}] row={target['row_number']} site={target['site_url']}",
            flush=True,
        )
        current_system_text = read_text_file(system_prompt_path, "System prompt file")
        current_system_hash = sha256_hex(current_system_text.encode("utf-8"))
        if current_system_hash != baseline_system_hash:
            raise SystemExit(
                "System prompt changed during run. "
                f"Expected {baseline_system_hash}, got {current_system_hash}."
            )

        prompt_vars = {
            "iteration": iteration,
            "total": len(selected_targets),
            "csv_file": str(csv_path),
            "site_url": target["site_url"],
            "site_slug": target["site_slug"],
            "expected_count": target["expected_count"],
            "row_number": target["row_number"],
            "csv_line": target["csv_line"],
            "site_column": resolved_site_column,
            "count_column": resolved_count_column,
        }
        rendered_user_prompt = render_prompt(user_prompt_template, prompt_vars)
        prompt_hash = sha256_hex(rendered_user_prompt.encode("utf-8"))
        artifacts = expected_artifacts(target["row_number"], target["site_url"])
        remove_stale_artifacts(artifacts)
        cleanup_row_extras(target["row_number"], target["site_url"])

        meta_file = paths["meta"] / f"iteration_{iteration:04d}.json"

        record = {
            "iteration": iteration,
            "started_at_utc": utc_now_iso(),
            "system_prompt_sha256": baseline_system_hash,
            "user_prompt_sha256": prompt_hash,
            "exit_code": None,
            "elapsed_seconds": None,
            "timed_out": False,
            "target": target,
            "artifact_paths": {k: str(v) for k, v in artifacts.items()},
            "attempts": [],
        }

        if args.dry_run:
            dry_prompt = build_attempt_prompt(
                base_prompt=rendered_user_prompt,
                row_number=target["row_number"],
                attempt=1,
                max_attempts=args.max_attempts_per_target,
                artifacts=artifacts,
            )
            prompt_file = paths["prompts"] / f"iteration_{iteration:04d}_attempt_01.txt"
            prompt_file.write_text(dry_prompt, encoding="utf-8")
            record["exit_code"] = 0
            record["elapsed_seconds"] = 0.0
            record["status"] = "dry-run"
            record["attempts"].append(
                {
                    "attempt": 1,
                    "status": "dry-run",
                    "prompt_file": str(prompt_file),
                }
            )
            meta_file.write_text(json.dumps(record, indent=2), encoding="utf-8")
            manifest["results"].append(record)
            continue

        previous_issues: list[str] | None = None
        artifact_success = False
        for attempt in range(1, args.max_attempts_per_target + 1):
            print(
                f"  attempt {attempt}/{args.max_attempts_per_target} for row {target['row_number']}",
                flush=True,
            )
            attempt_tag = f"iteration_{iteration:04d}_attempt_{attempt:02d}"
            prompt_file = paths["prompts"] / f"{attempt_tag}.txt"
            response_file = paths["responses"] / f"{attempt_tag}.txt"
            events_file = paths["events"] / f"{attempt_tag}.jsonl"
            stderr_file = paths["stderr"] / f"{attempt_tag}.log"

            attempt_prompt = build_attempt_prompt(
                base_prompt=rendered_user_prompt,
                row_number=target["row_number"],
                attempt=attempt,
                max_attempts=args.max_attempts_per_target,
                artifacts=artifacts,
                previous_issues=previous_issues,
            )
            prompt_file.write_text(attempt_prompt, encoding="utf-8")

            command = build_command(
                codex_bin=args.codex_bin,
                response_file=response_file,
                system_prompt_file=system_prompt_path,
                model=args.model,
                sandbox_mode=args.sandbox_mode,
                approval_policy=args.approval_policy,
                ephemeral=args.ephemeral,
                disable_web_search=args.disable_web_search,
            )

            started = time.time()
            timeout_expired = False
            try:
                proc = subprocess.run(
                    command,
                    input=attempt_prompt,
                    text=True,
                    capture_output=True,
                    timeout=args.iteration_timeout_seconds,
                )
            except subprocess.TimeoutExpired as exc:
                timeout_expired = True

                class _TimedOutProc:
                    returncode = 124
                    stdout = as_text(exc.stdout)
                    stderr = as_text(exc.stderr)

                proc = _TimedOutProc()  # type: ignore
            elapsed = round(time.time() - started, 3)

            events_file.write_text(as_text(proc.stdout), encoding="utf-8")
            stderr_file.write_text(as_text(proc.stderr), encoding="utf-8")
            artifact_check = validate_artifacts(artifacts, expected_count=target["expected_count"])
            previous_issues = artifact_check["issues"]
            cleanup_row_extras(target["row_number"], target["site_url"])

            attempt_status = (
                "ok_artifacts"
                if artifact_check["success"]
                else ("timed_out" if timeout_expired else "missing_artifacts")
            )
            attempt_record = {
                "attempt": attempt,
                "status": attempt_status,
                "timed_out": timeout_expired,
                "exit_code": proc.returncode,
                "elapsed_seconds": elapsed,
                "prompt_file": str(prompt_file),
                "response_file": str(response_file),
                "events_file": str(events_file),
                "stderr_file": str(stderr_file),
                "command": command,
                "artifact_check": artifact_check,
            }
            record["attempts"].append(attempt_record)
            print(
                f"    status={attempt_status} exit={proc.returncode} "
                f"jobs_count={artifact_check.get('jobs_count')} issues={len(artifact_check.get('issues', []))}",
                flush=True,
            )

            if artifact_check["success"]:
                artifact_success = True
                break

        last_attempt = record["attempts"][-1]
        record["exit_code"] = last_attempt["exit_code"]
        record["elapsed_seconds"] = sum(float(a["elapsed_seconds"]) for a in record["attempts"])
        record["timed_out"] = any(bool(a["timed_out"]) for a in record["attempts"])
        record["status"] = "ok_artifacts" if artifact_success else "failed_missing_artifacts"
        record["finished_at_utc"] = utc_now_iso()
        record["attempts_used"] = len(record["attempts"])

        if artifact_success and not args.no_aggregate:
            aggregate_stats = append_jobs_to_aggregate(
                aggregate_path=aggregate_path,
                jobs_path=artifacts["jobs_json"],
            )
            record["aggregate"] = {
                "file": str(aggregate_path),
                **aggregate_stats,
            }
            print(
                f"  aggregate added={aggregate_stats['added']} "
                f"skipped_duplicates={aggregate_stats['skipped_duplicates']} "
                f"total={aggregate_stats['aggregate_total']}",
                flush=True,
            )

        if not artifact_success:
            failures += 1
            print(f"  row {target['row_number']} failed", flush=True)

        meta_file.write_text(json.dumps(record, indent=2), encoding="utf-8")
        manifest["results"].append(record)

        if not artifact_success and not args.continue_on_error:
            break

        if args.delay_seconds > 0 and iteration < len(selected_targets):
            time.sleep(args.delay_seconds)

    manifest["finished_at_utc"] = utc_now_iso()
    manifest["iterations_completed"] = len(manifest["results"])
    manifest["failures"] = failures
    manifest["status"] = "ok" if failures == 0 else "failed"
    (paths["root"] / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    summary = {
        "status": manifest["status"],
        "iterations_completed": manifest["iterations_completed"],
        "iterations_requested": args.iterations,
        "iterations_planned": len(selected_targets),
        "failures": failures,
        "run_dir": str(paths["root"].resolve()),
    }
    print(json.dumps(summary, indent=2))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
