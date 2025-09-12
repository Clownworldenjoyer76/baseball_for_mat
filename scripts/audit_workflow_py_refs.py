#!/usr/bin/env python3
"""
audit_workflow_py_refs.py

Run from the repository root. It will:
  - Inspect .github/workflows/*.yml|*.yaml
  - Parse each workflow's jobs/steps and scan `run:` script blocks
  - Detect .py files that are:
      (A) referenced/executed (e.g., "python some/path/foo.py")
      (B) written/created from the workflow (e.g., "cat > scripts/bar.py <<'PY'")
  - Write results to:
      /audit/workflow_py_files.csv
      /audit/summary.txt

Exit code 0 on success. Non-zero if workflow directory not found or on unexpected errors.
"""

from __future__ import annotations
import sys
import re
import json
import csv
from pathlib import Path

try:
    import yaml  # PyYAML
except Exception:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

ROOT = Path(".").resolve()
WF_DIR = ROOT / ".github" / "workflows"
AUDIT_DIR = ROOT / "audit"
CSV_PATH = AUDIT_DIR / "workflow_py_files.csv"
SUMMARY_PATH = AUDIT_DIR / "summary.txt"

# --- Regex detectors ---------------------------------------------------------

# 1) .py paths referenced/executed (python/pypy optional)
#    Examples:
#      python scripts/foo.py
#      pypy ./tools/build.py
#      ./scripts/run.py
#      bash scripts/do.py   (rare, but still a reference)
PY_REF_RE = re.compile(
    r"""(?xi)
    (?:
        (?:(?:^|\s)(?:python|pypy)\s+)|   # optional python/pypy prefix
        (?:^|\s)                          # or just whitespace/start
    )
    (?P<path>(?:\.?/?[\w.\-\/]+)\.py)\b
    """
)

# 2) .py files written/created by shell redirections or tee:
#    Examples:
#      cat > scripts/gen.py <<'PY'
#      printf '...' > tools/x.py
#      tee -a src/made.py <<EOF
PY_WRITE_RE = re.compile(
    r"""(?xi)
    (?:
        (?:>\s*|>>\s*|tee\s+-?a?\s+)
        (?P<path>[\w.\-\/]+\.py)\b
    )
    """
)

# 3) Generic .py appearance (fallback, used for raw-text scan)
PY_ANY_RE = re.compile(r"(?P<path>[\w.\-\/]+\.py)\b")


def _as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def extract_from_run_block(cmd: str) -> list[dict]:
    """
    Inspect a single shell `run:` string and return list of detections:
        { 'type': 'referenced'|'written', 'script_path': 'path/to/x.py', 'line': <line_no> }
    """
    findings = []
    for idx, line in enumerate(cmd.splitlines(), start=1):
        # referenced/executed
        for m in PY_REF_RE.finditer(line):
            findings.append({"type": "referenced", "script_path": m.group("path"), "line": idx})
        # written/created
        for m in PY_WRITE_RE.finditer(line):
            findings.append({"type": "written", "script_path": m.group("path"), "line": idx})
    return findings


def scan_yaml_workflow(path: Path) -> list[dict]:
    """
    Parse a workflow YAML and scan all jobs[*].steps[*].run blocks.
    Returns a list of rows with context.
    """
    rows = []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as e:
        # If YAML parse fails, fall back to raw text scan
        rows.extend(scan_raw_text(path))
        return rows

    if not isinstance(data, dict):
        return rows

    jobs = data.get("jobs", {}) or {}
    for job_id, job in (jobs.items() if isinstance(jobs, dict) else []):
        steps = job.get("steps", []) or []
        for step in steps:
            if not isinstance(step, dict):
                continue
            name = step.get("name") or ""
            run = step.get("run")
            if isinstance(run, str):
                for item in extract_from_run_block(run):
                    rows.append({
                        "workflow_file": str(path.relative_to(ROOT)),
                        "job_id": str(job_id),
                        "step_name": name,
                        "detection_type": item["type"],
                        "script_path": item["script_path"],
                        "approx_line_in_run": item["line"],
                    })
    # If nothing found via structured parse, also do a raw text sweep as a safety net
    if not rows:
        rows.extend(scan_raw_text(path))
    return rows


def scan_raw_text(path: Path) -> list[dict]:
    """
    Fallback: scan the raw file text for .py occurrences and try to classify.
    Less precise (no job/step context), but better than missing items.
    """
    rows = []
    text = path.read_text(encoding="utf-8", errors="ignore")
    for idx, line in enumerate(text.splitlines(), start=1):
        # classify referenced/written if possible
        ref_hits = list(PY_REF_RE.finditer(line))
        write_hits = list(PY_WRITE_RE.finditer(line))
        if ref_hits or write_hits:
            for m in ref_hits:
                rows.append({
                    "workflow_file": str(path.relative_to(ROOT)),
                    "job_id": "",
                    "step_name": "",
                    "detection_type": "referenced",
                    "script_path": m.group("path"),
                    "approx_line_in_run": idx,
                })
            for m in write_hits:
                rows.append({
                    "workflow_file": str(path.relative_to(ROOT)),
                    "job_id": "",
                    "step_name": "",
                    "detection_type": "written",
                    "script_path": m.group("path"),
                    "approx_line_in_run": idx,
                })
        else:
            # generic .py fallback
            for m in PY_ANY_RE.finditer(line):
                rows.append({
                    "workflow_file": str(path.relative_to(ROOT)),
                    "job_id": "",
                    "step_name": "",
                    "detection_type": "unknown",
                    "script_path": m.group("path"),
                    "approx_line_in_run": idx,
                })
    return rows


def main() -> int:
    if not WF_DIR.exists():
        print(f"ERROR: {WF_DIR} not found. Run from repo root.", file=sys.stderr)
        return 1

    ymls = sorted([p for p in WF_DIR.glob("*.yml")] + [p for p in WF_DIR.glob("*.yaml")])
    if not ymls:
        print("No workflow files found in .github/workflows/*.yml|*.yaml")
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        # still write empty outputs
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["workflow_file", "job_id", "step_name", "detection_type", "script_path", "approx_line_in_run"])
        SUMMARY_PATH.write_text("No workflows found.\n", encoding="utf-8")
        return 0

    all_rows = []
    for wf in ymls:
        all_rows.extend(scan_yaml_workflow(wf))

    # Normalize & de-duplicate rows
    norm_rows = []
    seen = set()
    for r in all_rows:
        key = (
            r.get("workflow_file", ""),
            r.get("job_id", ""),
            r.get("step_name", ""),
            r.get("detection_type", ""),
            r.get("script_path", ""),
            int(r.get("approx_line_in_run") or 0),
        )
        if key in seen:
            continue
        seen.add(key)
        norm_rows.append(r)

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    # Write CSV
    headers = ["workflow_file", "job_id", "step_name", "detection_type", "script_path", "approx_line_in_run"]
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in sorted(norm_rows, key=lambda x: (x["workflow_file"], x["job_id"], x["step_name"], x["script_path"], x["approx_line_in_run"])):
            writer.writerow(r)

    # Write summary
    total = len(norm_rows)
    by_type = {"written": 0, "referenced": 0, "unknown": 0}
    unique_scripts = set(r["script_path"] for r in norm_rows)
    for r in norm_rows:
        by_type[r["detection_type"]] = by_type.get(r["detection_type"], 0) + 1

    lines = [
        "Workflow Python File Audit\n",
        f"Workflows scanned: {len(ymls)}",
        f"Detections (rows): {total}",
        f"  - referenced: {by_type.get('referenced', 0)}",
        f"  - written:    {by_type.get('written', 0)}",
        f"  - unknown:    {by_type.get('unknown', 0)}",
        f"Unique .py paths: {len(unique_scripts)}",
        "",
        "Unique script paths:",
    ]
    for p in sorted(unique_scripts):
        lines.append(f"  - {p}")
    lines.append("")
    lines.append(f"Details CSV: {CSV_PATH.relative_to(ROOT)}")

    SUMMARY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote: {CSV_PATH}")
    print(f"Wrote: {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
