#!/usr/bin/env python3
"""
Basic exploratory analysis for Quick Stats-style JSONL (one JSON object per line).

Streams the file so multi-GB inputs stay memory-safe. Reports row counts, missing
rates, numeric summaries for YEAR and VALUE, and optional top-value counts for
categorical columns.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


DEFAULT_TOP_COLUMNS = (
    "COMMODITY_DESC",
    "STATE_ALPHA",
    "STATISTICCAT_DESC",
    "AGG_LEVEL_DESC",
    "GROUP_DESC",
)


def parse_year(raw: str) -> int | None:
    raw = raw.strip()
    if not raw:
        return None
    try:
        y = int(raw)
    except ValueError:
        return None
    if 1400 <= y <= 2200:
        return y
    return None


def parse_value_numeric(raw: str) -> float | None:
    """Quick Stats VALUE often uses commas (e.g. '2,236,000') or '(D)' for withheld."""
    raw = raw.strip()
    if not raw or raw in {"(D)", "(H)", "(L)", "(NA)", "(X)", "(Z)"}:
        return None
    cleaned = raw.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def iter_jsonl(path: Path, strict: bool) -> Any:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield line_no, json.loads(line), None
            except json.JSONDecodeError as exc:
                err = f"line {line_no}: {exc}"
                if strict:
                    raise RuntimeError(f"invalid JSON at {err}") from exc
                yield line_no, None, err


def run_eda(
    path: Path,
    max_rows: int | None,
    top_k: int,
    top_columns: tuple[str, ...],
    strict_json: bool,
    progress_every: int | None = None,
) -> dict[str, Any]:
    columns: list[str] | None = None
    row_count = 0
    bad_rows = 0
    json_errors = 0
    empty_per_col: dict[str, int] = {}
    year_min: int | None = None
    year_max: int | None = None
    year_parsed = 0
    value_min: float | None = None
    value_max: float | None = None
    value_sum = 0.0
    value_parsed = 0
    counters: dict[str, Counter[str]] = {c: Counter() for c in top_columns}
    t0 = time.perf_counter() if progress_every else None

    try:
        for _line_no, obj, parse_err in iter_jsonl(path, strict=strict_json):
            if parse_err is not None:
                json_errors += 1
                continue
            if not isinstance(obj, dict):
                bad_rows += 1
                continue
            if columns is None:
                columns = list(obj.keys())
                empty_per_col = {k: 0 for k in columns}
            row_count += 1
            for key in columns:
                v = obj.get(key, "")
                if v is None:
                    empty_per_col[key] += 1
                elif isinstance(v, str) and not v.strip():
                    empty_per_col[key] += 1

            y = parse_year(str(obj.get("YEAR", "")))
            if y is not None:
                year_parsed += 1
                year_min = y if year_min is None else min(year_min, y)
                year_max = y if year_max is None else max(year_max, y)

            vn = parse_value_numeric(str(obj.get("VALUE", "")))
            if vn is not None and math.isfinite(vn):
                value_parsed += 1
                value_sum += vn
                value_min = vn if value_min is None else min(value_min, vn)
                value_max = vn if value_max is None else max(value_max, vn)

            for col in top_columns:
                if col in counters:
                    raw = obj.get(col, "")
                    s = "" if raw is None else str(raw).strip()
                    label = s if s else "(empty)"
                    counters[col][label] += 1

            if progress_every and t0 is not None and row_count % progress_every == 0:
                elapsed = time.perf_counter() - t0
                rate = row_count / elapsed if elapsed > 0 else 0.0
                print(
                    f"[eda] rows={row_count:,}  elapsed={elapsed:,.1f}s  rate={rate:,.0f}/s",
                    flush=True,
                )

            if max_rows is not None and row_count >= max_rows:
                break
    except RuntimeError as exc:
        return {
            "error": str(exc),
            "rows_processed": row_count,
        }

    result: dict[str, Any] = {
        "input": str(path),
        "rows": row_count,
        "bad_rows": bad_rows,
        "json_parse_errors": json_errors,
        "top_k": top_k,
        "columns": columns or [],
        "column_count": len(columns or []),
        "missing": {},
        "year": {
            "parsed_count": year_parsed,
            "min": year_min,
            "max": year_max,
        },
        "value_numeric": {
            "parsed_count": value_parsed,
            "min": value_min,
            "max": value_max,
            "mean": (value_sum / value_parsed) if value_parsed else None,
        },
        "top_values": {},
    }

    if columns and row_count:
        for key in columns:
            miss = empty_per_col.get(key, 0)
            result["missing"][key] = {
                "empty": miss,
                "pct": round(100.0 * miss / row_count, 3),
            }

    for col, ctr in counters.items():
        result["top_values"][col] = ctr.most_common(top_k)

    if progress_every and t0 is not None and "error" not in result:
        elapsed = time.perf_counter() - t0
        rate = row_count / elapsed if elapsed > 0 else 0.0
        print(
            f"[eda] finished  rows={row_count:,}  total_time={elapsed:,.1f}s  avg_rate={rate:,.0f}/s",
            flush=True,
        )

    return result


def print_report(data: dict[str, Any]) -> None:
    if "error" in data:
        print(f"error: {data['error']}", file=sys.stderr)
        print(f"(processed {data.get('rows_processed', 0)} rows before failure)", file=sys.stderr)
        return

    print("=== JSONL EDA ===")
    print(f"file:     {data['input']}")
    print(f"rows:     {data['rows']:,}")
    if data.get("bad_rows"):
        print(f"skipped:  {data['bad_rows']:,} non-object JSON values")
    if data.get("json_parse_errors"):
        print(f"json err: {data['json_parse_errors']:,} lines (use --strict to fail fast)")
    print(f"columns:  {data['column_count']}")
    print()

    y = data["year"]
    print("YEAR (parsed as integer in a reasonable range)")
    print(f"  count: {y['parsed_count']:,}")
    if y["min"] is not None:
        print(f"  min:   {y['min']}")
        print(f"  max:   {y['max']}")
    print()

    v = data["value_numeric"]
    print("VALUE (numeric only: commas stripped, withheld codes ignored)")
    print(f"  count: {v['parsed_count']:,}")
    if v["min"] is not None:
        print(f"  min:   {v['min']}")
        print(f"  max:   {v['max']}")
        print(f"  mean:  {v['mean']}")
    print()

    print("Missing / empty by column (sorted by %; top 25 with any empties)")
    miss = data["missing"]
    ranked = sorted(miss.items(), key=lambda kv: kv[1]["pct"], reverse=True)
    shown = 0
    for key, m in ranked:
        if m["empty"] <= 0:
            continue
        print(f"  {key}: {m['empty']:,} ({m['pct']}%)")
        shown += 1
        if shown >= 25:
            break
    if shown == 0:
        print("  (no empty string values observed)")
    print()

    k = int(data.get("top_k", 10))
    print(f"Top values (up to {k} per column)")
    for col, pairs in data["top_values"].items():
        print(f"  {col}")
        for val, cnt in pairs:
            print(f"    {cnt:>10,}  {val[:80]}{'…' if len(val) > 80 else ''}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Basic EDA for Quick Stats JSONL files.")
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=Path("data/out/crops.jsonl"),
        help="Path to .jsonl file",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Only read the first N rows (for quick tests)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="How many frequent values to show per --top-column",
    )
    parser.add_argument(
        "--top-columns",
        type=str,
        default=",".join(DEFAULT_TOP_COLUMNS),
        help="Comma-separated column names for value counts",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full report as JSON instead of text",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Abort on the first invalid JSON line",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=None,
        metavar="N",
        help="Print progress every N data rows (stderr-friendly for long runs)",
    )
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"error: file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    top_cols = tuple(c.strip() for c in args.top_columns.split(",") if c.strip())
    if not top_cols:
        top_cols = DEFAULT_TOP_COLUMNS

    report = run_eda(
        args.input,
        args.max_rows,
        args.top_k,
        top_cols,
        strict_json=args.strict,
        progress_every=args.progress_every,
    )
    if "error" in report:
        print_report(report)
        sys.exit(1)
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print_report(report)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
