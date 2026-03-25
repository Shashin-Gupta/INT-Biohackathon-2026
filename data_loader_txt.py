#!/usr/bin/env python3
"""
Stream USDA Quick Stats-style tab-delimited .txt exports into CSV, JSON Lines, or XML.

The raw files are large (often multi-GB); this module reads row-by-row and never
loads the full dataset into memory.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import xml.sax.saxutils as xml_esc
from pathlib import Path
import re
from typing import Iterator, TextIO


def clean_cell(value: str) -> str:
    return value.replace("\x00", "").strip()


def iter_quickstats_rows(path: Path) -> Iterator[dict[str, str]]:
    """
    Yield one dict per data row. Keys are header names from the first line.
    """
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        try:
            raw_header = next(reader)
        except StopIteration:
            return
        headers = [clean_cell(h) for h in raw_header]
        if not headers or all(not h for h in headers):
            return
        for raw in reader:
            row = [clean_cell(c) for c in raw]
            if len(row) < len(headers):
                row.extend([""] * (len(headers) - len(row)))
            elif len(row) > len(headers):
                row = row[: len(headers)]
            yield dict(zip(headers, row))


def _open_text_out(path: Path | None) -> TextIO:
    if path is None:
        return sys.stdout
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("w", encoding="utf-8", newline="")


def _xml_root_tag(name: str) -> str:
    safe = re.sub(r"[^\w.\-]", "_", name.strip() or "dataset")
    if not re.match(r"^[A-Za-z_]", safe):
        safe = f"_{safe}"
    return safe[:200]


def write_csv(rows: Iterator[dict[str, str]], out: TextIO) -> int:
    writer: csv.DictWriter[str] | None = None
    count = 0
    for row in rows:
        if writer is None:
            fieldnames = list(row.keys())
            writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
        writer.writerow(row)
        count += 1
    return count


def write_jsonl(rows: Iterator[dict[str, str]], out: TextIO) -> int:
    count = 0
    for row in rows:
        out.write(json.dumps(row, ensure_ascii=False) + "\n")
        count += 1
    return count


def write_json_array(rows: Iterator[dict[str, str]], out: TextIO) -> int:
    out.write("[\n")
    count = 0
    first = True
    for row in rows:
        if not first:
            out.write(",\n")
        first = False
        out.write(json.dumps(row, ensure_ascii=False))
        count += 1
    out.write("\n]\n")
    return count


def write_xml(rows: Iterator[dict[str, str]], out: TextIO, root_tag: str = "dataset") -> int:
    tag = _xml_root_tag(root_tag)
    out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    out.write(f"<{tag}>\n")
    count = 0
    for row in rows:
        out.write("  <row>\n")
        for key, val in row.items():
            safe_key = xml_esc.escape(key, {'"': "&quot;"})
            safe_val = xml_esc.escape(val)
            out.write(f'    <field name="{safe_key}">{safe_val}</field>\n')
        out.write("  </row>\n")
        count += 1
    out.write(f"</{tag}>\n")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Quick Stats tab-delimited .txt in data/ to CSV, JSON, or XML (streaming)."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=Path("data/qs.crops_20260325.txt"),
        help="Path to the tab-delimited .txt file",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output file (default: stdout). Parent dirs are created as needed.",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=("csv", "json", "jsonl", "xml"),
        default="csv",
        help="csv: RFC-style CSV; jsonl: one JSON object per line; json: single JSON array (needs RAM for huge files); xml: one <row> per record",
    )
    parser.add_argument(
        "--xml-root",
        default="dataset",
        help="Root element name for XML output",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Stop after this many data rows (for testing)",
    )
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"error: input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    def limited(it: Iterator[dict[str, str]]) -> Iterator[dict[str, str]]:
        if args.max_rows is None:
            yield from it
            return
        n = 0
        for row in it:
            yield row
            n += 1
            if n >= args.max_rows:
                break

    rows = limited(iter_quickstats_rows(args.input))

    if args.format == "json":
        if args.max_rows is None and args.input.stat().st_size > 50 * 1024 * 1024:
            print(
                "error: --format json loads structure in memory; use --format jsonl "
                "or set --max-rows for large files.",
                file=sys.stderr,
            )
            sys.exit(2)
        out = _open_text_out(args.output)
        try:
            n = write_json_array(rows, out)
        finally:
            if args.output is not None:
                out.close()
    elif args.format == "jsonl":
        out = _open_text_out(args.output)
        try:
            n = write_jsonl(rows, out)
        finally:
            if args.output is not None:
                out.close()
    elif args.format == "csv":
        out = _open_text_out(args.output)
        try:
            n = write_csv(rows, out)
        finally:
            if args.output is not None:
                out.close()
    else:
        out = _open_text_out(args.output)
        try:
            n = write_xml(rows, out, root_tag=args.xml_root)
        finally:
            if args.output is not None:
                out.close()

    print(f"wrote {n} rows ({args.format})", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(130)
