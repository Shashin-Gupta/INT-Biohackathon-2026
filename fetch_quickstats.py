#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional


BASE_URL = "https://quickstats.nass.usda.gov/api"
LIMIT = 50000
SPLIT_ORDER = [
    "source_desc",
    "sector_desc",
    "year",
    "group_desc",
    "commodity_desc",
    "statisticcat_desc",
    "short_desc",
    "domain_desc",
    "domaincat_desc",
    "agg_level_desc",
    "state_alpha",
    "county_ansi",
    "class_desc",
    "util_practice_desc",
    "prodn_practice_desc",
    "freq_desc",
    "reference_period_desc",
    "asd_code",
    "region_desc",
    "zip_5",
    "watershed_code",
    "congr_district_code",
]


def load_api_key(env_path: Path) -> str:
    pattern = re.compile(r"^API_KEY\s*=+\s*(.+?)\s*$")
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(line)
        if match:
            return match.group(1).strip().strip("\"'")
    raise RuntimeError(f"API_KEY not found in {env_path}")


def sanitize(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    cleaned = cleaned.strip("._-")
    return cleaned[:80] or "blank"


def stable_key(filters: dict) -> str:
    return "&".join(
        f"{k}={filters[k]}"
        for k in sorted(filters)
    )


class QuickStatsClient:
    def __init__(self, api_key: str, pause: float, timeout: float):
        self.api_key = api_key
        self.pause = pause
        self.timeout = timeout
        self.values_cache = {}
        self.count_cache = {}

    def _request(self, endpoint: str, params: dict, binary: bool = False):
        query = {"key": self.api_key, **params}
        url = f"{BASE_URL}/{endpoint}/?{urllib.parse.urlencode(query)}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{endpoint} failed ({exc.code}): {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"{endpoint} failed: {exc}") from exc
        finally:
            time.sleep(self.pause)
        return payload if binary else payload.decode("utf-8")

    def get_count(self, filters: dict) -> int:
        key = stable_key(filters)
        if key in self.count_cache:
            return self.count_cache[key]
        text = self._request("get_counts", filters)
        payload = json.loads(text)
        count = int(payload["count"])
        self.count_cache[key] = count
        return count

    def get_values(self, filters: dict, param: str):
        key = (stable_key(filters), param)
        if key in self.values_cache:
            return self.values_cache[key]
        payload = json.loads(self._request("get_param_values", {**filters, "param": param}))
        values = payload.get(param, [])
        self.values_cache[key] = values
        return values

    def download_csv(self, filters: dict) -> bytes:
        return self._request("api_GET", {**filters, "format": "CSV"}, binary=True)


def load_manifest(path: Path):
    if not path.exists():
        return {}
    with path.open() as handle:
        return json.load(handle)


def save_manifest(path: Path, manifest: dict):
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def next_split_param(client: QuickStatsClient, filters: dict):
    for param in SPLIT_ORDER:
        if param in filters:
            continue
        values = client.get_values(filters, param)
        if len(values) > 1:
            return param, values
    return None, []


def build_filename(filters: dict, index: int) -> str:
    if not filters:
        return f"chunk_{index:06d}.csv"
    parts = [f"{key}-{sanitize(str(value))}" for key, value in sorted(filters.items())]
    return f"chunk_{index:06d}__{'__'.join(parts)}.csv"


def write_csv(path: Path, content: bytes):
    path.write_bytes(content)
    with path.open(newline="") as handle:
        row_count = sum(1 for _ in csv.reader(handle))
    return max(row_count - 1, 0)


def process_queue(client: QuickStatsClient, out_dir: Path, manifest_path: Path, max_chunks: Optional[int]):
    manifest = load_manifest(manifest_path)
    manifest.setdefault("pending", [{"filters": {}, "depth": 0}])
    manifest.setdefault("completed", [])
    manifest.setdefault("skipped", [])
    manifest.setdefault("failed", [])
    manifest.setdefault("over_limit", [])
    manifest.setdefault("downloaded_rows", 0)
    manifest.setdefault("downloaded_files", 0)

    completed_keys = {
        entry["filter_key"]
        for entry in manifest["completed"]
    }
    skipped_keys = {
        entry["filter_key"]
        for entry in manifest["skipped"]
    }

    chunk_index = manifest["downloaded_files"] + 1
    downloaded_this_run = 0

    while manifest["pending"]:
        item = manifest["pending"].pop(0)
        filters = item["filters"]
        depth = item["depth"]
        filter_key = stable_key(filters)
        if filter_key in completed_keys or filter_key in skipped_keys:
            continue

        try:
            count = client.get_count(filters)
        except Exception as exc:
            manifest["failed"].append({
                "filters": filters,
                "filter_key": filter_key,
                "stage": "count",
                "error": str(exc),
            })
            save_manifest(manifest_path, manifest)
            print(f"[error] count failed for {filters}: {exc}", flush=True)
            continue
        print(f"[count] {count:>8} rows for {filters or {'all_data': True}}", flush=True)

        if count == 0:
            manifest["skipped"].append({
                "filters": filters,
                "filter_key": filter_key,
                "reason": "empty",
            })
            skipped_keys.add(filter_key)
            save_manifest(manifest_path, manifest)
            continue

        if count <= LIMIT:
            filename = build_filename(filters, chunk_index)
            target = out_dir / filename
            try:
                if not target.exists():
                    csv_bytes = client.download_csv(filters)
                    actual_rows = write_csv(target, csv_bytes)
                else:
                    actual_rows = None
            except Exception as exc:
                manifest["failed"].append({
                    "filters": filters,
                    "filter_key": filter_key,
                    "stage": "download",
                    "error": str(exc),
                })
                save_manifest(manifest_path, manifest)
                print(f"[error] download failed for {filters}: {exc}", flush=True)
                continue
            manifest["completed"].append({
                "filters": filters,
                "filter_key": filter_key,
                "count": count,
                "file": filename,
                "actual_rows": actual_rows,
            })
            completed_keys.add(filter_key)
            manifest["downloaded_rows"] += count
            manifest["downloaded_files"] += 1
            downloaded_this_run += 1
            chunk_index += 1
            save_manifest(manifest_path, manifest)
            print(f"[saved] {filename}", flush=True)
            if max_chunks is not None and downloaded_this_run >= max_chunks:
                break
            continue

        try:
            split_param, values = next_split_param(client, filters)
        except Exception as exc:
            manifest["failed"].append({
                "filters": filters,
                "filter_key": filter_key,
                "stage": "split",
                "error": str(exc),
            })
            save_manifest(manifest_path, manifest)
            print(f"[error] split failed for {filters}: {exc}", flush=True)
            continue
        if not split_param:
            manifest["over_limit"].append({
                "filters": filters,
                "filter_key": filter_key,
                "count": count,
            })
            save_manifest(manifest_path, manifest)
            print(f"[warn] unable to split further: {filters}", flush=True)
            continue

        children = []
        for value in values:
            child_filters = dict(filters)
            child_filters[split_param] = value
            children.append({"filters": child_filters, "depth": depth + 1})
        manifest["pending"] = children + manifest["pending"]
        save_manifest(manifest_path, manifest)
        print(f"[split] {split_param} into {len(values)} branches", flush=True)

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Download USDA Quick Stats data into CSV chunks.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--out-dir", default="data/usda_quickstats")
    parser.add_argument("--manifest", default="data/usda_quickstats/manifest.json")
    parser.add_argument("--pause", type=float, default=0.2)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--max-chunks", type=int, default=None)
    args = parser.parse_args()

    env_path = Path(args.env_file)
    out_dir = Path(args.out_dir)
    manifest_path = Path(args.manifest)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    api_key = load_api_key(env_path)
    client = QuickStatsClient(api_key=api_key, pause=args.pause, timeout=args.timeout)
    manifest = process_queue(client, out_dir, manifest_path, args.max_chunks)

    print(
        json.dumps(
            {
                "downloaded_files": manifest["downloaded_files"],
                "downloaded_rows": manifest["downloaded_rows"],
                "pending": len(manifest["pending"]),
                "failed": len(manifest["failed"]),
                "over_limit": len(manifest["over_limit"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
