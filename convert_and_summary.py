#!/usr/bin/env python3
"""
convert_and_summary.py
- Reads data/raw/shl_catalog_raw.jsonl (one JSON object per line)
- Normalizes fields, deduplicates, writes data/processed/shl_catalog.csv
- Prints a small summary (counts, top categories, missing-field counts)
"""

import json, csv, collections, math
from pathlib import Path

RAW_JSONL = Path("data/raw/shl_catalog_raw.json")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "shl_catalog.csv"

def normalize(item):
    # canonical fieldnames used in previous scripts
    out = {}
    out["name"] = item.get("assessment_name") or item.get("name") or item.get("title") or ""
    out["url"] = item.get("url", "")
    out["category"] = item.get("category") or item.get("categories") or ""
    # flatten test_type list to semicolon-separated string
    tt = item.get("test_type") or item.get("testTypes") or item.get("test_type_list") or item.get("test_type", "")
    if isinstance(tt, list):
        out["test_type"] = ";".join([str(x).strip() for x in tt if x])
    else:
        out["test_type"] = str(tt or "")
    out["short_description"] = item.get("short_description") or item.get("description") or item.get("summary") or ""
    # use full_text if available else copy short_description
    out["full_text"] = item.get("full_text") or item.get("description") or out["short_description"]
    # Keep any other metadata if present
    out["adaptive_support"] = item.get("adaptive_support") or item.get("adaptive") or ""
    out["remote_support"] = item.get("remote_support") or item.get("remote") or ""
    out["duration_minutes"] = item.get("duration_minutes") or item.get("duration") or ""
    return out

def main():
    if not RAW_JSONL.exists():
        print("ERROR: expected input:", RAW_JSONL)
        return
    rows = []
    seen_urls = set()
    with RAW_JSONL.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            row = normalize(obj)
            # dedupe by URL or name+category
            key = row.get("url") or (row.get("name") + "|" + row.get("category"))
            if key in seen_urls:
                continue
            seen_urls.add(key)
            rows.append(row)

    # write CSV
    fieldnames = ["name","url","category","test_type","adaptive_support","remote_support","duration_minutes","short_description","full_text"]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            # ensure strings
            for k in fieldnames:
                if r.get(k) is None:
                    r[k] = ""
            w.writerow({k: r[k] for k in fieldnames})

    # quick summary
    total = len(rows)
    cats = collections.Counter()
    missing = collections.Counter()
    for r in rows:
        cats.update([r.get("category") or "UNKNOWN"])
        if not r.get("short_description"):
            missing["short_description"] += 1
        if not r.get("test_type"):
            missing["test_type"] += 1
        if not r.get("duration_minutes"):
            missing["duration_minutes"] += 1

    print("Wrote:", OUT_CSV)
    print("Total unique rows:", total)
    print("Top categories (top 10):")
    for cat, cnt in cats.most_common(10):
        print(f"  {cnt:4d}  {cat}")
    print("Missing fields counts:")
    for k, v in missing.items():
        print(f"  {k:20s}: {v}")

if __name__ == "__main__":
    main()
