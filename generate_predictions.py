#!/usr/bin/env python3
"""
generate_predictions.py

Generates the final prediction CSV in the exact Appendix 3 format:
Columns: Query, Assessment_url

Behavior:
- Loads test queries from one of:
    data/test_queries.xlsx (sheet1, column named 'query')
    data/test_queries.csv (column named 'query')
    test_queries.xlsx / test_queries.csv
  If none found, prompts user to paste queries.
- Calls the /recommend endpoint for each query and collects top_k URLs.
- Writes predictions.csv with rows:
    Query,Assessment_url
    Query1,URL1
    Query1,URL2
    ...
"""
import os
import sys
import json
import csv
import argparse
from pathlib import Path
import requests

try:
    import pandas as pd
except Exception:
    pd = None

DEFAULT_API = os.environ.get("API_URL", "http://127.0.0.1:8000/recommend")
OUT_CSV = Path("predictions.csv")

def load_queries():
    # possible locations
    candidates = [
        Path("data/Gen_AI Dataset.xlsx"),
        Path("data/test_queries.csv"),
        Path("test_queries.xlsx"),
        Path("test_queries.csv"),
    ]
    for p in candidates:
        if p.exists():
            print("Loading queries from:", p)
            if p.suffix.lower() in (".xlsx", ".xls"):
                if pd is None:
                    raise SystemExit("pandas is required to read Excel files. Install with: python -m pip install pandas openpyxl")
                df = pd.read_excel(p, sheet_name=0)
                # try common column names
                for col in ("query","Query","text","Text"):
                    if col in df.columns:
                        return [str(x).strip() for x in df[col].dropna().astype(str).tolist()]
                # otherwise use the first column
                first = df.columns[0]
                return [str(x).strip() for x in df[first].dropna().astype(str).tolist()]
            else:
                # CSV
                if pd is not None:
                    df = pd.read_csv(p)
                    for col in ("query","Query","text","Text"):
                        if col in df.columns:
                            return [str(x).strip() for x in df[col].dropna().astype(str).tolist()]
                    first = df.columns[0]
                    return [str(x).strip() for x in df[first].dropna().astype(str).tolist()]
                else:
                    # fallback: simple CSV parse
                    with p.open("r", encoding="utf-8") as f:
                        rows = [line.strip() for line in f if line.strip()]
                    return rows
    # none found -> interactive input
    print("No test_queries file found. Please paste queries one per line. End with an empty line.")
    queries = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if not line.strip():
            break
        queries.append(line.strip())
    if not queries:
        raise SystemExit("No queries provided. Place a test_queries.csv/xlsx in data/ or run the script interactively and paste queries.")
    return queries

def call_api(api_url, query, top_k=5, timeout=30):
    payload = {"query": query, "top_k": top_k}
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(api_url, json=payload, headers=headers, timeout=timeout)
    except Exception as e:
        raise RuntimeError(f"Failed to call API {api_url}: {e}")
    if r.status_code != 200:
        raise RuntimeError(f"API returned status {r.status_code}: {r.text[:400]}")
    try:
        data = r.json()
    except Exception:
        raise RuntimeError("API returned non-JSON response")
    # Accept either {"recommended_assessments": [...]} or direct list for compatibility
    if isinstance(data, dict) and "recommended_assessments" in data:
        recs = data["recommended_assessments"]
    elif isinstance(data, list):
        recs = data
    else:
        # try common key "results" or "items"
        for key in ("results","items","recommended"):
            if isinstance(data, dict) and key in data:
                recs = data[key]
                break
        else:
            raise RuntimeError("Unexpected API JSON structure: keys = " + ",".join(data.keys() if isinstance(data, dict) else []))
    # each rec must have a url field
    urls = []
    for r in recs:
        if isinstance(r, dict) and r.get("url"):
            urls.append(r["url"])
        elif isinstance(r, str) and r.startswith("http"):
            urls.append(r)
    return urls

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", type=str, default=DEFAULT_API, help="Recommend API URL (default from API_URL env or http://127.0.0.1:8000/recommend)")
    parser.add_argument("--top_k", type=int, default=5, help="Number of recommendations per query (1..10)")
    parser.add_argument("--out", type=str, default=str(OUT_CSV), help="Output CSV path (default: predictions.csv)")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds per API call")
    args = parser.parse_args()

    api_url = args.api
    top_k = args.top_k
    if top_k < 1:
        top_k = 1
    if top_k > 10:
        top_k = 10

    queries = load_queries()
    print(f"Loaded {len(queries)} queries. Calling API: {api_url} (top_k={top_k})")

    out_rows = []
    errors = []
    for q in queries:
        try:
            urls = call_api(api_url, q, top_k=top_k, timeout=args.timeout)
            if not urls:
                print(f"Warning: no results for query: {q!r}")
            for u in urls:
                out_rows.append((q, u))
        except Exception as e:
            errors.append((q, str(e)))
            print(f"ERROR calling API for query {q!r}: {e}")

    # write CSV in Appendix 3 format: header "Query Assessment_url"
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Query","Assessment_url"])
        for q,u in out_rows:
            writer.writerow([q, u])

    print(f"Wrote predictions: {out_path} (rows: {len(out_rows)})")
    if errors:
        print("There were errors for some queries:")
        for q,e in errors:
            print(" -", q, ":", e)
    print("Done.")

if __name__ == '__main__':
    main()
