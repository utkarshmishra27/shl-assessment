#!/usr/bin/env python3
"""
recommend_api_fixed.py

FastAPI app implementing the exact API contract from Appendix 2 of the assignment.
- GET  /health
- POST /recommend

Expect data/processed/shl_catalog.csv to exist (run convert_and_summary.py first).
"""
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import re

# Path to processed CSV produced by your conversion step
CSV_PATH = Path("data/processed/shl_catalog.csv")
ASSIGNMENT_PDF_FILE = Path("/mnt/data/SHL AI Intern RE Generative AI assignment Updated(1).pdf")

app = FastAPI(title="SHL Catalog Recommender (Appendix-2 compatible)")
ALLOWED_ORIGINS = [
    "http://localhost:3000",   # local React or static test
    "http://127.0.0.1:3000",
    "http://localhost:63342/shl/index.html?_ijt=2mtlun21a85k55dr5bhu0eot2u&_ij_reload=RELOAD_ON_SAVE",
    "https://utkarshmishra27-shl-assessment.vercel.app/",  # replace after deploy
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RecommendRequest(BaseModel):
    query: str
    top_k: Optional[int] = 5

def normalize_yes_no(val: Any) -> str:
    """
    Normalize various textual representations to "Yes" or "No".
    Empty/unknown -> "No".
    """
    if val is None:
        return "No"
    s = str(val).strip().lower()
    if not s or s in ("n/a", "none", "no", "false", "0"):
        return "No"
    # treat presence of yes/true/available/supported as Yes
    if any(tok in s for tok in ("yes", "true", "available", "supported", "y", "1", "able")):
        return "Yes"
    return "No"

def normalize_test_types(tt) -> List[str]:
    if pd.isna(tt) or tt is None:
        return []
    if isinstance(tt, (list, tuple)):
        return [str(x).strip() for x in tt if str(x).strip()]
    s = str(tt).strip()
    if not s:
        return []
    # common separators
    parts = re.split(r"[;,/|]+", s)
    # also split on double spaces or " and "
    final = []
    for p in parts:
        for q in re.split(r"\band\b", p, flags=re.I):
            q = q.strip()
            if q:
                final.append(q)
    # dedupe preserving order
    seen = set()
    out = []
    for p in final:
        if p.lower() not in seen:
            seen.add(p.lower())
            out.append(p)
    return out

# Load data and build TF-IDF at startup
if not CSV_PATH.exists():
    raise SystemExit(f"CSV not found: {CSV_PATH} — run convert_and_summary.py or the JSON->CSV converter first")

df = pd.read_csv(CSV_PATH).fillna("")

# Normalize key fields and create the document text used for retrieval
def build_records(df):
    records = []
    for _, row in df.iterrows():
        url = str(row.get("url","")).strip()
        name = str(row.get("name","")).strip() or ""
        short_desc = str(row.get("short_description","")).strip() if "short_description" in row.index else ""
        full_text = str(row.get("full_text","")).strip() if "full_text" in row.index else ""
        description = short_desc or full_text or ""
        # duration: try to parse integer
        duration_val = row.get("duration_minutes", "") if "duration_minutes" in row.index else row.get("duration","")
        try:
            duration = int(float(duration_val)) if str(duration_val).strip() else 0
        except Exception:
            # try to extract digits
            m = re.search(r"(\d+)", str(duration_val))
            duration = int(m.group(1)) if m else 0
        adaptive_support = normalize_yes_no(row.get("adaptive_support", row.get("adaptive", "")))
        remote_support = normalize_yes_no(row.get("remote_support", row.get("remote", "")))
        test_type = normalize_test_types(row.get("test_type", row.get("test_types", row.get("category", ""))))
        doc = (name + " " + description)[:200000]
        records.append({
            "url": url,
            "name": name,
            "adaptive_support": adaptive_support,
            "description": description,
            "duration": int(duration),
            "remote_support": remote_support,
            "test_type": test_type,
            "__doc__": doc
        })
    return records

records = build_records(df)
docs = [r["__doc__"] for r in records]

# Build TF-IDF vectorizer as baseline retrieval
vectorizer = TfidfVectorizer(max_features=50000, stop_words="english")
if len(docs) == 0:
    X = None
else:
    X = vectorizer.fit_transform(docs)

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "n_items": len(records),
        "assignment_pdf_url": f"file://{ASSIGNMENT_PDF_FILE}"
    }

@app.post("/recommend")
def recommend(req: RecommendRequest):
    q = req.query
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="query must be non-empty")
    top_k = req.top_k or 5
    if top_k < 1:
        top_k = 1
    if top_k > 10:
        top_k = 10

    # If no index built, return best-effort empty
    if X is None:
        # fallback: return first top_k records
        out = []
        for r in records[:top_k]:
            out.append({
                "url": r["url"],
                "name": r["name"],
                "adaptive_support": r["adaptive_support"],
                "description": r["description"],
                "duration": r["duration"],
                "remote_support": r["remote_support"],
                "test_type": r["test_type"]
            })
        return {"recommended_assessments": out}

    qv = vectorizer.transform([q])
    sims = linear_kernel(qv, X).flatten()
    # rank indices by similarity
    idxs = np.argsort(-sims)[: top_k]
    out = []
    for i in idxs:
        r = records[int(i)]
        out.append({
            "url": r["url"],
            "name": r["name"],
            # ensure exact "Yes"/"No"
            "adaptive_support": r["adaptive_support"],
            "description": r["description"],
            "duration": int(r["duration"]),
            "remote_support": r["remote_support"],
            "test_type": r["test_type"]
        })
    return {"recommended_assessments": out}
