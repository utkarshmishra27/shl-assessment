import os
import time
import json
import re
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path

import requests
from bs4 import BeautifulSoup
def clean_html(html: str) -> str:
    """Remove scripts, trackers, styles, iframes, NR scripts from HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove harmful/unwanted tags completely
    for tag in soup.find_all(["script", "noscript", "iframe", "style", "link"]):
        tag.decompose()

    # Remove inline javascript (onclick, onhover…) and javascript: links
    for el in soup.find_all():
        # remove javascript: hrefs
        if el.has_attr("href") and str(el["href"]).strip().lower().startswith("javascript:"):
            del el["href"]

        # remove inline event handlers
        for attr in list(el.attrs.keys()):
            if attr.lower().startswith("on"):
                del el.attrs[attr]

    return str(soup)

from tqdm import tqdm

# Optional Playwright rendering
USE_PLAYWRIGHT = False

# START URL - catalog root (adjust if needed)
START_URL = "https://www.shl.com/solutions/products/product-catalog/"

# Path to uploaded assignment PDF (for reference / logging)
ASSIGNMENT_PDF_PATH = "/mnt/data/SHL AI Intern RE Generative AI assignment Updated(1).pdf"

# Output paths
RAW_DIR = Path("data/raw")
PAGES_DIR = RAW_DIR / "shl_product_pages"
OUT_JSON = RAW_DIR / "shl_catalog_raw.json"

# Scraper settings
HEADERS = {
    "User-Agent": "shl-catalog-scraper/1.0 (+https://github.com/your-repo)"
}
REQUEST_DELAY = 1.0  # seconds between requests (be polite)
MAX_PRODUCT_PAGES = None  # set to integer to limit during development

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Utilities
def safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-_. ]", "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:200]

def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PAGES_DIR.mkdir(parents=True, exist_ok=True)

def save_html(page_html: str, filename: Path):
    filename.parent.mkdir(parents=True, exist_ok=True)
    filename.write_text(page_html, encoding="utf-8")

def append_jsonl(path: Path, obj: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# Fetch helpers
def fetch_url_requests(url: str, timeout=30):
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text

# Optional: Playwright render (JS-heavy sites)
def fetch_url_playwright(url: str, timeout=60):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=timeout * 1000)
        page.wait_for_load_state("networkidle", timeout=timeout * 1000)
        content = page.content()
        browser.close()
        return content

def fetch(url: str):
    if USE_PLAYWRIGHT:
        return fetch_url_playwright(url)
    else:
        return fetch_url_requests(url)

# Parsing helpers - heuristic-based, robust to layout changes
def find_product_links_from_catalog(html: str, base_url: str):
    """Return absolute product links found on the catalog/listing page."""
    soup = BeautifulSoup(html, "lxml")
    links = set()
    # Common pattern: links in anchor tags within product lists
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # skip mailto, tel
        if href.startswith("mailto:") or href.startswith("tel:"):
            continue
        # Heuristic: product links often contain '/products/' or '/product/'
        if "/product" in href.lower() or "/solutions/" in href.lower() or "/products/" in href.lower():
            full = urljoin(base_url, href)
            links.add(full)
    return list(links)

def parse_product_page(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")
    # Heuristics to extract title
    title = None
    if soup.find("h1"):
        title = soup.find("h1").get_text(strip=True)
    elif soup.title:
        title = soup.title.get_text(strip=True)

    # description candidates
    description = ""
    # look for meta description
    md = soup.find("meta", {"name": "description"})
    if md and md.get("content"):
        description = md["content"].strip()

    # try common product description selectors
    selectors = [
        {"name": "div", "attrs": {"class": re.compile(r"(product|description|intro|summary)", re.I)}},
        {"name": "section", "attrs": {"class": re.compile(r"(product|description|intro|summary)", re.I)}},
        {"name": "div", "attrs": {"id": re.compile(r"(product|description|intro|summary)", re.I)}},
    ]

    for sel in selectors:
        el = soup.find(sel["name"], sel.get("attrs"))
        if el:
            txt = el.get_text(separator=" ", strip=True)
            if len(txt) > len(description):
                description = txt

    # collect all visible text as full_text (cleaned)
    texts = []
    for p in soup.find_all(["p", "li", "div"]):
        if p.string and p.get_text(strip=True):
            texts.append(p.get_text(" ", strip=True))
    full_text = " ".join(texts)
    if not full_text:
        full_text = description

    # category / type heuristics (try to find labels or breadcrumbs)
    test_type = None
    category = None
    # breadcrumbs
    breadcrumb = soup.find("nav", {"aria-label": re.compile(r"breadcrumb", re.I)})
    if breadcrumb:
        parts = [x.get_text(strip=True) for x in breadcrumb.find_all("a")]
        if parts:
            category = " > ".join(parts)

    # fallback: look for "Test Type" text in page
    for label in soup.find_all(text=re.compile(r"Test Type|Test type|TestType|Type:", re.I)):
        parent = label.parent
        try:
            candidate = parent.get_text(" ", strip=True)
            if len(candidate) > 3:
                test_type = candidate
                break
        except Exception:
            pass

    return {
        "assessment_name": title or "",
        "url": url,
        "category": category or "",
        "test_type": test_type or "",
        "short_description": description or "",
        "full_text": full_text[:20000],  # truncate for safety
    }

def is_prepackaged(text: str) -> bool:
    if not text:
        return False
    return "pre-packaged" in text.lower() or "pre packaged" in text.lower() or "prepackaged" in text.lower()

def crawl(start_url: str, max_products=None):
    logging.info("Starting crawl from %s", start_url)
    seen_product_urls = set()
    queued = [start_url]
    discovered_products = []

    # load existing output to avoid duplicates
    if OUT_JSON.exists():
        logging.info("Found existing output %s, loading seen URLs to avoid duplication", OUT_JSON)
        with OUT_JSON.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    seen_product_urls.add(obj.get("url"))
                except Exception:
                    continue

    processed_pages = set()
    while queued:
        page_url = queued.pop(0)
        if page_url in processed_pages:
            continue
        try:
            html = fetch(page_url)
        except Exception as e:
            logging.warning("Failed to fetch %s : %s", page_url, e)
            continue
        processed_pages.add(page_url)
        # find product links on this page
        links = find_product_links_from_catalog(html, page_url)
        for link in links:
            # Normalize link: remove fragments
            normalized = link.split("#")[0].rstrip("/")
            if normalized.lower().endswith(".pdf"):
                continue
            # Basic filter: same domain only
            parsed = urlparse(normalized)
            if parsed.netloc and "shl.com" not in parsed.netloc:
                continue
            if normalized in seen_product_urls:
                continue
            # Heuristic: skip list pages that look like pagination or duplicate category index
            if re.search(r"/page/|/tag/|/category/|/blog/", normalized, re.I):
                # still queue them to find more product pages if they are catalog pages
                if normalized not in queued and normalized not in processed_pages:
                    queued.append(normalized)
                continue
            # Now fetch product page and parse
            try:
                prod_html = fetch(normalized)
                time.sleep(REQUEST_DELAY)
                prod_html = clean_html(prod_html)
            except Exception as e:
                logging.warning("Failed to fetch product page %s: %s", normalized, e)
                continue

            # parse product info
            parsed_obj = parse_product_page(prod_html, normalized)

            # ignore "Pre-packaged Job Solutions" or similar categories
            combined_text = " ".join([parsed_obj.get("assessment_name",""), parsed_obj.get("category",""), parsed_obj.get("short_description","")])
            if is_prepackaged(combined_text):
                logging.info("Skipping pre-packaged job solution: %s", parsed_obj.get("assessment_name"))
                # mark as seen to avoid refetch in future
                seen_product_urls.add(normalized)
                continue

            # Save HTML snapshot
            safe_id = safe_filename(parsed_obj.get("assessment_name") or normalized)
            page_file = PAGES_DIR / f"{safe_id}.html"
            if not page_file.exists():
                try:
                    save_html(prod_html, page_file)
                except Exception:
                    pass

            # Write to JSONL output
            append_jsonl(OUT_JSON, parsed_obj)
            seen_product_urls.add(normalized)
            discovered_products.append(parsed_obj)

            logging.info("Saved product: %s", parsed_obj.get("assessment_name"))
            # Stop if reach max_products (developer/testing)
            if max_products and len(discovered_products) >= max_products:
                logging.info("Reached max_products=%s, stopping", max_products)
                return discovered_products

        # find next pages to queue: simple approach - find anchors that look like catalog pages
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("mailto:") or href.startswith("tel:"):
                continue
            full = urljoin(page_url, href)
            if full in processed_pages or full in queued:
                continue
            # queue more catalog/listing pages heuristically
            if "/solutions/" in full.lower() or "/products/" in full.lower() or re.search(r"page=\d+|page/\d+", full, re.I):
                queued.append(full)
        # politeness delay
        time.sleep(REQUEST_DELAY)

    return discovered_products


def main():
    ensure_dirs()
    logging.info("Assignment file available at: %s", ASSIGNMENT_PDF_PATH)
    logging.info("Starting scraper. START_URL=%s", START_URL)
    products = crawl(START_URL, max_products=MAX_PRODUCT_PAGES)
    logging.info("Crawl finished. Found %d new products (this run).", len(products))

    # Count total items saved in JSONL
    total = 0
    if OUT_JSON.exists():
        with OUT_JSON.open("r", encoding="utf-8") as f:
            for _ in f:
                total += 1
    logging.info("Total products in %s = %d", OUT_JSON, total)
    print(f"Scrape complete. Total items in output: {total}")

    if total < 377:
        logging.warning("Total products found (%d) < 377. You need to ensure you crawled all individual test solutions. Consider setting USE_PLAYWRIGHT=True if the site is JS heavy or increase crawling depth.", total)
    else:
        logging.info("Requirement satisfied: >= 377 individual test solutions found.")

if __name__ == "__main__":
    main()