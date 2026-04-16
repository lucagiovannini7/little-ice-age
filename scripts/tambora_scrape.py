# -*- coding: utf-8 -*-
"""
tambora_scrape.py
Scrapes PERIOD, POSITION, CODING from tambora.org search result pages.
For each URL, iterates pages 1-5 (or fewer if results run out).
Outputs: tambora_scrape.csv  with columns  CITY, PERIOD, POSITION, CODING
"""

import csv
import time
import urllib.parse
import sys

import requests
from bs4 import BeautifulSoup

# ── Configuration ──────────────────────────────────────────────────────────────

URLS = [
    "https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Regensburg&t%5Byb%5D=1551&t%5Bye%5D=1556&s%5Bcn%5D=9173&sort=t.begin&mode=search&page=1", 
    "https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Augsburg&t%5Byb%5D=1560&t%5Bye%5D=1565&s%5Bcn%5D=9173&sort=t.begin&mode=search&page=1",
    "https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=N%C3%BCrnberg&t%5Byb%5D=1561&t%5Bye%5D=1566&s%5Bcn%5D=9173&sort=t.begin&mode=search&page=1",
    
"https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Z%C3%BCrich&t%5Byb%5D=1566&t%5Bye%5D=1571&sort=t.begin&mode=search&page=1", 

"https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Magdeburg&t%5Byb%5D=1611&t%5Bye%5D=1616&sort=t.begin&mode=search&page=1",

"https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Freiberg&t%5Byb%5D=1634&t%5Bye%5D=1639&sort=t.begin&mode=search&page=1",

"https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Jena&t%5Byb%5D=1644&t%5Bye%5D=1649&sort=t.begin&mode=search&page=1",

"https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Leipzig&t%5Byb%5D=1702&t%5Bye%5D=1707&sort=t.begin&mode=search&page=1",

"https://www.tambora.org/index.php/grouping/event/list?g%5Bqt%5D=Ansbach&t%5Byb%5D=1743&t%5Bye%5D=1748&sort=t.begin&mode=search&page=1",

]

MAX_PAGES   = 5      # pages per URL
SLEEP       = 0.6    # seconds between requests (be polite)
RETRIES     = 3      # per-request retry attempts
TIMEOUT     = 20     # seconds
OUTPUT_FILE = "metadata/tambora_scrape.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def extract_city(url: str) -> str:
    """Pull city from g[qt] query parameter."""
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    return qs.get("g[qt]", qs.get("g%5Bqt%5D", ["unknown"]))[0]


def build_page_url(url: str, page: int) -> str:
    """Replace or append page= parameter."""
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page)]
    new_query = urllib.parse.urlencode(
        {k: v[0] for k, v in params.items()}
    )
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


def fetch(url: str) -> str | None:
    """GET url with retries; return HTML text or None on failure."""
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except requests.RequestException as exc:
            print(f"  [attempt {attempt}/{RETRIES}] error: {exc}", file=sys.stderr)
            if attempt < RETRIES:
                time.sleep(2 * attempt)
    return None


def parse_rows(html: str) -> list[tuple[str, str, str]]:
    """
    Parse data rows from the kv-grid-table.
    Returns list of (period, position, coding) tuples.
    Header row has th elements; data rows have td elements only.
    Column layout (confirmed from live th headers):
      td[0]=quote  td[1]=ID  td[2]=Period  td[3]=Position  td[4]=Coding  ...
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="kv-grid-table")
    if not table:
        return []

    results = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue          # skip header row (th-only) and malformed rows
        period   = tds[2].get_text(strip=True)
        position = tds[3].get_text(strip=True)
        coding   = tds[4].get_text(strip=True)
        results.append((period, position, coding))
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    total = 0

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["CITY", "PERIOD", "POSITION", "CODING"])

        for url in URLS:
            city = extract_city(url)
            print(f"\n>> {city}  ({url[:80]}...)")

            url_rows = 0
            for page in range(1, MAX_PAGES + 1):
                page_url = build_page_url(url, page)
                print(f"  page {page}: {page_url}")

                html = fetch(page_url)
                if html is None:
                    print(f"  ✗ skipping page {page} (fetch failed)", file=sys.stderr)
                    break

                rows = parse_rows(html)
                print(f"  -> {len(rows)} rows")

                for period, position, coding in rows:
                    writer.writerow([city, period, position, coding])

                url_rows += len(rows)

                if len(rows) < 20:
                    print(f"  (last page reached at page {page})")
                    break

                if page < MAX_PAGES:
                    time.sleep(SLEEP)

            print(f"  subtotal: {url_rows} rows")
            total += url_rows

    print(f"\nDone -- {total} rows written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
