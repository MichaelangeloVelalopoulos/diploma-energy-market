# fetch_henex_idm_scrape.py
# Scrape HEnEx site for IDM (IDA1/IDA2/IDA3) Excel result files.
# Compatible with Python 3.8+

import os
import re
import argparse
from datetime import datetime
from typing import Optional, Set, List
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

# Σελίδες όπου εμφανίζονται τα IDM αρχεία
BASE_PAGES = [
    "https://www.enexgroup.gr/el/dam-idm-archive",
    "https://www.enexgroup.gr/el/markets-publications-el-intraday-market",
]

# Pattern για filenames τύπου:
#   20251120_EL-IDA1_Results_EN_v01.xlsx
FNAME_RE = re.compile(
    r"(?P<date>\d{8})_EL-IDA(?P<auction>\d)_Results_EN_v(?P<ver>\d+)\.xlsx"
)


def parse_date_from_name(filename: str) -> Optional[datetime]:
    """Extract datetime from IDM filename. Return None if not match."""
    m = FNAME_RE.search(filename)
    if not m:
        return None
    return datetime.strptime(m.group("date"), "%Y%m%d")


def fetch_xlsx_links_from_page(url: str, session: requests.Session) -> Set[str]:
    """
    Κατεβάζει μια HTML σελίδα του HEnEx και επιστρέφει όλα τα πλήρη URLs
    για αρχεία .xlsx που περιέχουν 'EL-IDA' στο όνομα.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.enexgroup.gr/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    print(f"🔎 Fetching page: {url}")
    resp = session.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    links: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Πρέπει να περιέχει .xlsx και EL-IDA στο όνομα
        if ".xlsx" not in href:
            continue
        if "EL-IDA" not in href and "EL-IDA" not in href.upper():
            continue

        full_url = urljoin(url, href)
        links.add(full_url)

    print(f"  → Found {len(links)} .xlsx links on this page")
    return links


def download_file(url: str, outdir: str, session: requests.Session, overwrite: bool = False) -> str:
    """
    Download a single file from URL into outdir.
    If file already exists and overwrite=False, it is skipped.
    """
    os.makedirs(outdir, exist_ok=True)
    fname = os.path.basename(urlparse(url).path)
    out_path = os.path.join(outdir, fname)

    if os.path.exists(out_path) and not overwrite:
        print(f"  • already exists: {fname}")
        return out_path

    print(f"  ↓ downloading {fname}")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.enexgroup.gr/",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*",
    }
    resp = session.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    with open(out_path, "wb") as f:
        f.write(resp.content)

    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Scrape HEnEx site for IDM (IDA1/2/3) XLSX result files."
    )
    parser.add_argument(
        "--outdir",
        default="data/raw/henex_idm",
        help="Output folder for .xlsx files",
    )
    parser.add_argument(
        "--start",
        help="Minimum date (YYYY-MM-DD), e.g. 2024-01-01",
        default=None,
    )
    parser.add_argument(
        "--end",
        help="Maximum date (YYYY-MM-DD), e.g. 2024-12-31",
        default=None,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, re-download files even if they already exist",
    )
    parser.add_argument(
        "--extra-page",
        action="append",
        default=[],
        help="Extra HEnEx page URL to scan (can be used multiple times).",
    )

    args = parser.parse_args()

    start_date = datetime.fromisoformat(args.start).date() if args.start else None
    end_date = datetime.fromisoformat(args.end).date() if args.end else None

    pages: List[str] = list(BASE_PAGES) + list(args.extra_page)

    session = requests.Session()

    # 1) Μαζεύουμε ΟΛΑ τα .xlsx links από όλες τις σελίδες
    all_links: Set[str] = set()
    for page in pages:
        all_links |= fetch_xlsx_links_from_page(page, session)

    print(f"\n🌐 Total unique .xlsx links found: {len(all_links)}")

    # 2) Φιλτράρουμε με βάση το filename pattern και τις ημερομηνίες
    filtered_links: List[str] = []
    for link in sorted(all_links):
        fname = os.path.basename(urlparse(link).path)
        d = parse_date_from_name(fname)
        if d is None:
            # μπορεί να είναι άλλο excel, άσχετο με IDM
            continue
        d_date = d.date()

        if start_date and d_date < start_date:
            continue
        if end_date and d_date > end_date:
            continue

        filtered_links.append(link)

    print(f"✅ Links after date/filename filtering: {len(filtered_links)}")

    # 3) Κατεβάζουμε τα αρχεία
    for link in filtered_links:
        download_file(link, args.outdir, session, overwrite=args.overwrite)

    print("\n🎉 Done. Files are in:", args.outdir)


if __name__ == "__main__":
    main()
