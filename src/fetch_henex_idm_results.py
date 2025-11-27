# fetch_henex_idm_results.py
# Full working version — no XML parsing, only regex.
# Supports IDA1 / IDA2 / IDA3.

import os
import re
import argparse
from datetime import datetime
from urllib.parse import urljoin

import requests


RSS_URLS = {
    "IDA1": "https://www.enexgroup.gr/el/web/guest/markets-publications-el-intraday-market/-/asset_publisher/Ibj5yiegpvGr/rss",
    "IDA2": "https://www.enexgroup.gr/el/web/guest/markets-publications-el-intraday-market/-/asset_publisher/Y8yXgbTu2HIv/rss",
    "IDA3": "https://www.enexgroup.gr/el/web/guest/markets-publications-el-intraday-market/-/asset_publisher/h9LM4w9p33nM/rss",
}

FNAME_RE = re.compile(
    r"(?P<date>\d{8})_EL-IDA(?P<auction>\d)_Results_EN_v(?P<ver>\d+)\.xlsx"
)


def parse_date_from_name(filename: str):
    m = FNAME_RE.search(filename)
    if not m:
        return None
    return datetime.strptime(m.group("date"), "%Y%m%d").date()


def extract_entries_from_text(text: str):
    """
    Extract (filename, document_page_url) from the RSS text using regex.
    """
    entries = []

    # Regex for <entry> ... </entry>
    pattern_entry = re.compile(r"<entry>(.*?)</entry>", re.DOTALL)
    for block in pattern_entry.findall(text):

        # title
        m_title = re.search(r"<title>(.*?)</title>", block)
        if not m_title:
            continue
        title = m_title.group(1).strip()

        if not title.endswith(".xlsx"):
            continue

        # link href="..."
        m_link = re.search(r'href="([^"]+document[^"]+)"', block)
        if not m_link:
            continue

        doc_url = m_link.group(1).strip()
        entries.append((title, doc_url))

    return entries


def find_real_xlsx(doc_url: str, expected_fname: str):
    """
    Load document page and extract real XLSX URL.
    """
    r = requests.get(doc_url, timeout=30)
    r.raise_for_status()
    html = r.text

    # Try exact filename
    m = re.search(r'href="([^"]*%s[^"]*)"' % re.escape(expected_fname), html)
    if m:
        return urljoin(doc_url, m.group(1))

    # fallback
    m2 = re.search(r'href="([^"]+\.xlsx)"', html)
    if m2:
        return urljoin(doc_url, m2.group(1))

    return None


def download(url: str, fname: str, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, fname)

    if os.path.exists(path):
        print(f"  • exists: {fname}")
        return

    print(f"  ↓ downloading {fname}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default="data/raw/henex_idm")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    args = parser.parse_args()

    start_date = datetime.fromisoformat(args.start).date() if args.start else None
    end_date = datetime.fromisoformat(args.end).date() if args.end else None

    for ida, url in RSS_URLS.items():
        print(f"\n=== {ida} ===")

        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print("  ⚠️ Cannot fetch RSS:", e)
            continue

        entries = extract_entries_from_text(r.text)
        print(f"  Found {len(entries)} entries")

        for fname, doc_url in entries:
            date_obj = parse_date_from_name(fname)
            if not date_obj:
                continue

            if start_date and date_obj < start_date:
                continue
            if end_date and date_obj > end_date:
                continue

            real_url = find_real_xlsx(doc_url, fname)
            if not real_url:
                print(f"  ❌ XLSX not found for {fname}")
                continue

            download(real_url, fname, args.outdir)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()

