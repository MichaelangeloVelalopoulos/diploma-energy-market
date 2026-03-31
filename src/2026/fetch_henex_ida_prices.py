from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = PROJECT_ROOT / "data" / "processed" / "HENEX" / "raw"

DATE_FROM = "20260101"
DATE_TO = "20260330"

DOC_BASE = "https://www.enexgroup.gr/documents/20126"
FOLDERS = {
    "IDA1": "3257249",
    "IDA2": "3257281",
    "IDA3": "3257522",
}

MAX_VERSION = 10
REQUEST_DELAY = 0.8


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; diploma-research/1.0)",
            "Accept": "*/*",
        }
    )
    return session


SESSION = make_session()


def daterange_yyyymmdd(start_yyyymmdd: str, end_yyyymmdd: str):
    start = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def build_url(ida_tag: str, yyyymmdd: str, version: int) -> str:
    folder = FOLDERS[ida_tag]
    return f"{DOC_BASE}/{folder}/{yyyymmdd}_EL-{ida_tag}_Results_EN_v{version:02d}.xlsx"


def url_exists(url: str, timeout: int = 10) -> bool:
    time.sleep(REQUEST_DELAY)
    try:
        response = SESSION.get(url, timeout=timeout, stream=True, allow_redirects=True)
        if response.status_code == 200:
            magic = response.raw.read(2)
            response.close()
            return magic == b"PK"
        response.close()
        return False
    except requests.RequestException:
        return False


def download_url(url: str, destination: Path, timeout: int = 120) -> None:
    time.sleep(REQUEST_DELAY)
    response = SESSION.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    destination.write_bytes(response.content)


def find_best_version_url(ida_tag: str, yyyymmdd: str) -> tuple[str | None, str | None]:
    best_url = None
    best_name = None
    for version in range(1, MAX_VERSION + 1):
        url = build_url(ida_tag, yyyymmdd, version)
        if url_exists(url):
            best_url = url
            best_name = url.rsplit("/", 1)[-1]
        elif best_url is not None:
            break
    return best_url, best_name


def download_market(ida_tag: str) -> None:
    market_dir = RAW_ROOT / ida_tag
    market_dir.mkdir(parents=True, exist_ok=True)

    found_days = 0
    downloaded_days = 0
    cached_days = 0
    missing_days = 0

    print(f"\n[{ida_tag}] target range: {DATE_FROM} -> {DATE_TO}")

    for yyyymmdd in daterange_yyyymmdd(DATE_FROM, DATE_TO):
        url, filename = find_best_version_url(ida_tag, yyyymmdd)
        if not url or not filename:
            missing_days += 1
            continue

        found_days += 1
        destination = market_dir / filename

        if destination.exists() and destination.stat().st_size > 100 and destination.read_bytes()[:2] == b"PK":
            cached_days += 1
            continue

        try:
            print(f"[{ida_tag}] download {filename}")
            download_url(url, destination)
            downloaded_days += 1
        except Exception as exc:
            missing_days += 1
            print(f"[{ida_tag}] failed {filename}: {exc}", file=sys.stderr)

    print(
        f"[{ida_tag}] found={found_days} downloaded={downloaded_days} "
        f"cached={cached_days} missing={missing_days}"
    )


def main() -> None:
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    for ida_tag in ["IDA1", "IDA2", "IDA3"]:
        download_market(ida_tag)


if __name__ == "__main__":
    main()
