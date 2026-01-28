import os
import re
import json
import time
import hashlib
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_RANGE = "https://www.admie.gr/getOperationMarketFilewRange"
BASE_EXACT = "https://www.admie.gr/getOperationMarketFile"

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "admie-downloader/1.0 (research; intraday forecasting)"
    })
    return s

def safe_filename(name: str) -> str:
    name = name.strip().replace("\u200b", "")
    name = re.sub(r"[^\w\-. ()]+", "_", name)
    return name[:180] if len(name) > 180 else name

def infer_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    base = os.path.basename(path) or "download.bin"
    return safe_filename(base)

def find_urls_in_json(obj):
    """
    Robustly extract URLs from unknown JSON schema.
    - Collects any string value that looks like http(s)://...
    - Also checks common keys (url, fileUrl, downloadUrl, path)
    """
    urls = set()
    common_keys = {"url", "fileurl", "downloadurl", "href", "link", "path", "file", "file_path"}

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, str):
                    if v.startswith("http://") or v.startswith("https://"):
                        urls.add(v)
                    elif k and k.lower() in common_keys and (v.startswith("/") or v.startswith("http")):
                        # Sometimes returned as relative path
                        if v.startswith("/"):
                            urls.add("https://www.admie.gr" + v)
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)
        elif isinstance(x, str):
            if x.startswith("http://") or x.startswith("https://"):
                urls.add(x)

    walk(obj)
    return sorted(urls)

def fetch_file_list(session: requests.Session, file_category: str, date_start: str, date_end: str, mode="range"):
    """
    mode:
      - "range" -> getOperationMarketFilewRange (overlapping range)
      - "exact" -> getOperationMarketFile (exact coverage dates)
    """
    base = BASE_RANGE if mode == "range" else BASE_EXACT
    params = {"dateStart": date_start, "dateEnd": date_end, "FileCategory": file_category}
    r = session.get(base, params=params, timeout=30)
    r.raise_for_status()
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Response not JSON. Status={r.status_code}, text head={r.text[:200]}")
    return data

def download_url(session: requests.Session, url: str, out_dir: str, filename: str | None = None) -> str:
    os.makedirs(out_dir, exist_ok=True)

    if filename is None:
        filename = infer_filename_from_url(url)

    out_path = os.path.join(out_dir, filename)
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return out_path  # skip

    with session.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        tmp_path = out_path + ".part"
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        os.replace(tmp_path, out_path)

    return out_path

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

def run_download(
    file_category: str,
    date_start: str,
    date_end: str,
    out_dir: str = "./admie_downloads",
    mode: str = "range",
):
    session = make_session()

    data = fetch_file_list(session, file_category, date_start, date_end, mode=mode)
    urls = find_urls_in_json(data)

    if not urls:
        print("No URLs found. Save the JSON and inspect keys.")
        # Keep the raw response for debugging
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, f"raw_{file_category}_{date_start}_{date_end}.json"), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return

    manifest = {
        "file_category": file_category,
        "date_start": date_start,
        "date_end": date_end,
        "mode": mode,
        "count": len(urls),
        "items": []
    }

    cat_dir = os.path.join(out_dir, safe_filename(file_category))
    os.makedirs(cat_dir, exist_ok=True)

    for i, url in enumerate(urls, 1):
        try:
            path = download_url(session, url, cat_dir)
            item = {
                "url": url,
                "local_path": path,
                "sha256": sha256_file(path),
                "bytes": os.path.getsize(path),
            }
            manifest["items"].append(item)
            print(f"[{i}/{len(urls)}] OK -> {os.path.basename(path)}")
            time.sleep(0.2)  # be polite
        except Exception as e:
            print(f"[{i}/{len(urls)}] FAIL -> {url} | {e}")

    with open(os.path.join(cat_dir, f"manifest_{date_start}_{date_end}_{mode}.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print("Done.")

if __name__ == "__main__":
    OUT_DIR = "./data/processed/admie_downloads"

    date_start = "2024-06-12"
    date_end   = "2024-12-31"

    # Αυτά αντιστοιχούν ΑΚΡΙΒΩΣ στα φίλτρα της σελίδας σου (Balancing Market Settlement / WEEK)
    categories = [
        "imbabe",                   # Activated Balancing Energy and Settlement Prices (IMBABE)
        "balancingcapacityproduct",  # Balancing Capacity per Product
        "balancingenergyproduct",    # Balancing Energy per Product
    ]

    for cat in categories:
        print(f"\n=== Downloading category: {cat} ===")
        run_download(
            file_category=cat,
            date_start=date_start,
            date_end=date_end,
            out_dir=OUT_DIR,
            mode="range",  # σωστό για date range (επικάλυψη)
        )

