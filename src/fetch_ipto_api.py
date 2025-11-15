import argparse, os, sys, time, datetime as dt, requests

BASE = "https://www.admie.gr"
LANDING = f"{BASE}/agora/statistika-agonas/synolika-dedomena"
FT_EN   = f"{BASE}/getFiletypeInfoEN"
OP_FILE = f"{BASE}/getOperationMarketFile"
OP_FILE_RANGE = f"{BASE}/getOperationMarketFileRange"

HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": LANDING,
}

def with_session():
    s = requests.Session()
    s.headers.update(HDRS)
    r = s.get(LANDING, timeout=30)  # πάρε cookies
    r.raise_for_status()
    time.sleep(0.5)
    return s

def get_urls(s, category, d1, d2, use_range):
    url = OP_FILE_RANGE if use_range else OP_FILE
    params = {"dateStart": d1, "dateEnd": d2, "FileCategory": category}
    r = s.get(url, params=params, timeout=60)
    if r.status_code == 403:
        raise SystemExit("❌ 403 από IPTO. Άνοιξε μια φορά τη σελίδα στο browser (ίδιο μηχάνημα) και ξανάτρεξε.")
    r.raise_for_status()
    try:
        js = r.json()
    except Exception:
        print("⚠️ Μη-JSON απόκριση:", r.text[:200]); return []
    if isinstance(js, dict) and "data" in js:
        js = js["data"]
    return js or []

def daterange_chunks(start, end, chunk_days):
    cur = start
    while cur <= end:
        cur2 = min(cur + dt.timedelta(days=chunk_days-1), end)
        yield cur, cur2
        cur = cur2 + dt.timedelta(days=1)

def download(s, url, outdir, fname=None):
    os.makedirs(outdir, exist_ok=True)
    if not fname:
        fname = (url.split("/")[-1].split("?")[0] or "file.bin")
    path = os.path.join(outdir, fname)
    with s.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk: f.write(chunk)
    return path

def main():
    ap = argparse.ArgumentParser(description="IPTO bulk downloader (with session & chunking)")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    ap.add_argument("--category", required=True, help="π.χ. RealTimeSCADARES, RealTimeSCADASystemLoad")
    ap.add_argument("--range", action="store_true", help="χρήση getOperationMarketFileRange")
    ap.add_argument("--chunk", type=int, default=31, help="μέρες ανά κομμάτι (default 31)")
    ap.add_argument("--outdir", default="data/raw/ipto")
    args = ap.parse_args()

    s = with_session()
    saved = 0

    d1 = dt.date.fromisoformat(args.date_from)
    d2 = dt.date.fromisoformat(args.date_to)
    for a, b in daterange_chunks(d1, d2, args.chunk):
        a_str, b_str = a.isoformat(), b.isoformat()
        print(f"🔎 {args.category}: {a_str} → {b_str}")
        items = get_urls(s, args.category, a_str, b_str, use_range=args.range)
        if not items and not args.range:
            print("  …no items (retry with --range)")
            continue
        for it in items:
            url = it.get("FileUrl") or it.get("url") or it.get("Link")
            fname = it.get("FileName") or None
            if not url:
                continue
            path = download(s, url, args.outdir, fname)
            print("  ✅", os.path.basename(path))
            saved += 1

    print(f"🎉 Ολοκληρώθηκε: {saved} αρχεία στο {args.outdir}")

if __name__ == "__main__":
    main()
