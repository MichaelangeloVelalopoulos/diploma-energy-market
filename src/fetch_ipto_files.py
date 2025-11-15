import os, argparse, requests, pandas as pd

# Βασικά endpoints του IPTO File Download API (επιστρέφουν JSON με λίστες αρχείων)
FILETYPE_INFO = "https://www.admie.gr/getFiletypeInfo"
FILE_URLS      = "https://www.admie.gr/getFileUrls"   # params: fileType, dateFrom, dateTo (YYYY-MM-DD)

# Παραδείγματα FileTypes που σε ενδιαφέρουν:
# - "RealTimeSCADASystemLoad"  -> System Load (15')
# - "GenerationPerFuel"        -> Generation per fuel (συνήθως περιλαμβάνει Wind/PV ανά 15' ή ώρα)
# Θα επιβεβαιώσεις ποια ακριβώς είναι διαθέσιμα από το /getFiletypeInfo.

def list_filetypes():
    r = requests.get(FILETYPE_INFO, timeout=30)
    r.raise_for_status()
    return r.json()

def get_file_urls(filetype: str, date_from: str, date_to: str):
    params = {"fileType": filetype, "dateFrom": date_from, "dateTo": date_to}
    r = requests.get(FILE_URLS, params=params, timeout=60)
    r.raise_for_status()
    js = r.json()
    # Συνήθως επιστρέφει λίστα αντικειμένων με πεδία όπως { "FileName": "...", "FileUrl": "..." }
    return js

def download_one(url: str, out_dir: str, filename: str | None = None):
    os.makedirs(out_dir, exist_ok=True)
    if filename is None:
        filename = url.split("/")[-1].split("?")[0]
    out_path = os.path.join(out_dir, filename)
    with requests.get(url, timeout=120, stream=True) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return out_path

def main():
    ap = argparse.ArgumentParser(description="Fetch IPTO (ADMIE) files via File Download API")
    ap.add_argument("--filetype", required=True, help="π.χ. RealTimeSCADASystemLoad ή GenerationPerFuel")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    ap.add_argument("--outdir", default="data/raw/ipto", help="πού να σωθούν τα αρχεία")
    args = ap.parse_args()

    print("🔎 Λίστα διαθέσιμων FileTypes (sample):")
    try:
        fts = list_filetypes()
        print("…", list(fts)[:10])
    except Exception as _:
        print("⚠️ Δεν μπόρεσα να φέρω /getFiletypeInfo (ίσως rate-limit/403). Συνεχίζω με το filetype που έδωσες.")

    print(f"📥 Αναζήτηση URLs για {args.filetype} {args.date_from}→{args.date_to}")
    urls = get_file_urls(args.filetype, args.date_from, args.date_to)
    if not urls:
        raise SystemExit("✖ Δεν βρέθηκαν αρχεία για το κριτήριο.")

    saved = []
    for item in urls:
        # Συνήθη πεδία: FileUrl, FileName (το schema μπορεί να αλλάζει· εκτύπωσε item αν θες)
        url = item.get("FileUrl") or item.get("url") or item.get("Link") or ""
        name = item.get("FileName") or None
        if not url:
            print("⚠️ Παράλειψη αντικειμένου χωρίς URL:", item)
            continue
        path = download_one(url, args.outdir, name)
        print("✅ Saved:", path)
        saved.append(path)

    print(f"🎉 Ολοκληρώθηκε. Αρχεία: {len(saved)} αποθηκεύτηκαν στο {args.outdir}")

if __name__ == "__main__":
    main()
