import re
import sys
import warnings
from pathlib import Path
from datetime import datetime, timedelta

import requests
import pandas as pd

warnings.filterwarnings("ignore", message="Workbook contains no default style")

# =========================
# CONFIG
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

OUT_DIR = PROJECT_ROOT / "data" / "processed" / "henex_ida_results"
RAW_DIR = OUT_DIR / "raw"
PROC_DIR = OUT_DIR / "processed"

OUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

DATE_FROM = "20251001"
DATE_TO = datetime.now().strftime("%Y%m%d")

# You gave these (stable):
DOC_BASE = "https://www.enexgroup.gr/documents/20126"
FOLDERS = {
    "IDA1": "3257249",
    "IDA2": "3257281",
    "IDA3": "3257522",
}

# How many versions to try per day
MAX_V = 20  # tries v01..v20

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; diploma-research/1.0)",
    "Accept": "*/*",
}

SESSION = requests.Session()
SESSION.headers.update(REQUEST_HEADERS)

# =========================
# HELPERS
# =========================
def daterange_yyyymmdd(start_yyyymmdd: str, end_yyyymmdd: str):
    s = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    e = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    d = s
    while d <= e:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)

def build_url(ida_tag: str, yyyymmdd: str, v: int) -> str:
    folder = FOLDERS[ida_tag]
    vv = f"{v:02d}"
    fname = f"{yyyymmdd}_EL-{ida_tag}_Results_EN_v{vv}.xlsx"
    return f"{DOC_BASE}/{folder}/{fname}"

def url_exists(url: str, timeout=30) -> bool:
    # HEAD sometimes blocked; use GET with stream=True and close quickly
    try:
        r = SESSION.get(url, timeout=timeout, stream=True, allow_redirects=True)
        if r.status_code == 200:
            # quick magic check: xlsx is zip => PK
            chunk = r.raw.read(2)
            r.close()
            return chunk == b"PK"
        r.close()
        return False
    except requests.RequestException:
        return False

def download_url(url: str, out_path: Path, timeout=120):
    r = SESSION.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    out_path.write_bytes(r.content)

def find_best_version_url(ida_tag: str, yyyymmdd: str) -> tuple[str, str] | tuple[None, None]:
    """
    Return (url, filename) for the highest v## that exists.
    We probe v01..vMAX_V and keep the max that exists.
    """
    best = None
    best_fname = None
    for v in range(1, MAX_V + 1):
        url = build_url(ida_tag, yyyymmdd, v)
        if url_exists(url):
            best = url
            best_fname = url.split("/")[-1]
        else:
            # Optimization: many days only have v01.
            # Once we fail AFTER we already found one, we can stop.
            if best is not None:
                break
    return best, best_fname

def load_xlsx_results(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]
    return df

def reshape_ida_results(df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "TARGET","BIDDING_ZONE_DESCR","DDAY","DELIVERY_MTU","DELIVERY_DURATION","SORT","PUB_TIME","VER",
        "SIDE_DESCR","ASSET_DESCR","CLASSIFICATION","MCP","TOTAL_TRADES"
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {sorted(missing)}")

    df = df.copy()
    df["DDAY"] = pd.to_datetime(df["DDAY"].astype(str), format="%Y%m%d", errors="coerce")
    df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce")

    # fast SERIES_ID
    side = df["SIDE_DESCR"].astype(str).str.strip()
    asset = df["ASSET_DESCR"].astype(str).str.strip()
    cls = df["CLASSIFICATION"].astype(str).str.strip()
    series = (side + "__" + asset + "__" + cls)
    df["SERIES_ID"] = series.str.replace(r"[^A-Za-z0-9]+", "_", regex=True).str.strip("_")

    meta = ["TARGET","BIDDING_ZONE_DESCR","DDAY","DELIVERY_MTU","DELIVERY_DURATION","SORT","PUB_TIME","VER"]

    agg = (
        df.groupby(meta + ["SERIES_ID"], as_index=False)
          .agg({"MCP": "mean", "TOTAL_TRADES": "sum"})
    )

    wide_mcp = agg.pivot(index=meta, columns="SERIES_ID", values="MCP")
    wide_trd = agg.pivot(index=meta, columns="SERIES_ID", values="TOTAL_TRADES")

    wide_mcp.columns = [f"MCP__{c}" for c in wide_mcp.columns]
    wide_trd.columns = [f"TRADES__{c}" for c in wide_trd.columns]

    return pd.concat([wide_mcp, wide_trd], axis=1).reset_index()

def write_outputs(df: pd.DataFrame, out_base: Path):
    out_csv = out_base.with_suffix(".csv")
    df.to_csv(out_csv, index=False)
    print(f"Write: {out_csv}")

    try:
        out_parquet = out_base.with_suffix(".parquet")
        df.to_parquet(out_parquet, index=False)
        print(f"Write: {out_parquet}")
    except Exception as e:
        print(f"Parquet skipped (install pyarrow): {e}")

# =========================
# PIPELINE
# =========================
def fetch_and_build(ida_tag: str):
    print(f"\n[{ida_tag}] Target range: {DATE_FROM} -> {DATE_TO}")
    ida_raw_dir = RAW_DIR / ida_tag
    ida_raw_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    found_days = 0
    missing_days = 0

    for yyyymmdd in daterange_yyyymmdd(DATE_FROM, DATE_TO):
        url, fname = find_best_version_url(ida_tag, yyyymmdd)
        if not url:
            missing_days += 1
            continue

        found_days += 1
        raw_path = ida_raw_dir / fname

        if not raw_path.exists() or raw_path.read_bytes()[:2] != b"PK":
            print(f"[{ida_tag}] Download {fname}")
            download_url(url, raw_path)

        try:
            df_long = load_xlsx_results(raw_path)
            df_wide = reshape_ida_results(df_long)
            df_wide["SOURCE_FILE"] = raw_path.name
            rows.append(df_wide)
        except Exception as e:
            print(f"[{ida_tag}] ERROR parsing {raw_path.name}: {e}", file=sys.stderr)

    print(f"[{ida_tag}] Days found: {found_days} | missing: {missing_days}")

    if not rows:
        print(f"[{ida_tag}] Nothing parsed successfully.")
        return

    full = (
        pd.concat(rows, ignore_index=True)
          .sort_values(["DDAY", "DELIVERY_MTU", "SORT"])
          .reset_index(drop=True)
    )

    out_base = PROC_DIR / f"EL-{ida_tag}_Results_{DATE_FROM}_{DATE_TO}"
    write_outputs(full, out_base)

def main():
    for ida in ["IDA1", "IDA2", "IDA3"]:
        fetch_and_build(ida)

if __name__ == "__main__":
    main()
