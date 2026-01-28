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
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed" / "DAM2026"

RAW_DIR = DATA_DIR / "raw"
OUT_DIR = DATA_DIR / "DAM"

DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"[DEBUG] Script: {Path(__file__).resolve()}")
print(f"[DEBUG] PROJECT_ROOT: {PROJECT_ROOT}")
print(f"[DEBUG] DATA_DIR: {DATA_DIR}")
print(f"[DEBUG] RAW_DIR: {RAW_DIR}")
print(f"[DEBUG] OUT_DIR: {OUT_DIR}")
print(f"[DEBUG] RAW_DIR exists: {RAW_DIR.exists()}")
print(f"[DEBUG] OUT_DIR exists: {OUT_DIR.exists()}")

DATE_FROM = "20251001"
DATE_TO   = "20260127"

DOC_BASE = "https://www.enexgroup.gr/documents/20126/366820"
MAX_V = 20

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

def build_url(yyyymmdd: str, v: int) -> str:
    vv = f"{v:02d}"
    fname = f"{yyyymmdd}_EL-DAM_ResultsSummary_EN_v{vv}.xlsx"
    return f"{DOC_BASE}/{fname}"

def url_exists_xlsx(url: str, timeout=30) -> bool:
    # HEAD can be blocked; use GET stream and check first 2 bytes ("PK")
    try:
        r = SESSION.get(url, timeout=timeout, stream=True, allow_redirects=True)
        if r.status_code == 200:
            head = r.raw.read(2)
            r.close()
            return head == b"PK"
        r.close()
        return False
    except requests.RequestException:
        return False

def download_url(url: str, out_path: Path, timeout=120):
    r = SESSION.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    out_path.write_bytes(r.content)

def find_best_version_url(yyyymmdd: str):
    best_url = None
    best_fname = None
    for v in range(1, MAX_V + 1):
        url = build_url(yyyymmdd, v)
        if url_exists_xlsx(url):
            best_url = url
            best_fname = url.split("/")[-1]
            print(f"[DEBUG] Found version {v} for {yyyymmdd}: {best_fname}")
        else:
            if best_url is not None:
                print(f"[DEBUG] Version {v} not found for {yyyymmdd}, stopping search")
                break
    if best_url is None:
        print(f"[DEBUG] No versions found for {yyyymmdd}")
    return best_url, best_fname

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
# PARSE DAM ResultsSummary
# =========================
def load_dam_results_summary(path: Path) -> pd.DataFrame:
    """
    Load MCP from EL-DAM ResultsSummary Excel files.
    
    Structure:
    - Sheet: "MKT_Coupling"
    - Row 2 (idx=2): hour numbers (1-24, starting from col 1)
    - Row 7 (idx=7): "Greece Mainland  (15min MCP)" with MCP values
    
    Returns: DataFrame with DDAY, DELIVERY_MTU, DAM_MCP
    """
    # Read raw data without header
    df_raw = pd.read_excel(path, sheet_name="MKT_Coupling", engine="openpyxl", header=None)
    
    # Extract date from filename
    m = re.match(r"(\d{8})_EL-DAM_ResultsSummary_EN_v\d+\.xlsx", path.name)
    if not m:
        raise ValueError(f"Cannot infer date from filename: {path.name}")
    dday = pd.to_datetime(m.group(1), format="%Y%m%d")
    
    # Row 2 contains hour numbers, row 7 contains MCP values
    hours_row = df_raw.iloc[2, 1:].dropna()  # Skip col 0 (label), start from col 1
    mcp_row = df_raw.iloc[7, 1:].dropna()    # Skip col 0 (label), start from col 1
    
    if len(hours_row) == 0 or len(mcp_row) == 0:
        raise ValueError(f"Could not find hours or MCP data in sheet")
    
    # Align lengths (use minimum length)
    n = min(len(hours_row), len(mcp_row))
    hours = pd.to_numeric(hours_row.iloc[:n], errors="coerce")
    mcps = pd.to_numeric(mcp_row.iloc[:n], errors="coerce")
    
    # Convert hour (1-24) to 0-23 for timedelta
    h0 = (hours - 1).fillna(0).astype("Int64")
    
    out = pd.DataFrame()
    out["DDAY"] = dday
    out["HOUR"] = h0
    out["DELIVERY_MTU"] = out["DDAY"] + pd.to_timedelta(out["HOUR"], unit="h")
    out["DAM_MCP"] = mcps
    
    return out[["DDAY", "DELIVERY_MTU", "DAM_MCP"]]

# =========================
# PIPELINE
# =========================
def main():
    frames = []
    found_days = 0
    missing_days = 0
    
    print(f"\n[DEBUG] Starting DAM pipeline...")
    print(f"[DEBUG] Date range: {DATE_FROM} to {DATE_TO}")

    for yyyymmdd in daterange_yyyymmdd(DATE_FROM, DATE_TO):
        url, fname = find_best_version_url(yyyymmdd)
        if not url:
            print(f"[DEBUG] Skipping {yyyymmdd} - no URL found")
            missing_days += 1
            continue

        found_days += 1
        raw_path = RAW_DIR / fname
        
        print(f"[DEBUG] Processing {yyyymmdd}: {fname}")

        if not raw_path.exists() or raw_path.read_bytes()[:2] != b"PK":
            print(f"[DAM] Download {fname}")
            try:
                download_url(url, raw_path)
                print(f"[DEBUG] Downloaded successfully: {raw_path}")
            except Exception as e:
                print(f"[ERROR] Failed to download {fname}: {e}", file=sys.stderr)
                continue

        try:
            day_df = load_dam_results_summary(raw_path)
            print(f"[DEBUG] Parsed {len(day_df)} rows from {fname}")
            day_df["SOURCE_FILE"] = raw_path.name
            frames.append(day_df)
        except Exception as e:
            print(f"[DAM] ERROR parsing {raw_path.name}: {e}", file=sys.stderr)

    print(f"\n[DEBUG] Summary: Found {found_days} days | Missing {missing_days} days")
    print(f"[DEBUG] Successfully parsed {len(frames)} files")

    if not frames:
        print("[DAM] Nothing parsed successfully.")
        return

    full = (
        pd.concat(frames, ignore_index=True)
          .sort_values(["DDAY", "DELIVERY_MTU"])
          .reset_index(drop=True)
    )
    
    print(f"[DEBUG] Final dataset: {len(full)} rows")
    print(f"[DEBUG] Date range in data: {full['DDAY'].min()} to {full['DDAY'].max()}")

    out_base = OUT_DIR / f"EL-DAM_MASTER_{DATE_FROM}_{DATE_TO}"
    print(f"[DEBUG] Writing outputs to: {OUT_DIR}")
    write_outputs(full, out_base)

if __name__ == "__main__":
    main()
