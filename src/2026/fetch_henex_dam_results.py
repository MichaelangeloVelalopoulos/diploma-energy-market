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
PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUT_DIR = PROJECT_ROOT / "data" / "processed" / "DAM"
RAW_DIR = OUT_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

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
        else:
            if best_url is not None:
                break
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
    Robust loader:
    - reads first sheet by default
    - tries to locate the MCP column and an hour/time column
    - returns standardized columns: DDAY, DELIVERY_MTU, DAM_MCP
    """
    xls = pd.ExcelFile(path, engine="openpyxl")
    # Prefer a sheet that contains "Summary" if present
    sheet = xls.sheet_names[0]
    for s in xls.sheet_names:
        if "summary" in s.lower():
            sheet = s
            break

    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    # Find MCP column (case-insensitive contains "MCP")
    mcp_candidates = [c for c in df.columns if "mcp" in c.lower()]
    if not mcp_candidates:
        raise ValueError(f"No MCP-like column found in DAM summary. Columns: {df.columns.tolist()[:30]}")
    mcp_col = mcp_candidates[0]

    # Find hour / time column
    # Common possibilities: "DELIVERY_MTU", "Delivery Hour", "Hour", "Period", etc.
    time_candidates = [c for c in df.columns if any(k in c.lower() for k in ["delivery_mtu", "hour", "period", "mtu", "time"])]
    time_col = time_candidates[0] if time_candidates else None

    # Build DDAY from filename (safer)
    m = re.match(r"(\d{8})_EL-DAM_ResultsSummary_EN_v\d+\.xlsx", path.name)
    if not m:
        raise ValueError(f"Cannot infer date from filename: {path.name}")
    dday = pd.to_datetime(m.group(1), format="%Y%m%d")

    out = pd.DataFrame()
    out["DDAY"] = dday

    if time_col is None:
        # If no time column, we assume 24 rows in order and create hour index
        out = pd.concat([out] * len(df), ignore_index=True)
        out["HOUR"] = range(len(df))
        out["DELIVERY_MTU"] = out["DDAY"] + pd.to_timedelta(out["HOUR"], unit="h")
    else:
        t = df[time_col]

        # If it's already datetime -> use it
        if pd.api.types.is_datetime64_any_dtype(t):
            out = pd.DataFrame({"DDAY": [dday] * len(df)})
            out["DELIVERY_MTU"] = pd.to_datetime(t, errors="coerce")
        else:
            # Try to parse hour numbers like 1..24 or 0..23
            hour = pd.to_numeric(t, errors="coerce")
            out = pd.DataFrame({"DDAY": [dday] * len(df)})
            if hour.notna().sum() >= max(10, len(df) // 2):
                # if 1..24 convert to 0..23
                h = hour.astype("Int64")
                h0 = h - 1 if h.max() == 24 else h
                out["HOUR"] = h0
                out["DELIVERY_MTU"] = out["DDAY"] + pd.to_timedelta(out["HOUR"].fillna(0), unit="h")
            else:
                # last resort: try parse as string time
                out["DELIVERY_MTU"] = pd.to_datetime(t.astype(str), errors="coerce")

    out["DAM_MCP"] = pd.to_numeric(df[mcp_col], errors="coerce")
    return out[["DDAY", "DELIVERY_MTU", "DAM_MCP"]]

# =========================
# PIPELINE
# =========================
def main():
    frames = []
    found_days = 0
    missing_days = 0

    for yyyymmdd in daterange_yyyymmdd(DATE_FROM, DATE_TO):
        url, fname = find_best_version_url(yyyymmdd)
        if not url:
            missing_days += 1
            continue

        found_days += 1
        raw_path = RAW_DIR / fname

        if not raw_path.exists() or raw_path.read_bytes()[:2] != b"PK":
            print(f"[DAM] Download {fname}")
            download_url(url, raw_path)

        try:
            day_df = load_dam_results_summary(raw_path)
            day_df["SOURCE_FILE"] = raw_path.name
            frames.append(day_df)
        except Exception as e:
            print(f"[DAM] ERROR parsing {raw_path.name}: {e}", file=sys.stderr)

    print(f"[DAM] Days found: {found_days} | missing: {missing_days}")

    if not frames:
        print("[DAM] Nothing parsed successfully.")
        return

    full = (
        pd.concat(frames, ignore_index=True)
          .sort_values(["DDAY", "DELIVERY_MTU"])
          .reset_index(drop=True)
    )

    out_base = OUT_DIR / f"EL-DAM_MASTER_{DATE_FROM}_{DATE_TO}"
    write_outputs(full, out_base)

if __name__ == "__main__":
    main()
