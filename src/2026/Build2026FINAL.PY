import re
from pathlib import Path

import pandas as pd


# =========================
# CONFIG
# =========================
MERGED_IN = Path("data/processed/MERGED/EL-IDA1_WITH_DAM_20251001_20260127.csv")

IMBABE_DIR = Path("data/processed/admie_downloads/imbabe")

OUT_CSV = Path("data/processed/MERGED/EL-IDA1_WITH_DAM_BM_20251001_20260127.csv")
DATE_FROM = "2025-10-01 00:00:00"
DATE_TO   = "2026-01-27 23:45:00"

# IMBABE data usually on first sheet, but we'll prefer the known one if present
PREFERRED_SHEET = "Activated_bal_Energy_and_Prices"


# =========================
# HELPERS
# =========================
def parse_dt_series(s: pd.Series) -> pd.Series:
    """
    Robust parse for mixed formats like:
      12/29/25 0:00
      2025-12-29 00:00:00
    """
    # Try generic parse first
    dt = pd.to_datetime(s, errors="coerce")

    # For stubborn US-format strings (m/d/yy h:mm)
    if dt.isna().any():
        dt2 = pd.to_datetime(s, format="%m/%d/%y %H:%M", errors="coerce")
        dt = dt.fillna(dt2)

    # Normalize to 15-min grid (exact matching key)
    dt = dt.dt.floor("15min")
    return dt


def find_delivery_col(df: pd.DataFrame) -> str:
    cols = {c.lower().strip(): c for c in df.columns}
    if "delivery_mtu" in cols:
        return cols["delivery_mtu"]
    for c in df.columns:
        cl = c.lower()
        if "delivery" in cl and "mtu" in cl:
            return c
    raise ValueError(f"DELIVERY_MTU column not found. Columns={df.columns.tolist()}")


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # remove weird spaces (NBSP etc)
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", str(c).replace("\u00a0", " ").strip()) for c in df.columns]
    return df


def load_all_imbabe(imbabe_dir: Path) -> pd.DataFrame:
    files = sorted(imbabe_dir.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"No .xlsx found in {imbabe_dir}")

    frames = []
    for f in files:
        xl = pd.ExcelFile(f)

        sheet = PREFERRED_SHEET if PREFERRED_SHEET in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
        df = normalize_cols(df)

        # Must have STARTDATE and Imbalance Price
        if "STARTDATE" not in df.columns:
            # sometimes lowercase or with spaces
            start_candidates = [c for c in df.columns if c.strip().lower() == "startdate"]
            if start_candidates:
                df = df.rename(columns={start_candidates[0]: "STARTDATE"})
            else:
                raise ValueError(f"{f.name}: STARTDATE not found. Columns={df.columns.tolist()}")

        price_col = None
        for c in df.columns:
            if "imbalance price" in c.lower():
                price_col = c
                break
        if price_col is None:
            raise ValueError(f"{f.name}: Imbalance Price column not found. Columns={df.columns.tolist()}")

        out = df[["STARTDATE", price_col]].copy()
        out = out.rename(columns={price_col: "BM_IMBALANCE_PRICE"})

        out["DELIVERY_MTU"] = parse_dt_series(out["STARTDATE"])
        out["BM_IMBALANCE_PRICE"] = pd.to_numeric(out["BM_IMBALANCE_PRICE"], errors="coerce")

        out = out.dropna(subset=["DELIVERY_MTU"]).copy()
        frames.append(out[["DELIVERY_MTU", "BM_IMBALANCE_PRICE"]])

    bm = pd.concat(frames, ignore_index=True)
    bm = bm.dropna(subset=["BM_IMBALANCE_PRICE"]).copy()
    bm = bm.sort_values("DELIVERY_MTU").drop_duplicates(subset=["DELIVERY_MTU"], keep="last").reset_index(drop=True)
    return bm


# =========================
# MAIN
# =========================
def main():
    if not MERGED_IN.exists():
        raise FileNotFoundError(f"MERGED_IN not found: {MERGED_IN}")
    if not IMBABE_DIR.exists():
        raise FileNotFoundError(f"IMBABE_DIR not found: {IMBABE_DIR}")

    merged = pd.read_csv(MERGED_IN)
    merged = normalize_cols(merged)

    mtu_col = find_delivery_col(merged)
    merged["DELIVERY_MTU"] = parse_dt_series(merged[mtu_col])
    merged = merged.dropna(subset=["DELIVERY_MTU"]).copy()

    # Load IMBABE
    bm = load_all_imbabe(IMBABE_DIR)

    # Filter to exact target window
    t0 = pd.to_datetime(DATE_FROM)
    t1 = pd.to_datetime(DATE_TO)
    merged = merged[(merged["DELIVERY_MTU"] >= t0) & (merged["DELIVERY_MTU"] <= t1)].copy()
    bm = bm[(bm["DELIVERY_MTU"] >= t0) & (bm["DELIVERY_MTU"] <= t1)].copy()

    # Exact merge on DELIVERY_MTU
    out = merged.merge(bm, on="DELIVERY_MTU", how="left")

    if "DAM_MCP" not in out.columns:
        raise ValueError(f"DAM_MCP not found in merged input. Columns={out.columns.tolist()}")

    out["DAM_MCP"] = pd.to_numeric(out["DAM_MCP"], errors="coerce")
    out["BMmDAMMCP"] = out["BM_IMBALANCE_PRICE"] - out["DAM_MCP"]

    out = out.sort_values("DELIVERY_MTU").reset_index(drop=True)

    total = len(out)
    matched = out["BM_IMBALANCE_PRICE"].notna().sum()
    print(f"Rows: {total}")
    print(f"BM matched: {matched} ({matched/total:.2%})")
    print(f"Range: {out['DELIVERY_MTU'].min()} -> {out['DELIVERY_MTU'].max()}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"Wrote: {OUT_CSV}")


if __name__ == "__main__":
    main()
