import re
import sys
from pathlib import Path

import pandas as pd

# =========================
# CONFIG (αλλαξέ το path αν χρειάζεται)
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Βάλε εδώ τον φάκελο που έχεις τα xlsx του IDA1
# π.χ. PROJECT_ROOT / "data" / "processed" / "IDA1"
# ή αν είναι στο προηγούμενο pipeline: PROJECT_ROOT/"data"/"processed"/"henex_ida_results"/"raw"/"IDA1"
IN_DIR = PROJECT_ROOT / "data" / "processed" / "henex_ida_results" / "raw" / "IDA1"

OUT_DIR = PROJECT_ROOT / "data" / "processed" / "henex_ida_results" / "processed"/"2026idaonlydataset"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_FROM = "20251001"
DATE_TO   = "20260127"

IDA_TAG = "IDA1"

# =========================
# HELPERS
# =========================
PAT = re.compile(r"^(?P<d>\d{8})_EL-IDA1_Results_EN_v(?P<v>\d{2})\.xlsx$", re.IGNORECASE)

def within_range(d: str) -> bool:
    return int(DATE_FROM) <= int(d) <= int(DATE_TO)

def pick_one_file_per_day(paths: list[Path]) -> dict[str, Path]:
    """
    Αν υπάρχουν διπλά για την ίδια μέρα (v01, v02, ...),
    κρατάμε το μεγαλύτερο v##. Αν tie, κρατάμε το πιο πρόσφατο mtime.
    """
    best: dict[str, tuple[int, float, Path]] = {}  # d -> (v, mtime, path)

    for p in paths:
        m = PAT.match(p.name)
        if not m:
            continue
        d = m.group("d")
        if not within_range(d):
            continue
        v = int(m.group("v"))
        mtime = p.stat().st_mtime

        if d not in best:
            best[d] = (v, mtime, p)
        else:
            v0, t0, p0 = best[d]
            if (v > v0) or (v == v0 and mtime > t0):
                best[d] = (v, mtime, p)

    return {d: tup[2] for d, tup in best.items()}

def reshape_ida_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Long -> wide ανά DELIVERY_MTU με MCP__/TRADES__ columns.
    """
    required = {
        "TARGET","BIDDING_ZONE_DESCR","DDAY","DELIVERY_MTU","DELIVERY_DURATION","SORT","PUB_TIME","VER",
        "SIDE_DESCR","ASSET_DESCR","CLASSIFICATION","MCP","TOTAL_TRADES"
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    df = df.copy()
    df["DDAY"] = pd.to_datetime(df["DDAY"].astype(str), format="%Y%m%d", errors="coerce")
    df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce")

    # fast SERIES_ID (vectorized)
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

def read_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]
    return df

# =========================
# MAIN
# =========================
def main():
    if not IN_DIR.exists():
        raise FileNotFoundError(f"IN_DIR does not exist: {IN_DIR}")

    all_xlsx = sorted(IN_DIR.glob("*.xlsx"))
    chosen = pick_one_file_per_day(all_xlsx)

    if not chosen:
        print(f"No matching IDA1 files found in {IN_DIR} for range {DATE_FROM}-{DATE_TO}")
        return

    days_sorted = sorted(chosen.keys())
    print(f"Found days: {len(days_sorted)} (min={days_sorted[0]} max={days_sorted[-1]})")

    frames = []
    for d in days_sorted:
        path = chosen[d]
        try:
            df_long = read_xlsx(path)
            df_wide = reshape_ida_results(df_long)
            df_wide["SOURCE_FILE"] = path.name
            frames.append(df_wide)
            print(f"[OK] {d} <- {path.name}")
        except Exception as e:
            print(f"[FAIL] {d} <- {path.name} | {e}", file=sys.stderr)

    if not frames:
        print("Nothing parsed successfully.")
        return

    full = (
        pd.concat(frames, ignore_index=True)
          .sort_values(["DDAY","DELIVERY_MTU","SORT"])
          .reset_index(drop=True)
    )
    # -------------------------
    # Create unified MCP column
    # -------------------------
    mcp_cols = [c for c in full.columns if c.startswith("MCP__")]
    if not mcp_cols:
        raise ValueError("No MCP__ columns found to build unified MCP")

    # Row-wise: first non-null across all MCP__ columns
    full["MCP"] = full[mcp_cols].bfill(axis=1).iloc[:, 0]

    # Sanity: if still NaN, report
    missing_mcp = full["MCP"].isna().sum()
    if missing_mcp > 0:
        print(f"[WARN] MCP still missing for {missing_mcp} rows (no MCP__ value present in any category).")

    # OPTIONAL: drop all the MCP__ category columns now that we have unified MCP
    full = full.drop(columns=mcp_cols)
    # -------------------------
    # Drop TRADES__* columns
    # -------------------------
    trades_cols = [c for c in full.columns if c.startswith("TRADES__")]
    if trades_cols:
        full = full.drop(columns=trades_cols)
        print(f"[INFO] Dropped {len(trades_cols)} TRADES__ columns")

    out_base = OUT_DIR / f"EL-{IDA_TAG}_MASTER_{DATE_FROM}_{DATE_TO}"

    # CSV (πάντα)
    out_csv = out_base.with_suffix(".csv")
    full.to_csv(out_csv, index=False)
    print(f"Wrote CSV: {out_csv}")

    # Parquet (αν υπάρχει engine)
    try:
        out_parquet = out_base.with_suffix(".parquet")
        full.to_parquet(out_parquet, index=False)
        print(f"Wrote Parquet: {out_parquet}")
    except Exception as e:
        print(f"Parquet skipped (install pyarrow): {e}")

if __name__ == "__main__":
    main()
