from pathlib import Path
import sys
import pandas as pd

# Resolve project root: diploma-energy-market/
PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_FILE = PROJECT_ROOT / "data" / "processed" / "feature_enriched_idm_entsoe_2024.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "2024final.csv"
DROP_LOG_FILE = PROJECT_ROOT / "data" / "processed" / "2024final_dropped_columns.txt"

# -------------------------
# Columns we NEVER drop
# -------------------------
PROTECT_COLS = {
    "DELIVERY_MTU",   # keep timestamp column in the CSV
    # add others if needed:
    # "AUCTION", "MCP"
}

# Pattern-based drops (case-insensitive substring match)
DROP_PATTERNS = [
    "gust",
    "cloud",
    "precip",
    "is_day",
    "rollmean",
    "rollstd",
    "delta",
    "lag",
    "spread",
    "mcp_diff",
    "shortwave",
    "temperature",
    "fossil",
    "hydro"
]

# Explicit drops (case-insensitive substring match)
EXPLICIT_DROPS = [
    "fossil oil",
    "hydro pumped storage",
    "dam_mcp_lag1",
    "dam_mcp_lag2",
    "dam_mcp_lag3",
    "bidding_zone_descr",
    "delivery_duration",
    "total_trades",
    "ver",
    "dam_total_trades",
    "dam_ver",
    "cal_dow",
    "cal_month",
    "cal_hour",
    "cb_import_mw",
    "cb_export_mw",
    "SystemLoad_actual"
]

def drop_solar_wind_except_id(df: pd.DataFrame):
    """
    Drop all columns that are related to solar or wind generation/forecasts
    EXCEPT those that contain 'id' (intraday forecast).
    """
    cols_to_drop = []
    for c in df.columns:
        cl = c.lower()
        if ("solar" in cl or "wind" in cl):
            if "da" not in cl:
                cols_to_drop.append(c)

    # protect columns
    cols_to_drop = [c for c in cols_to_drop if c not in PROTECT_COLS]
    return df.drop(columns=cols_to_drop, errors="ignore"), cols_to_drop

def main():
    if not INPUT_FILE.exists():
        print(f"[ERROR] Input file not found: {INPUT_FILE}")
        sys.exit(1)

    print(f"[INFO] Reading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    print("[SANITY] DELIVERY_MTU exists before drops?", "DELIVERY_MTU" in df.columns)

    # 1) pattern drops
    pattern_cols_to_drop = [
        c for c in df.columns
        if any(pat in c.lower() for pat in DROP_PATTERNS)
    ]

    # 2) explicit drops (substring match)
    explicit_cols_to_drop = [
        c for c in df.columns
        if any(exp in c.lower() for exp in EXPLICIT_DROPS)
    ]

    cols_to_drop = sorted(set(pattern_cols_to_drop + explicit_cols_to_drop))

    # Safety: never drop target
    if "TARGET" in cols_to_drop:
        cols_to_drop.remove("TARGET")

    # PROTECT: never drop protected columns
    cols_to_drop = [c for c in cols_to_drop if c not in PROTECT_COLS]

    before_cols = df.shape[1]
    df_clean = df.drop(columns=cols_to_drop, errors="ignore")

    # 3) solar/wind cleanup (drop everything except *_ID)
    df_clean, dropped_res = drop_solar_wind_except_id(df_clean)

    after_cols = df_clean.shape[1]

    # Build final dropped list for logging
    cols_to_drop_final = sorted(set(cols_to_drop + dropped_res))
    if "TARGET" in cols_to_drop_final:
        cols_to_drop_final.remove("TARGET")
    cols_to_drop_final = [c for c in cols_to_drop_final if c not in PROTECT_COLS]

    print(f"[INFO] Columns before: {before_cols}")
    print(f"[INFO] Columns after : {after_cols}")
    print(f"[INFO] Dropped columns (total): {len(cols_to_drop_final)}")

    print("[SANITY] DELIVERY_MTU exists after drops?", "DELIVERY_MTU" in df_clean.columns)

    # Quick sanity: show remaining solar/wind columns (should be only *_ID)
    remaining_sw = [c for c in df_clean.columns if ("solar" in c.lower() or "wind" in c.lower())]
    if remaining_sw:
        print("[INFO] Remaining Solar/Wind columns:")
        for c in remaining_sw:
            print("  -", c)
    else:
        print("[WARN] No Solar/Wind columns remain (OK only if you intended that).")

    # Write output CSV
    df_clean.to_csv(OUTPUT_FILE, index=False)
    print(f"[OK] Wrote cleaned dataset: {OUTPUT_FILE}")

    # Write drop log
    DROP_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DROP_LOG_FILE, "w", encoding="utf-8") as f:
        for c in cols_to_drop_final:
            f.write(c + "\n")
    print(f"[OK] Wrote drop log: {DROP_LOG_FILE}")

if __name__ == "__main__":
    main()
