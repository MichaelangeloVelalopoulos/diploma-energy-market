from pathlib import Path
import pandas as pd

# =========================
# CONFIG
# =========================
BASE_DIR = Path("data/processed")
INPUT_FILE = BASE_DIR / "2024final.csv"

OUT_IDA1 = BASE_DIR / "IDA1.csv"
OUT_IDA2 = BASE_DIR / "IDA2.csv"
OUT_IDA3 = BASE_DIR / "IDA3.csv"

TIME_COL = "DELIVERY_MTU"

# Preference order for "keep the best row" per DELIVERY_MTU
# (we keep the latest update / most reliable record if possible)
PREF_COLS = ["PUB_TIME", "VER", "SORT"]

# =========================
# HELPERS
# =========================
def keep_one_row_per_delivery_mtu(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure exactly 1 row per DELIVERY_MTU.
    Strategy:
      - If PUB_TIME exists: keep the max PUB_TIME (latest publish)
      - Else if VER exists: keep max VER
      - Else if SORT exists: keep max SORT
      - Else: keep last occurrence in file order
    """
    df = df.copy()

    # Ensure datetime for DELIVERY_MTU
    df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")
    df = df.dropna(subset=[TIME_COL])

    # Choose best sort key available
    sort_cols = [TIME_COL]
    ascending = [True]

    if "PUB_TIME" in df.columns:
        df["PUB_TIME"] = pd.to_datetime(df["PUB_TIME"], errors="coerce")
        # sort by delivery time, then pub_time (latest last)
        sort_cols += ["PUB_TIME"]
        ascending += [True]
        # We'll keep last -> max PUB_TIME
    elif "VER" in df.columns:
        sort_cols += ["VER"]
        ascending += [True]
    elif "SORT" in df.columns:
        sort_cols += ["SORT"]
        ascending += [True]
    else:
        # No preference columns; keep last occurrence by original order
        df["_row_order"] = range(len(df))
        sort_cols += ["_row_order"]
        ascending += [True]

    df = df.sort_values(sort_cols, ascending=ascending)

    # Drop duplicates: keep the last (best) row per DELIVERY_MTU
    df = df.drop_duplicates(subset=[TIME_COL], keep="last")

    # Clean helper
    if "_row_order" in df.columns:
        df = df.drop(columns=["_row_order"], errors="ignore")

    return df

def build_window(df: pd.DataFrame, h_from: int, h_to: int) -> pd.DataFrame:
    """
    Keep rows where DELIVERY_MTU hour is within [h_from, h_to], inclusive.
    Then keep exactly 1 row per DELIVERY_MTU.
    """
    d = df.copy()
    d["delivery_hour"] = d[TIME_COL].dt.hour
    d = d[(d["delivery_hour"] >= h_from) & (d["delivery_hour"] <= h_to)].copy()

    # Ensure chronological
    d = d.sort_values(TIME_COL)

    # CRITICAL: keep exactly one record per hour
    d = keep_one_row_per_delivery_mtu(d)

    # Final sort
    d = d.sort_values(TIME_COL).reset_index(drop=True)
    return d

# =========================
# MAIN
# =========================
df = pd.read_csv(INPUT_FILE)

if TIME_COL not in df.columns:
    raise ValueError(f"Missing required column: {TIME_COL}")

df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")
df = df.dropna(subset=[TIME_COL]).copy()

print("[INFO] Input rows:", len(df))
print("[INFO] Example duplicates per DELIVERY_MTU (top 5):")
print(df[TIME_COL].value_counts().head())

# Build the 3 day-windows
df_ida1 = build_window(df, 0, 7)
df_ida2 = build_window(df, 8, 15)
df_ida3 = build_window(df, 16, 23)

# Sanity checks: no duplicates
for name, d in [("IDA1", df_ida1), ("IDA2", df_ida2), ("IDA3", df_ida3)]:
    dup = d.duplicated(subset=[TIME_COL]).sum()
    print(f"[SANITY] {name}: rows={len(d)} | duplicate DELIVERY_MTU rows={dup}")

# Save
BASE_DIR.mkdir(parents=True, exist_ok=True)
df_ida1.to_csv(OUT_IDA1, index=False)
df_ida2.to_csv(OUT_IDA2, index=False)
df_ida3.to_csv(OUT_IDA3, index=False)

print("[OK] Wrote:", OUT_IDA1)
print("[OK] Wrote:", OUT_IDA2)
print("[OK] Wrote:", OUT_IDA3)
