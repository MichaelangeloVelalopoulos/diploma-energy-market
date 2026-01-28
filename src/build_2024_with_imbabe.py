import pandas as pd
from pathlib import Path

# =========================
# PATHS
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "processed"

MAIN_CSV = DATA_DIR / "2024final.csv"
IMBABE_DIR = DATA_DIR / "admie_downloads" / "imbabe"
OUT_CSV = DATA_DIR / "2024final_with_imbabe_intrahour.csv"

# =========================
# LOAD MAIN DATASET
# =========================
df = pd.read_csv(MAIN_CSV)
df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce")
if df["DELIVERY_MTU"].isna().any():
    raise ValueError("Unparsable DELIVERY_MTU found in 2024final.csv")

# Ensure hourly stamps
df["DELIVERY_MTU"] = df["DELIVERY_MTU"].dt.floor("H")

# =========================
# LOAD ALL IMBABE FILES (weekly) AND CONCAT
# =========================
files = sorted(IMBABE_DIR.glob("*_IMBABE_*.xlsx"))
if not files:
    raise FileNotFoundError(f"No IMBABE xlsx files in: {IMBABE_DIR}")

all_imb = []
for fp in files:
    x = pd.read_excel(fp)

    # expected columns from your file:
    # STARTDATE, ENDDATE, Imbalance Price  (€/MWh)
    if "STARTDATE" not in x.columns:
        raise ValueError(f"{fp.name}: missing STARTDATE. Columns: {list(x.columns)}")

    # pick imbalance price column robustly
    price_col = None
    for c in x.columns:
        cl = c.lower()
        if "imbalance" in cl and "price" in cl:
            price_col = c
            break
    if price_col is None:
        raise ValueError(f"{fp.name}: cannot find Imbalance Price column. Columns: {list(x.columns)}")

    tmp = x[["STARTDATE", price_col]].copy()
    tmp.rename(columns={"STARTDATE": "start_dt", price_col: "bm_price"}, inplace=True)
    tmp["start_dt"] = pd.to_datetime(tmp["start_dt"], errors="coerce")
    tmp = tmp.dropna(subset=["start_dt", "bm_price"])

    all_imb.append(tmp)

imb = pd.concat(all_imb, ignore_index=True)

# Keep only exact quarter starts and build q index
imb["minute"] = imb["start_dt"].dt.minute
imb = imb[imb["minute"].isin([0, 15, 30, 45])].copy()

imb["DELIVERY_MTU"] = imb["start_dt"].dt.floor("H")
imb["q"] = (imb["minute"] // 15) + 1  # 0->1, 15->2, 30->3, 45->4

# Pivot to 4 columns per hour (keep last if duplicates exist)
bm_pivot = (
    imb.pivot_table(
        index="DELIVERY_MTU",
        columns="q",
        values="bm_price",
        aggfunc="last"
    )
    .rename(columns={1: "BM_q1", 2: "BM_q2", 3: "BM_q3", 4: "BM_q4"})
    .reset_index()
)

# =========================
# MERGE INTO MAIN (same day/hour)
# =========================
out = df.merge(bm_pivot, on="DELIVERY_MTU", how="left")

# =========================
# SPREADS (BM - DAM_MCP) using existing DAM_MCP
# =========================
for q in range(1, 5):
    out[f"BMmDAM_q{q}"] = out[f"BM_q{q}"] - out["DAM_MCP"]

# =========================
# REPORT COVERAGE
# =========================
total = len(out)
covered = out["BM_q1"].notna().sum()
print(f"Rows total: {total}")
print(f"Rows with BM quarters: {covered} ({covered/total:.1%})")
print(f"Saving -> {OUT_CSV}")

# =========================
# SAVE NEW FILE (does not modify originals)
# =========================
out.to_csv(OUT_CSV, index=False)
print("Done.")
