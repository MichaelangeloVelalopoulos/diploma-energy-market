import pandas as pd
from pathlib import Path

# =========================
# Config (LOCAL / VS CODE)
# =========================
BASE_DIR = Path("data/processed")

IN_PATH  = BASE_DIR / "IDA1.csv"
OUT_PATH = BASE_DIR / "IDA1lag.csv"

TIME_COL = "DELIVERY_MTU"
AUCTION_COL = "AUCTION"     # if exists
AUCTION_NAME = "IDA1"

MCP_COL = "MCP"
DAM_COL = "DAM_MCP"

MCP_LAGS = [1, 2, 3, 48, 168]
DAM_LAGS = [1, 2, 3]

# =========================
# Load
# =========================
print(f"Loading: {IN_PATH.resolve()}")
df = pd.read_csv(IN_PATH, parse_dates=[TIME_COL])

# Filter only IDA1 if column exists
if AUCTION_COL in df.columns:
    df = df[df[AUCTION_COL] == AUCTION_NAME].copy()

# Sort by delivery time
df = df.sort_values(TIME_COL).reset_index(drop=True)

# =========================
# Sanity checks
# =========================
missing = [c for c in [MCP_COL, DAM_COL] if c not in df.columns]
if missing:
    raise ValueError(
        f"Missing required columns: {missing}\n"
        f"Available columns: {list(df.columns)}"
    )

print("Rows before lags:", len(df))
print("Time range:", df[TIME_COL].min(), "->", df[TIME_COL].max())

# =========================
# Create lag features
# =========================
for k in MCP_LAGS:
    df[f"{MCP_COL}_lag{k}"] = df[MCP_COL].shift(k)

for k in DAM_LAGS:
    df[f"{DAM_COL}_lag{k}"] = df[DAM_COL].shift(k)

# =========================
# Drop rows with NaNs caused by lags
# =========================
lag_cols = (
    [f"{MCP_COL}_lag{k}" for k in MCP_LAGS] +
    [f"{DAM_COL}_lag{k}" for k in DAM_LAGS]
)

df_lagged = df.dropna(subset=lag_cols + [MCP_COL, DAM_COL]).reset_index(drop=True)

print("Rows after dropping lag NaNs:", len(df_lagged))
print("Lag columns added:", lag_cols)

# =========================
# Save
# =========================
df_lagged.to_csv(OUT_PATH, index=False)
print(f"Saved lagged dataset to: {OUT_PATH.resolve()}")
