from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
IN_DIR = PROJECT_ROOT / "data" / "processed" / "HENEX" / "processed"
OUT_CSV = IN_DIR / "HENEX_Aligned_Prices_20251001_20260330.csv"

DATE_FROM = "2025-10-01 00:00:00"
DATE_TO = "2026-03-30 23:45:00"
FREQ = "15min"

FILES = {
    "IDA1": IN_DIR / "EL-IDA1_Results_20251001_20260330.csv",
    "IDA2": IN_DIR / "EL-IDA2_Results_20251001_20260330.csv",
    "IDA3": IN_DIR / "EL-IDA3_Results_20251001_20260330.csv",
    "DAM price": IN_DIR / "EL-DAM_Results_20251001_20260330.csv",
}


def load_ida_series(path: Path, column_name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    df = pd.read_csv(path, usecols=["DELIVERY_MTU", "MCP"])
    df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce").dt.floor(FREQ)
    df["MCP"] = pd.to_numeric(df["MCP"], errors="coerce")

    df = df.dropna(subset=["DELIVERY_MTU"]).copy()
    df = (
        df.sort_values("DELIVERY_MTU")
        .drop_duplicates(subset=["DELIVERY_MTU"], keep="last")
        .rename(columns={"MCP": column_name})
        .reset_index(drop=True)
    )
    return df


def load_dam_series(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    df = pd.read_csv(path, usecols=["DELIVERY_MTU", "DAM_MCP"])
    df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce").dt.floor(FREQ)
    df["DAM_MCP"] = pd.to_numeric(df["DAM_MCP"], errors="coerce")

    df = df.dropna(subset=["DELIVERY_MTU"]).copy()
    df = (
        df.sort_values("DELIVERY_MTU")
        .drop_duplicates(subset=["DELIVERY_MTU"], keep="last")
        .rename(columns={"DAM_MCP": "DAM price"})
        .reset_index(drop=True)
    )
    return df


def build_calendar() -> pd.DataFrame:
    calendar = pd.DataFrame(
        {
            "Day time": pd.date_range(
                start=pd.Timestamp(DATE_FROM),
                end=pd.Timestamp(DATE_TO),
                freq=FREQ,
            )
        }
    )
    return calendar


def main() -> None:
    calendar = build_calendar()

    ida1 = load_ida_series(FILES["IDA1"], "IDA1")
    ida2 = load_ida_series(FILES["IDA2"], "IDA2")
    ida3 = load_ida_series(FILES["IDA3"], "IDA3")
    dam = load_dam_series(FILES["DAM price"])

    merged = (
        calendar.merge(dam, left_on="Day time", right_on="DELIVERY_MTU", how="left")
        .drop(columns=["DELIVERY_MTU"])
        .merge(ida1, left_on="Day time", right_on="DELIVERY_MTU", how="left")
        .drop(columns=["DELIVERY_MTU"])
        .merge(ida2, left_on="Day time", right_on="DELIVERY_MTU", how="left")
        .drop(columns=["DELIVERY_MTU"])
        .merge(ida3, left_on="Day time", right_on="DELIVERY_MTU", how="left")
        .drop(columns=["DELIVERY_MTU"])
    )

    merged = merged[["Day time", "DAM price", "IDA1", "IDA2", "IDA3"]].copy()
    merged = merged.sort_values("Day time").reset_index(drop=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)

    print(f"Wrote: {OUT_CSV}")
    print(f"Rows: {len(merged)}")
    print(f"Range: {merged['Day time'].min()} -> {merged['Day time'].max()}")
    for column in ["DAM price", "IDA1", "IDA2", "IDA3"]:
        print(f"{column} non-null: {merged[column].notna().sum()} / {len(merged)}")


if __name__ == "__main__":
    main()
