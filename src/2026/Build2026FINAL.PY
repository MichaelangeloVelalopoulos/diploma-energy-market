import re
import warnings
from pathlib import Path

import pandas as pd


warnings.filterwarnings("ignore", message="Workbook contains no default style")


# =========================
# CONFIG
# =========================
BASE_IN = Path("data/processed/HENEX/processed/HENEX_Aligned_Prices_20251001_20260330.csv")
IMBABE_DIR = Path("data/processed/admie_downloads/imbabe")
ISP_DIR = Path("data/processed/admie_downloadsForecasts")
OUT_CSV = Path("data/processed/MERGED/Final2026.csv")

DATE_FROM = "2025-10-01 00:00:00"
DATE_TO = "2026-03-30 23:45:00"
FREQ = "15min"

PREFERRED_IMBABE_SHEET = "Activated_bal_Energy_and_Prices"

FORECAST_FOLDERS = {
    "ISP1DayAheadRESForecast": "ISP1_RES",
    "ISP1DayAheadLoadForecast": "ISP1_Load",
    "ISP2DayAheadRESForecast": "ISP2_RES",
    "ISP2DayAheadLoadForecast": "ISP2_Load",
    "ISP2IntraDayRESForecast": "ISP2_RES",
    "ISP2IntraDayLoadForecast": "ISP2_Load",
    "ISP3IntraDayRESForecast": "ISP3_RES",
    "ISP3IntraDayLoadForecast": "ISP3_Load",
}


# =========================
# HELPERS
# =========================
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", str(c).replace("\u00a0", " ").strip()) for c in df.columns]
    return df


def parse_dt_series(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    if dt.isna().any():
        dt2 = pd.to_datetime(series, format="%m/%d/%y %H:%M", errors="coerce")
        dt = dt.fillna(dt2)
    return dt.dt.floor(FREQ)


def find_time_col(df: pd.DataFrame) -> str:
    cols = {c.lower().strip(): c for c in df.columns}
    for exact in ["delivery_mtu", "day time", "datetime", "timestamp"]:
        if exact in cols:
            return cols[exact]
    for column in df.columns:
        lowered = column.lower()
        if "delivery" in lowered or "time" in lowered:
            return column
    raise ValueError(f"Could not find time column. Columns={df.columns.tolist()}")


def build_calendar() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Day time": pd.date_range(
                start=pd.Timestamp(DATE_FROM),
                end=pd.Timestamp(DATE_TO),
                freq=FREQ,
            )
        }
    )


def load_base_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"BASE_IN not found: {path}")

    df = pd.read_csv(path)
    df = normalize_cols(df)

    time_col = find_time_col(df)
    df["Day time"] = parse_dt_series(df[time_col])
    df = df.dropna(subset=["Day time"]).copy()

    rename_map = {}
    lower_map = {c.lower().strip(): c for c in df.columns}
    if "dam price" not in lower_map and "dam_mcp" in lower_map:
        rename_map[lower_map["dam_mcp"]] = "DAM price"
    for name in ["ida1", "ida2", "ida3"]:
        if name not in lower_map and name.upper() in df.columns:
            rename_map[name.upper()] = name.upper()
    if rename_map:
        df = df.rename(columns=rename_map)

    expected = ["DAM price", "IDA1", "IDA2", "IDA3"]
    missing = [column for column in expected if column not in df.columns]
    if missing:
        raise ValueError(f"Base prices file is missing columns: {missing}")

    for column in expected:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = (
        df[["Day time", "DAM price", "IDA1", "IDA2", "IDA3"]]
        .sort_values("Day time")
        .drop_duplicates(subset=["Day time"], keep="last")
        .reset_index(drop=True)
    )
    return df


def load_all_imbabe(imbabe_dir: Path) -> pd.DataFrame:
    files = sorted(imbabe_dir.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"No .xlsx found in {imbabe_dir}")

    frames = []
    for file_path in files:
        xl = pd.ExcelFile(file_path)
        sheet = PREFERRED_IMBABE_SHEET if PREFERRED_IMBABE_SHEET in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
        df = normalize_cols(df)

        if "STARTDATE" not in df.columns:
            candidates = [c for c in df.columns if c.strip().lower() == "startdate"]
            if candidates:
                df = df.rename(columns={candidates[0]: "STARTDATE"})
            else:
                raise ValueError(f"{file_path.name}: STARTDATE not found. Columns={df.columns.tolist()}")

        price_col = None
        for column in df.columns:
            if "imbalance price" in column.lower():
                price_col = column
                break
        if price_col is None:
            raise ValueError(f"{file_path.name}: Imbalance Price column not found. Columns={df.columns.tolist()}")

        out = df[["STARTDATE", price_col]].copy()
        out.columns = ["STARTDATE", "BM price"]
        out["Day time"] = parse_dt_series(out["STARTDATE"])
        out["BM price"] = pd.to_numeric(out["BM price"], errors="coerce")
        out = out.dropna(subset=["Day time", "BM price"]).copy()
        frames.append(out[["Day time", "BM price"]])

    bm = pd.concat(frames, ignore_index=True)
    bm = (
        bm.sort_values("Day time")
        .drop_duplicates(subset=["Day time"], keep="last")
        .reset_index(drop=True)
    )
    return bm


def parse_isp_file(filepath: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(filepath, header=None, engine="openpyxl")
    except Exception as exc:
        print(f"  SKIP (can't read): {filepath.name} — {exc}")
        return None

    if df.shape[0] < 2:
        print(f"  SKIP (too few rows): {filepath.name}")
        return None

    qh_row = df.iloc[0, 4:].values
    data_row = df.iloc[1, 4:].values

    date_val = df.iloc[1, 3]
    if pd.isna(date_val):
        date_val = df.iloc[1, 2]
    if pd.isna(date_val):
        print(f"  SKIP (no date found): {filepath.name}")
        return None

    day = pd.Timestamp(date_val)
    rows = []
    for qh, value in zip(qh_row, data_row):
        if pd.notna(qh) and pd.notna(value):
            qh_int = int(qh)
            hour = (qh_int - 1) // 4
            minute = ((qh_int - 1) % 4) * 15
            rows.append(
                {
                    "Day time": day + pd.Timedelta(hours=hour, minutes=minute),
                    "value": float(value),
                }
            )

    return pd.DataFrame(rows) if rows else None


def parse_isp_folder(folder_path: Path, col_name: str) -> pd.DataFrame | None:
    if not folder_path.exists():
        return None

    files = sorted(folder_path.glob("*.xlsx"))
    if not files:
        return None

    best_by_day: dict[str, tuple[int, Path]] = {}
    for file_path in files:
        match = re.match(r"^(\d{8})_.+?_(\d+)\.xlsx$", file_path.name)
        if match:
            file_date = match.group(1)
            version = int(match.group(2))
        else:
            file_date = file_path.name[:8] if file_path.name[:8].isdigit() else None
            version = 0
        if not file_date:
            continue
        current = best_by_day.get(file_date)
        if current is None or version > current[0]:
            best_by_day[file_date] = (version, file_path)

    frames = []
    for _, file_path in sorted(best_by_day.values(), key=lambda item: item[1].name):
        parsed = parse_isp_file(file_path)
        if parsed is not None:
            frames.append(parsed)

    if not frames:
        return None

    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"value": col_name})
    out["Day time"] = parse_dt_series(out["Day time"])
    out = out.dropna(subset=["Day time"]).copy()
    out[col_name] = pd.to_numeric(out[col_name], errors="coerce")
    out = (
        out.sort_values("Day time")
        .drop_duplicates(subset=["Day time"], keep="last")
        .reset_index(drop=True)
    )
    return out


def load_all_isp_forecasts(isp_dir: Path) -> dict[str, pd.DataFrame]:
    if not isp_dir.exists():
        raise FileNotFoundError(f"ISP_DIR not found: {isp_dir}")

    frames = {}
    for folder_name, column_name in FORECAST_FOLDERS.items():
        parsed = parse_isp_folder(isp_dir / folder_name, column_name)
        if parsed is not None:
            frames[column_name] = parsed
    return frames


# =========================
# MAIN
# =========================
def main():
    calendar = build_calendar()
    base = load_base_prices(BASE_IN)
    bm = load_all_imbabe(IMBABE_DIR)
    isp_frames = load_all_isp_forecasts(ISP_DIR)

    out = calendar.merge(base, on="Day time", how="left")
    out = out.merge(bm, on="Day time", how="left")

    out["BMmDAM price"] = out["BM price"] - out["DAM price"]

    for column_name, frame in sorted(isp_frames.items()):
        out = out.merge(frame, on="Day time", how="left")

    ordered_columns = [
        "Day time",
        "DAM price",
        "IDA1",
        "IDA2",
        "IDA3",
        "BM price",
        "BMmDAM price",
        "ISP1_RES",
        "ISP1_Load",
        "ISP2_RES",
        "ISP2_Load",
        "ISP3_RES",
        "ISP3_Load",
    ]
    existing = [column for column in ordered_columns if column in out.columns]
    out = out[existing].sort_values("Day time").reset_index(drop=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    print(f"Wrote: {OUT_CSV}")
    print(f"Rows: {len(out)}")
    print(f"Range: {out['Day time'].min()} -> {out['Day time'].max()}")
    for column in out.columns[1:]:
        print(f"{column} non-null: {out[column].notna().sum()} / {len(out)}")


if __name__ == "__main__":
    main()
