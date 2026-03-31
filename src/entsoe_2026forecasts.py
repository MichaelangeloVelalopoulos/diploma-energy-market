import os
import re

import pandas as pd
from dateutil.relativedelta import relativedelta
from entsoe import EntsoePandasClient


# ======================================================
# CONFIG
# ======================================================

IN_CSV = "data/processed/MERGED/Final2026.csv"
OUT_CSV = "data/processed/MERGED/Final2026_with_ENTSOE.csv"

AREA = "GR"
TZ = "Europe/Athens"

START = pd.Timestamp("2025-10-01 00:00", tz=TZ)
END = pd.Timestamp("2026-03-31 00:00", tz=TZ)  # end exclusive -> includes 2026-03-30 23:45

API_KEY = os.getenv("ENTSOE_API_TOKEN") or os.getenv("ENTSOE_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "ENTSOE token not set. Export ENTSOE_API_TOKEN='YOUR_TOKEN' "
        "or ENTSOE_API_KEY='YOUR_TOKEN'."
    )


# ======================================================
# HELPERS
# ======================================================

def normalize_colname(value) -> str:
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def _is_tz_aware(dt_series: pd.Series) -> bool:
    try:
        return dt_series.dt.tz is not None
    except Exception:
        return False


def localize_greek_time_safe(series: pd.Series, tz: str) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    if dt.isna().any():
        bad = series[dt.isna()].head(10).tolist()
        raise ValueError(f"Unparseable datetime examples: {bad}")

    if _is_tz_aware(dt):
        return dt.dt.tz_convert(tz)

    try:
        return dt.dt.tz_localize(tz, ambiguous="infer", nonexistent="shift_forward")
    except Exception as exc:
        if "AmbiguousTimeError" in repr(exc) or "AmbiguousTimeError" in str(exc):
            return dt.dt.tz_localize(tz, ambiguous=True, nonexistent="shift_forward")
        raise


def find_time_col(df: pd.DataFrame) -> str:
    cols = {normalize_colname(c): c for c in df.columns}
    for exact in ["day time", "delivery_mtu", "datetime", "timestamp"]:
        if exact in cols:
            return cols[exact]
    for column in df.columns:
        lowered = normalize_colname(column)
        if "time" in lowered or "delivery" in lowered:
            return column
    raise ValueError(f"Could not find timestamp column. Columns={df.columns.tolist()}")


def fetch_chunked_monthly(fetch_fn, start: pd.Timestamp, end: pd.Timestamp, label: str):
    parts = []
    current = start
    while current < end:
        nxt = min(current + relativedelta(months=1), end)
        print(f"[ENTSOE] {label}: {current} -> {nxt}")
        try:
            out = fetch_fn(current, nxt)
            if out is None or (hasattr(out, "empty") and out.empty):
                print("   warning: empty chunk")
            else:
                parts.append(out)
        except Exception as exc:
            print("   warning:", repr(exc))
        current = nxt

    if not parts:
        return None

    combined = pd.concat(parts).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined


def upsample_ffill_to_target(obj, target_index: pd.DatetimeIndex):
    if isinstance(obj, pd.Series):
        series = obj.sort_index()
        series = series[~series.index.duplicated(keep="last")]
        return series.reindex(target_index).ffill()

    df = obj.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df.reindex(target_index).ffill()


def pick_first_matching_column(df: pd.DataFrame, must_have_keywords: list[str]) -> str:
    columns = list(df.columns)
    normalized = [normalize_colname(column) for column in columns]
    for idx, name in enumerate(normalized):
        if all(keyword in name for keyword in must_have_keywords):
            return columns[idx]
    raise KeyError(f"Could not find column with keywords={must_have_keywords}. Available={list(df.columns)}")


def extract_wind_solar_series(ws_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    if ws_df is None or ws_df.empty:
        raise RuntimeError("Wind/Solar forecast dataframe is empty.")

    solar_col = pick_first_matching_column(ws_df, ["solar"])
    try:
        wind_col = pick_first_matching_column(ws_df, ["wind", "onshore"])
    except KeyError:
        wind_col = pick_first_matching_column(ws_df, ["wind"])

    wind = pd.to_numeric(ws_df[wind_col], errors="coerce")
    solar = pd.to_numeric(ws_df[solar_col], errors="coerce")
    return wind, solar


# ======================================================
# MAIN
# ======================================================

def main():
    print("Using ENTSOE key:", API_KEY[:6], "...")

    base = pd.read_csv(IN_CSV)
    time_col = find_time_col(base)

    base["Day time"] = localize_greek_time_safe(base[time_col], TZ)
    base = base.sort_values("Day time").reset_index(drop=True)

    idx_15 = pd.DatetimeIndex(base["Day time"])

    client = EntsoePandasClient(api_key=API_KEY)

    load_fc = fetch_chunked_monthly(
        lambda s, e: client.query_load_forecast(country_code=AREA, start=s, end=e),
        START,
        END,
        "System Load Forecast",
    )
    if load_fc is None or len(load_fc) == 0:
        raise RuntimeError("No System Load forecast data returned from ENTSO-E.")

    ws_fc = fetch_chunked_monthly(
        lambda s, e: client.query_wind_and_solar_forecast(
            country_code=AREA,
            start=s,
            end=e,
            process_type="A01",
        ),
        START,
        END,
        "Wind & Solar DA Forecast",
    )
    if ws_fc is None or ws_fc.empty:
        raise RuntimeError("No Wind/Solar DA forecast data returned from ENTSO-E.")

    wind_series, solar_series = extract_wind_solar_series(ws_fc)

    base["SystemLoad_DA_forecast"] = upsample_ffill_to_target(load_fc, idx_15).to_numpy()
    base["Wind_DA_forecast"] = upsample_ffill_to_target(wind_series, idx_15).to_numpy()
    base["Solar_DA_forecast"] = upsample_ffill_to_target(solar_series, idx_15).to_numpy()

    base["Day time"] = base["Day time"].dt.tz_localize(None)

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    base.to_csv(OUT_CSV, index=False)

    print("DONE")
    print("Saved:", OUT_CSV)
    print("Rows:", len(base))
    print("Range:", base["Day time"].min(), "->", base["Day time"].max())


if __name__ == "__main__":
    main()
