# src/entsoe_2026forecasts.py
# ------------------------------------------------------
# Merges ENTSO-E forecasts (System Load DA, Wind DA, Solar DA)
# into your existing 15-min Greek-time dataset CSV.
#
# Key fixes:
# - Robust timezone localization across DST (AmbiguousTimeError on 2025-10-26 03:00:00)
# - No assumptions about ENTSO-E column naming (auto-detect solar/wind columns)
# - Chunked monthly fetching
# - Forward-fill to your exact DELIVERY_MTU index
# ------------------------------------------------------

import os
import re
import pandas as pd
from entsoe import EntsoePandasClient
from dateutil.relativedelta import relativedelta


# ======================================================
# CONFIG
# ======================================================

IN_CSV = "data/processed/merged/EL-IDA1_WITH_DAM_BM_20251001_20260127.csv"
OUT_CSV = "data/processed/merged/Final2026.csv"

AREA = "GR"
TZ = "Europe/Athens"

START = pd.Timestamp("2025-10-01 00:00", tz=TZ)
END   = pd.Timestamp("2026-01-28 00:00", tz=TZ)  # end exclusive

API_KEY = os.getenv("ENTSOE_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ ENTSOE_API_KEY not set. Run: export ENTSOE_API_KEY='YOUR_TOKEN'")


# ======================================================
# HELPERS
# ======================================================

def _is_tz_aware(dt_series: pd.Series) -> bool:
    try:
        return dt_series.dt.tz is not None
    except Exception:
        return False


def localize_greek_time_safe(ts: pd.Series, tz: str) -> pd.Series:
    """
    Robust localization for Greek timestamps with DST handling.
    Your CSV is in Greek local time (naive). On DST end, 03:00 occurs twice.
    We handle it deterministically.

    Strategy:
      1) parse to datetime (naive)
      2) try ambiguous='infer' (works if sequence is complete/monotonic)
      3) if still ambiguous -> use ambiguous=True (pick first occurrence) consistently
      4) handle DST-start nonexistent times with shift_forward
    """
    dt = pd.to_datetime(ts, errors="coerce")
    if dt.isna().any():
        bad = ts[dt.isna()].head(10).tolist()
        raise ValueError(f"Unparseable DELIVERY_MTU examples: {bad}")

    # If already tz-aware, just convert
    if _is_tz_aware(dt):
        return dt.dt.tz_convert(tz)

    # Try infer first (best when times are continuous 15-min grid)
    try:
        return dt.dt.tz_localize(
            tz,
            ambiguous="infer",
            nonexistent="shift_forward"
        )
    except Exception as e:
        # If ambiguous persists, force a deterministic choice
        msg = repr(e)
        if "AmbiguousTimeError" in msg or "AmbiguousTimeError" in str(e):
            return dt.dt.tz_localize(
                tz,
                ambiguous=True,              # choose FIRST occurrence consistently
                nonexistent="shift_forward"
            )
        raise


def fetch_chunked_monthly(fetch_fn, start: pd.Timestamp, end: pd.Timestamp, label: str):
    """
    Fetch ENTSO-E data month-by-month to reduce rate-limit issues.
    Returns pandas object (Series or DataFrame) with DatetimeIndex tz-aware.
    """
    parts = []
    cur = start
    while cur < end:
        nxt = min(cur + relativedelta(months=1), end)
        print(f"[ENTSOE] {label}: {cur} → {nxt}")
        try:
            out = fetch_fn(cur, nxt)
            if out is None:
                print("   ⚠️ returned None")
            elif hasattr(out, "empty") and out.empty:
                print("   ⚠️ empty chunk")
            else:
                parts.append(out)
        except Exception as e:
            print("   ⚠️", repr(e))
        cur = nxt

    if not parts:
        return None

    cat = pd.concat(parts).sort_index()
    cat = cat[~cat.index.duplicated(keep="last")]
    return cat


def upsample_ffill_to_target(obj, target_index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Reindex Series/DataFrame to target_index and forward-fill.
    Works for both Series and DataFrame.
    """
    if isinstance(obj, pd.Series):
        s = obj.sort_index()
        s = s[~s.index.duplicated(keep="last")]
        return s.reindex(target_index).ffill()
    else:
        df = obj.sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df.reindex(target_index).ffill()


def normalize_colname(x) -> str:
    return re.sub(r"\s+", " ", str(x)).strip().lower()


def pick_first_matching_column(df: pd.DataFrame, must_have_keywords: list[str]) -> str:
    """
    Find column whose normalized name contains ALL keywords.
    """
    cols = list(df.columns)
    norm = [normalize_colname(c) for c in cols]
    for i, name in enumerate(norm):
        ok = True
        for kw in must_have_keywords:
            if kw not in name:
                ok = False
                break
        if ok:
            return cols[i]
    raise KeyError(f"Could not find column with keywords={must_have_keywords}. Available={list(df.columns)}")


def extract_wind_solar_series(ws_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    ENTSO-E wind/solar forecast response column naming can vary.
    We'll robustly pick:
      - solar: any column containing 'solar'
      - wind: prefer 'wind onshore' else any 'wind'
    """
    if ws_df is None or ws_df.empty:
        raise RuntimeError("Wind/Solar forecast dataframe is empty.")

    # Ensure numeric
    ws_df = ws_df.copy()

    solar_col = pick_first_matching_column(ws_df, ["solar"])

    try:
        wind_col = pick_first_matching_column(ws_df, ["wind", "onshore"])
    except KeyError:
        wind_col = pick_first_matching_column(ws_df, ["wind"])

    solar = pd.to_numeric(ws_df[solar_col], errors="coerce")
    wind  = pd.to_numeric(ws_df[wind_col], errors="coerce")

    return wind, solar


# ======================================================
# MAIN
# ======================================================

def main():
    print("🔑 Using ENTSOE key:", API_KEY[:6], "...")

    # 1) Load base CSV
    base = pd.read_csv(IN_CSV)
    if "DELIVERY_MTU" not in base.columns:
        raise ValueError("Input CSV must contain 'DELIVERY_MTU' column.")

    # Parse & localize robustly (DST safe)
    base["DELIVERY_MTU"] = localize_greek_time_safe(base["DELIVERY_MTU"], TZ)

    # Sort for stability
    base = base.sort_values("DELIVERY_MTU").reset_index(drop=True)

    # Target index: exact 15-min timestamps present in your dataset
    idx_15 = pd.DatetimeIndex(base["DELIVERY_MTU"])

    # 2) ENTSO-E client
    client = EntsoePandasClient(api_key=API_KEY)

    # 3) System Load Forecast (DA)
    load_fc = fetch_chunked_monthly(
        lambda s, e: client.query_load_forecast(country_code=AREA, start=s, end=e),
        START, END, "System Load Forecast"
    )
    if load_fc is None or len(load_fc) == 0:
        raise RuntimeError("No System Load forecast data returned from ENTSO-E.")

    load_fc_15 = upsample_ffill_to_target(load_fc, idx_15)

    # 4) Wind & Solar Day-Ahead Forecast
    ws_fc = fetch_chunked_monthly(
        lambda s, e: client.query_wind_and_solar_forecast(country_code=AREA, start=s, end=e, process_type="A01"),
        START, END, "Wind & Solar DA Forecast"
    )
    if ws_fc is None or ws_fc.empty:
        raise RuntimeError("No Wind/Solar DA forecast data returned from ENTSO-E.")

    wind_s, solar_s = extract_wind_solar_series(ws_fc)

    wind_15 = upsample_ffill_to_target(wind_s, idx_15)
    solar_15 = upsample_ffill_to_target(solar_s, idx_15)

    # 5) Merge columns
    base["SystemLoad_DA_forecast"] = load_fc_15.to_numpy()
    base["Wind_DA_forecast"] = wind_15.to_numpy()
    base["Solar_DA_forecast"] = solar_15.to_numpy()

    # 6) Save (keep DELIVERY_MTU as naive Greek-local timestamps, matching your pipeline style)
    base["DELIVERY_MTU"] = base["DELIVERY_MTU"].dt.tz_localize(None)

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    base.to_csv(OUT_CSV, index=False)

    print("✅ DONE")
    print("💾 Saved:", OUT_CSV)


if __name__ == "__main__":
    main()
