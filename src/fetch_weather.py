import argparse
import json
import math
import os
import sys
from datetime import datetime

from dateutil import tz
import numpy as np
import pandas as pd
import requests

ATHENS_TZ = "Europe/Athens"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Variables: ώρα-ώρα (hourly). Θα τις κάνουμε resample σε 15' αν ζητηθεί.
HOURLY_VARS = [
    "temperature_2m",
    "wind_speed_10m",
    "wind_gusts_10m",
    "shortwave_radiation",
    "cloud_cover",
    "precipitation",
    "is_day",
]


def fetch_one(name, lat, lon, start_date, end_date, timezone=ATHENS_TZ):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": timezone,
        "start_date": start_date,
        "end_date": end_date,
    }
    r = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        raise RuntimeError(f"No data returned for {name} ({lat},{lon})")

    df = pd.DataFrame({"time": pd.to_datetime(times)})
    for v in HOURLY_VARS:
        df[v] = hourly.get(v, [np.nan] * len(times))
    df = df.set_index("time").sort_index()
    # Προσθέτουμε location prefix
    df = df.add_prefix(f"{name}__")
    return df


def resample_to_frequency(df, freq: str):
    """Resample από 60' -> freq (π.χ. '15T') με time-aware interpolation."""
    if freq.upper() in ["60T", "1H", "H"]:
        return df  # no-op
    # Χρησιμοποιούμε time-based interpolation
    num_cols = df.select_dtypes(include=[np.number]).columns
    non_num = df.columns.difference(num_cols)
    out = df[num_cols].resample(freq).interpolate(method="time")
    # Για non-numeric κρατάμε forward fill
    if len(non_num) > 0:
        out[non_num] = df[non_num].resample(freq).ffill()
    return out


def build_feature_block(df_all, freq, rolling_windows=(3, 6)):
    """Φτιάχνει aggregates ανά timestep + deltas + rolling std."""
    out = df_all.copy()

    # --- Aggregates across locations ---
    def cols_like(var):
        return [c for c in out.columns if c.endswith(f"__{var}")]

    for var in [
        "wind_speed_10m",
        "wind_gusts_10m",
        "shortwave_radiation",
        "cloud_cover",
        "precipitation",
    ]:
        cols = cols_like(var)
        if cols:
            out[f"AGG__mean__{var}"] = out[cols].mean(axis=1)
            out[f"AGG__median__{var}"] = out[cols].median(axis=1)

    # day/night mask ως mean(is_day)
    cols_day = cols_like("is_day")
    if cols_day:
        out["AGG__mean__is_day"] = out[cols_day].mean(axis=1)

    # --- Deltas ---
    for base in [
        "AGG__mean__wind_speed_10m",
        "AGG__mean__shortwave_radiation",
        "AGG__mean__cloud_cover",
    ]:
        if base in out.columns:
            out[f"{base}__delta1"] = out[base].diff(1)

    # --- Rolling std/mean ---
    for w in rolling_windows:
        for base in ["AGG__mean__wind_speed_10m", "AGG__mean__shortwave_radiation"]:
            if base in out.columns:
                out[f"{base}__rollstd{w}"] = out[base].rolling(w, min_periods=1).std()
                out[f"{base}__rollmean{w}"] = out[base].rolling(w, min_periods=1).mean()

    # --- Calendrical features ---
    idx = out.index
    out["cal_hour"] = idx.hour
    out["cal_dow"] = idx.dayofweek
    out["cal_month"] = idx.month

    return out


def main():
    print("🚀 Script ξεκίνησε κανονικά")

    parser = argparse.ArgumentParser(
        description="Fetch Open-Meteo weather for RES hotspots and produce features CSV."
    )
    parser.add_argument("--locations", default="locations.json", help="JSON file με name -> [lat, lon]")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--freq", default="15T", help="Output frequency: '15T' ή 'H'")
    parser.add_argument("--outdir", default="data/processed", help="Output folder")
    args = parser.parse_args()

    with open(args.locations, "r", encoding="utf-8") as f:
        locs = json.load(f)

    frames = []
    for name, (lat, lon) in locs.items():
        print(f"Fetching {name} ({lat},{lon}) ...")
        df = fetch_one(name, lat, lon, args.start, args.end)
        frames.append(df)

    # Συγχώνευση σε κοινό time index (outer join -> μετά forward-fill μικρά κενά)
    df_all = pd.concat(frames, axis=1).sort_index()
    df_all = df_all.asfreq("H")  # βεβαιωνόμαστε πως είναι ωριαίο grid
    df_all = df_all.ffill(limit=2)

    # Resample σε ζητούμενη συχνότητα (π.χ. 15’)
    df_res = resample_to_frequency(df_all, args.freq)

    # Feature block
    features = build_feature_block(df_res, args.freq)

    os.makedirs(args.outdir, exist_ok=True)
    out_csv = os.path.join(
        args.outdir,
        f"weather_features_{args.freq}_{args.start}_{args.end}.csv",
    )

    print(f"🟢 Θα αποθηκεύσω: {out_csv}")
    features.to_csv(out_csv, index=True, date_format="%Y-%m-%d %H:%M:%S")
    print(f"✅ Done. Saved: {out_csv}")


if __name__ == "__main__":
    main()
