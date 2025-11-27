# build_idm_dataset.py
#
# Συνδέει:
#   - HEnEx DAM αρχεία (ημερήσια αγορά)
#   - HEnEx IDM αρχεία (IDA1 / IDA2 / IDA3)
#   - Weather features (από Open-Meteo)
#
# και φτιάχνει ένα ενιαίο CSV για training μοντέλων.
#
# ΣΗΜΑΝΤΙΚΟ:
# - Δεν πειράζουμε ΚΑΘΟΛΟΥ τα column names των IDA αρχείων.
# - Τα DAM columns μπαίνουν με prefix "DAM_" (εκτός από DELIVERY_MTU).
# - Τα weather columns μένουν όπως είναι στο CSV.
#
# Run:
#   python src/build_idm_dataset.py \
#       --results_root data/raw/henex/Results2024 \
#       --weather_csv data/processed/weather_features_15T_2024-06-16_2024-12-31.csv \
#       --start_date 2024-06-16 \
#       --end_date 2024-12-31 \
#       --out data/processed/idm_dataset_2024.csv

import argparse
import os
from datetime import datetime, date

import pandas as pd


# ---------- Helpers ----------

def parse_fname_date(fname: str) -> date | None:
    """
    Παίρνει filename π.χ. '20240616_EL-DAM_Results_EN_v01.xlsx'
    και επιστρέφει date(2024,6,16).
    """
    try:
        base = os.path.basename(fname)
        d_str = base[:8]
        return datetime.strptime(d_str, "%Y%m%d").date()
    except Exception:
        return None


def load_dam_folder(dam_folder: str,
                    start_date: date,
                    end_date: date) -> pd.DataFrame:
    """
    Διαβάζει ΟΛΑ τα DAM αρχεία στο dam_folder, φιλτράρει στο date range
    και επιστρέφει:
        columns: ['DELIVERY_MTU', <όλα τα υπόλοιπα με prefix DAM_>]
    """
    print(f"📂 Loading DAM from {dam_folder}")
    all_frames: list[pd.DataFrame] = []

    files = sorted(
        f for f in os.listdir(dam_folder)
        if f.endswith(".xlsx")
    )

    print(f"  → Found {len(files)} DAM files")

    for fname in files:
        fdate = parse_fname_date(fname)
        if fdate is None:
            continue
        # Φιλτράρουμε με βάση filename date (ημέρα παράδοσης)
        if fdate < start_date or fdate > end_date:
            continue

        full_path = os.path.join(dam_folder, fname)
        print(f"    • Reading DAM file: {fname}")
        df = pd.read_excel(full_path, engine="openpyxl")

        # Αν δεν υπάρχει DELIVERY_MTU, δεν μπορούμε να κάνουμε merge
        if "DELIVERY_MTU" not in df.columns:
            print(f"      ⚠️ No DELIVERY_MTU in {fname}, skipping.")
            continue

        # Σε datetime
        df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce")
        df = df.dropna(subset=["DELIVERY_MTU"])

        # Επιπλέον φίλτρο με βάση πραγματική ημερομηνία
        d_col = df["DELIVERY_MTU"].dt.date
        df = df[(d_col >= start_date) & (d_col <= end_date)]

        if df.empty:
            continue

        all_frames.append(df)

    if not all_frames:
        print("  🔍 Raw DAM rows: 0")
        return pd.DataFrame(columns=["DELIVERY_MTU"])

    dam = pd.concat(all_frames, ignore_index=True)
    print(f"  🔍 Raw DAM rows: {len(dam)}")

    # Βάζουμε prefix DAM_ σε όλα τα columns εκτός από DELIVERY_MTU
    rename_map = {
        c: f"DAM_{c}"
        for c in dam.columns
        if c != "DELIVERY_MTU"
    }
    dam = dam.rename(columns=rename_map)

   # --- Fix: group only numeric columns ---
    numeric_cols = dam.select_dtypes(include=["number"]).columns.tolist()

    agg_dict = {c: "mean" for c in numeric_cols}

    dam_grouped = (
        dam.groupby("DELIVERY_MTU", as_index=False)
       .agg(agg_dict)
    )


    print(
        f"  ✅ After grouping: rows = {len(dam_grouped)} | "
        f"unique MTUs = {dam_grouped['DELIVERY_MTU'].nunique()}"
    )
    return dam_grouped


def load_ida_folder(folder: str,
                    auction_name: str,
                    start_date: date,
                    end_date: date) -> pd.DataFrame:
    """
    Διαβάζει ΟΛΑ τα IDA αρχεία από folder (IDA1 ή IDA2 ή IDA3),
    προσθέτει στήλη 'AUCTION' (π.χ. 'IDA1') και φιλτράρει στο date range.
    ΔΕΝ αλλάζουμε τα ονόματα των columns από HenEx.
    """
    print(f"📂 Loading {auction_name} from {folder}")
    files = sorted(
        f for f in os.listdir(folder)
        if f.endswith(".xlsx")
    )
    print(f"  → Found {len(files)} files in {auction_name}")

    frames: list[pd.DataFrame] = []

    for fname in files:
        fdate = parse_fname_date(fname)
        if fdate is None:
            continue
        if fdate < start_date or fdate > end_date:
            continue

        full_path = os.path.join(folder, fname)
        print(f"    • Reading {auction_name} file: {fname}")
        df = pd.read_excel(full_path, engine="openpyxl")

        if "DELIVERY_MTU" not in df.columns:
            print(f"      ⚠️ No DELIVERY_MTU in {fname}, skipping.")
            continue

        df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce")
        df = df.dropna(subset=["DELIVERY_MTU"])

        d_col = df["DELIVERY_MTU"].dt.date
        df = df[(d_col >= start_date) & (d_col <= end_date)]
        if df.empty:
            continue

        df["AUCTION"] = auction_name
        frames.append(df)

    if not frames:
        print(f"  ⚠️ No rows loaded for {auction_name}")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    print(f"  ✅ Loaded {len(out)} rows for {auction_name}")
    return out


def load_all_idas(idas_root: str,
                  start_date: date,
                  end_date: date) -> pd.DataFrame:
    """
    Συνδέει IDA1 / IDA2 / IDA3 σε ένα DataFrame.
    """
    ida1 = load_ida_folder(os.path.join(idas_root, "IDA1"), "IDA1", start_date, end_date)
    ida2 = load_ida_folder(os.path.join(idas_root, "IDA2"), "IDA2", start_date, end_date)
    ida3 = load_ida_folder(os.path.join(idas_root, "IDA3"), "IDA3", start_date, end_date)

    frames = [df for df in [ida1, ida2, ida3] if not df.empty]
    if not frames:
        print("⚠️ No IDA rows at all.")
        return pd.DataFrame()

    all_ida = pd.concat(frames, ignore_index=True)
    print(f"📊 Total IDA rows: {len(all_ida)}")
    return all_ida


def load_weather(weather_csv: str) -> pd.DataFrame:
    """
    Φορτώνει το weather_features CSV.

    Περιμένουμε:
      - Η πρώτη στήλη να είναι datetime index (το timestamp από fetch_weather).
    Το κάνουμε resample σε 1H και μετονομάζουμε αυτή τη στήλη σε DELIVERY_MTU
    για merge με IDM/DAM.
    """
    print(f"📂 Loading weather from {weather_csv}")
    w = pd.read_csv(weather_csv, parse_dates=[0])

    time_col = w.columns[0]
    w = w.rename(columns={time_col: "DELIVERY_MTU"})
    w = w.set_index("DELIVERY_MTU").sort_index()

    # Resample σε ωριαίο (1H) για να ταιριάζει με IDM/DAM
    w_hourly = w.resample("1H").mean().reset_index()

    print(f"✅ Weather hourly shape: {w_hourly.shape}")
    return w_hourly


# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(
        description="Build IDM dataset by merging HEnEx DAM, IDA1/2/3 and weather."
    )
    parser.add_argument(
        "--results_root",
        required=True,
        help="π.χ. data/raw/henex/Results2024",
    )
    parser.add_argument(
        "--weather_csv",
        required=True,
        help="π.χ. data/processed/weather_features_15T_2024-06-16_2024-12-31.csv",
    )
    parser.add_argument(
        "--start_date",
        required=True,
        help="YYYY-MM-DD (inclusive), π.χ. 2024-06-16",
    )
    parser.add_argument(
        "--end_date",
        required=True,
        help="YYYY-MM-DD (inclusive), π.χ. 2024-12-31",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output CSV path, π.χ. data/processed/idm_dataset_2024.csv",
    )

    args = parser.parse_args()

    start_date = datetime.fromisoformat(args.start_date).date()
    end_date = datetime.fromisoformat(args.end_date).date()

    dam_root = os.path.join(args.results_root, "DAM")
    idas_root = os.path.join(args.results_root, "IDAs")

    # 1) DAM
    dam_all = load_dam_folder(dam_root, start_date, end_date)

    # 2) IDA1/2/3
    ida_all = load_all_idas(idas_root, start_date, end_date)

    if ida_all.empty:
        print("❌ No IDA data loaded, cannot build dataset.")
        return

    # 3) Weather
    weather = load_weather(args.weather_csv)

    # ----- MERGE -----
    print(f"📅 IDA rows after date filter [{start_date}, {end_date}]: {ida_all.shape}")

    # Merge IDA + DAM (στο DELIVERY_MTU)
    if not dam_all.empty:
        merged = ida_all.merge(
            dam_all,
            on="DELIVERY_MTU",
            how="left",  # κρατάμε όλες τις IDA εγγραφές
        )
        print(f"🔗 After merging with DAM: {merged.shape}")
    else:
        print("⚠️ No DAM data, continuing with IDA only.")
        merged = ida_all.copy()

    # Merge με weather
    merged = merged.merge(
        weather,
        on="DELIVERY_MTU",
        how="left",
    )
    print(f"🌦 After merging with weather: {merged.shape}")

    # Save
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    merged.to_csv(args.out, index=False)
    print(f"✅ Saved final IDM dataset to: {args.out}")


if __name__ == "__main__":
    main()
