import argparse
import pandas as pd
import os

def main():
    ap = argparse.ArgumentParser(description="Merge weather features και IPTO RES data σε κοινό CSV")
    ap.add_argument("--weather", required=True, help="CSV με weather features (π.χ. weather_features_15min...)")
    ap.add_argument("--ipto", required=True, help="CSV με IPTO RES data (π.χ. ipto_15min.csv)")
    ap.add_argument("--out", default="data/processed/dataset_weather_ipto.csv", help="Output CSV")
    args = ap.parse_args()

    # Διαβάζουμε τα δύο datasets
    print(f"🌦️ Διαβάζω weather: {args.weather}")
    weather = pd.read_csv(args.weather, parse_dates=["time"])
    weather = weather.rename(columns={"time": "timestamp"})

    print(f"⚡ Διαβάζω IPTO RES: {args.ipto}")
    ipto = pd.read_csv(args.ipto, parse_dates=["timestamp"])

    # Κάνουμε merge με βάση το timestamp (inner join για να ταιριάζουν τα 15')
    print("🔄 Συγχώνευση δεδομένων...")
    merged = pd.merge_asof(
        weather.sort_values("timestamp"),
        ipto.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("8min")
    )

    # Αφαιρούμε NaNs όπου δεν υπάρχει τιμή RES
    merged = merged.dropna(subset=["res_mwh"])

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    merged.to_csv(args.out, index=False, date_format="%Y-%m-%d %H:%M:%S")

    print(f"✅ Αποθηκεύτηκε: {args.out} ({merged.shape[0]} γραμμές, {merged.shape[1]} στήλες)")

if __name__ == "__main__":
    main()
