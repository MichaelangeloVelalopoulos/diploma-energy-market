import os
import requests
import pandas as pd

# ===============================
# Paths
# ===============================
OUTPUT_DIR = "/data/processed"
OUTPUT_FILE = "forecasted_weather.csv"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===============================
# Representative central coordinates
# ===============================
REGIONS = {
    "Evia": {"lat": 38.55, "lon": 23.85},
    "Peloponnese": {"lat": 37.50, "lon": 22.37},
    "Thrace": {"lat": 41.12, "lon": 25.40},
    "Crete": {"lat": 35.24, "lon": 24.81},
}

DATE = "2024-06-16"
BASE_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# ===============================
# Fetch function
# ===============================
def fetch_wind_forecast(lat: float, lon: float, date: str, timezone: str = "Europe/Athens"):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date,
        "end_date": date,
        "hourly": "wind_speed_10m,wind_speed_80m",
        "timezone": timezone,
        # "windspeed_unit": "ms",  # optional
    }

    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    hourly = data.get("hourly", {})
    df = pd.DataFrame({
        "time": pd.to_datetime(hourly.get("time", [])),
        "wind_speed_10m": hourly.get("wind_speed_10m", []),
        "wind_speed_80m": hourly.get("wind_speed_80m", []),
    })

    df["latitude"] = data.get("latitude", lat)
    df["longitude"] = data.get("longitude", lon)

    return df

# ===============================
# Collect data
# ===============================
all_dfs = []

for region, coord in REGIONS.items():
    df_region = fetch_wind_forecast(coord["lat"], coord["lon"], DATE)
    df_region.insert(0, "region", region)
    all_dfs.append(df_region)

df_all = pd.concat(all_dfs, ignore_index=True)

# ===============================
# Save to disk
# ===============================
df_all.to_csv(OUTPUT_PATH, index=False)

# ===============================
# Sanity check
# ===============================
print(df_all.head(10))
print(f"\nSaved to: {OUTPUT_PATH}")
print(f"Rows: {len(df_all)} | Regions: {df_all['region'].nunique()}")
