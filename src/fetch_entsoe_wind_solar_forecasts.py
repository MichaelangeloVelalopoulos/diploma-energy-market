# src/fetch_entsoe_wind_solar_forecasts.py

import os
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

# -------------------------------------------------------------------
# Ρυθμίσεις
# -------------------------------------------------------------------

API_KEY = os.getenv("ENTSOE_API_KEY")
if not API_KEY:
    raise RuntimeError("ENTSOE_API_KEY environment variable is not set")

AREA = "GR"  # Ελλάδα
# Τα timestamps θα τα περνάμε σε Europe/Brussels (ENTSO-E style),
# αλλά ο client θα τα γυρίζει στο timezone της χώρας (Europe/Athens για GR)
TZ = "Europe/Brussels"

# Διάστημα που θέλεις να κατεβάσεις (προσαρμόζεις όπως θέλεις)
OVERALL_START = pd.Timestamp("2024-06-16 00:00", tz=TZ)
OVERALL_END   = pd.Timestamp("2024-12-31 23:00", tz=TZ)

OUT_PATH = "data/processed/entsoe_wind_solar_forecasts_delta_2024.csv"


# -------------------------------------------------------------------
# Helper για chunked fetch (ανά μήνα) για να αποφεύγουμε limits
# -------------------------------------------------------------------

def fetch_da_wind_solar_chunked(client: EntsoePandasClient,
                                start: pd.Timestamp,
                                end: pd.Timestamp) -> pd.DataFrame:
    """
    Day-Ahead wind & solar forecast (documentType=A69, processType=A01)
    Επιστρέφει DataFrame με index χρόνο και στήλες Solar / Wind κτλ.
    """
    print("🚀 Fetching DAY-AHEAD wind & solar forecast for", AREA)
    print("   Global range:", start, "→", end)

    pieces = []
    cur = start
    while cur < end:
        chunk_end = min(cur + relativedelta(months=1), end)
        print(f"   🔹 DA chunk {cur} → {chunk_end}")
        try:
            df_chunk = client.query_wind_and_solar_forecast(
                country_code=AREA,
                start=cur,
                end=chunk_end,
                process_type="A01",   # Day-Ahead
            )
            pieces.append(df_chunk)
        except NoMatchingDataError:
            # Καμία διαθέσιμη πρόβλεψη σε αυτό το range
            print("      ⚠️ No DA data for chunk", cur, "→", chunk_end)
        except Exception as e:
            print("      ❌ Error on DA chunk", cur, "→", chunk_end)
            print(repr(e))
            break

        cur = chunk_end

    if not pieces:
        print("⚠️ No DA forecast data fetched at all.")
        return pd.DataFrame()

    da_df = pd.concat(pieces).sort_index()
    print("   ✅ Final DA shape:", da_df.shape)
    return da_df


def fetch_id_wind_solar_chunked(client: EntsoePandasClient,
                                start: pd.Timestamp,
                                end: pd.Timestamp) -> pd.DataFrame:
    """
    Intraday wind & solar forecast (documentType=A69, processType=A40)
    μέσω του convenience method query_intraday_wind_and_solar_forecast
    """
    print("🚀 Fetching INTRADAY wind & solar forecast for", AREA)
    print("   Global range:", start, "→", end)

    pieces = []
    cur = start
    while cur < end:
        chunk_end = min(cur + relativedelta(months=1), end)
        print(f"   🔹 ID chunk {cur} → {chunk_end}")
        try:
            df_chunk = client.query_intraday_wind_and_solar_forecast(
                country_code=AREA,
                start=cur,
                end=chunk_end,
            )
            pieces.append(df_chunk)
        except NoMatchingDataError:
            print("      ⚠️ No ID data for chunk", cur, "→", chunk_end)
        except Exception as e:
            print("      ❌ Error on ID chunk", cur, "→", chunk_end)
            print(repr(e))
            break

        cur = chunk_end

    if not pieces:
        print("⚠️ No INTRADAY forecast data fetched at all.")
        return pd.DataFrame()

    id_df = pd.concat(pieces).sort_index()
    print("   ✅ Final ID shape:", id_df.shape)
    return id_df


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    print("🔑 Using ENTSOE_API_KEY starting with:", API_KEY[:6], "...")
    client = EntsoePandasClient(api_key=API_KEY)

    # 1) DA forecast (A69, A01)
    da_df = fetch_da_wind_solar_chunked(client, OVERALL_START, OVERALL_END)

    # 2) Intraday forecast (A69, A40)
    id_df = fetch_id_wind_solar_chunked(client, OVERALL_START, OVERALL_END)

    if da_df.empty or id_df.empty:
        print("⚠️ Either DA or ID forecast is empty. Aborting delta computation.")
        return

    # Φροντίζουμε να έχουν κοινό index resolution / τακτοποιημένα
    da_df = da_df.sort_index()
    id_df = id_df.sort_index()

    # Για να αποφύγουμε περίεργα, κρατάμε μόνο το κοινό χρονικό διάστημα
    common_index = da_df.index.intersection(id_df.index)
    da_aligned = da_df.loc[common_index]
    id_aligned = id_df.loc[common_index]

    # Προσθέτουμε suffixes για να ξεχωρίζουν
    da_aligned = da_aligned.add_suffix("_DA")
    id_aligned = id_aligned.add_suffix("_ID")

    combined = pd.concat([da_aligned, id_aligned], axis=1)

    # Βγάζουμε τα base names των τεχνολογιών από τις DA στήλες
    techs = [col.replace("_DA", "") for col in combined.columns if col.endswith("_DA")]

    # Υπολογίζουμε deltas: ID - DA
    for tech in techs:
        da_col = f"{tech}_DA"
        id_col = f"{tech}_ID"
        if da_col in combined.columns and id_col in combined.columns:
            combined[f"{tech}_DELTA"] = combined[id_col] - combined[da_col]

    print("✅ Combined shape (DA + ID + DELTAs):", combined.shape)

    # Αποθήκευση
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    combined.to_csv(OUT_PATH)
    print(f"💾 Saved combined DA/ID wind & solar forecasts with deltas to:\n   {OUT_PATH}")


if __name__ == "__main__":
    main()
