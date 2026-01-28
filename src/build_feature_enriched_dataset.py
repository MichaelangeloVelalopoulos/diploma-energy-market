# src/build_feature_enriched_dataset.py

import os
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

# ---------------------------------------------------------
# ΡΥΘΜΙΣΕΙΣ
# ---------------------------------------------------------

API_KEY = os.getenv("ENTSOE_API_KEY")
if not API_KEY:
    raise RuntimeError("ENTSOE_API_KEY environment variable ENTSOE_API_KEY is not set")

AREA = "GR"  # Ελλάδα
TZ_ENTSO = "Europe/Brussels"
TZ_LOCAL = "Europe/Athens"

INPUT_CSV = "data/processed/merged_idm_henex_entsoe_2024.csv"          # αυτό που ήδη έχεις
OUTPUT_CSV = "feature_enriched_idm_entsoe_2024.csv"     # ΝΕΟ αρχείο


# ---------------------------------------------------------
# ΒΟΗΘΗΤΙΚΕΣ ΣΥΝΑΡΤΗΣΕΙΣ ENTSO-E
# ---------------------------------------------------------

def fetch_load_series(client: EntsoePandasClient,
                      start: pd.Timestamp,
                      end: pd.Timestamp):
    """
    Κατεβάζει:
      - Actual load (A65, processType=A16)
      - Day-ahead load forecast (A65, processType=A01)
    και επιστρέφει δύο Series με index χρόνο (Europe/Athens).
    """
    print("🚀 Fetching LOAD (actual & DA forecast) from ENTSO-E")
    pieces_actual = []
    pieces_da = []

    cur = start
    while cur < end:
        chunk_end = min(cur + relativedelta(months=1), end)
        print(f"   🔹 Load chunk {cur} → {chunk_end}")
        try:
            # Actual load (processType A16)
            df_actual = client.query_load(
                country_code=AREA,
                start=cur,
                end=chunk_end,
            )
            pieces_actual.append(df_actual)
        except NoMatchingDataError:
            print("      ⚠️ No actual load data for chunk")
        except Exception as e:
            print("      ❌ Error on actual load chunk", repr(e))
            break

        try:
            # Day-ahead load forecast (processType A01)
            df_da = client.query_load_forecast(
                country_code=AREA,
                start=cur,
                end=chunk_end,
                process_type="A01",
            )
            pieces_da.append(df_da)
        except NoMatchingDataError:
            print("       No DA load forecast data for chunk")
        except Exception as e:
            print("       Error on DA load forecast chunk", repr(e))
            break

        cur = chunk_end

    if pieces_actual:
        load_actual = pd.concat(pieces_actual).sort_index()
        # συνήθως έχει μία στήλη μόνο, π.χ. "Load"
        if isinstance(load_actual, pd.DataFrame) and load_actual.shape[1] == 1:
            load_actual = load_actual.iloc[:, 0]
        load_actual.name = "SystemLoad_actual"
    else:
        load_actual = pd.Series(dtype=float, name="SystemLoad_actual")

    if pieces_da:
        load_da = pd.concat(pieces_da).sort_index()
        if isinstance(load_da, pd.DataFrame) and load_da.shape[1] == 1:
            load_da = load_da.iloc[:, 0]
        load_da.name = "SystemLoad_DA_forecast"
    else:
        load_da = pd.Series(dtype=float, name="SystemLoad_DA_forecast")

    return load_actual, load_da


def fetch_crossborder_flows(client: EntsoePandasClient,
                            start: pd.Timestamp,
                            end: pd.Timestamp):
    """
    Χρησιμοποιεί τη συνάρτηση query_physical_crossborder_allborders
    για να πάρει:
      - συνολικές εισαγωγές (import)
      - συνολικές εξαγωγές (export)
    και να υπολογίσει net_import = import - export (σε MW).
    """
    print(" Fetching cross-border physical flows for GR")

    try:
        # Συνολικές εισαγωγές
        df_import = client.query_physical_crossborder_allborders(
            country_code=AREA,
            start=start,
            end=end,
            export=False,
            per_hour=True,
        )
    except NoMatchingDataError:
        print("   No import flows data")
        df_import = pd.DataFrame()
    except Exception as e:
        print("    Error fetching imports:", repr(e))
        df_import = pd.DataFrame()

    try:
        # Συνολικές εξαγωγές
        df_export = client.query_physical_crossborder_allborders(
            country_code=AREA,
            start=start,
            end=end,
            export=True,
            per_hour=True,
        )
    except NoMatchingDataError:
        print("   No export flows data")
        df_export = pd.DataFrame()
    except Exception as e:
        print("   Error fetching exports:", repr(e))
        df_export = pd.DataFrame()

    # Παίρνουμε τη στήλη 'sum' (άθροισμα από όλες τις διασυνδέσεις)
    if not df_import.empty and "sum" in df_import.columns:
        s_import = df_import["sum"].rename("CB_import_MW")
    else:
        s_import = pd.Series(dtype=float, name="CB_import_MW")

    if not df_export.empty and "sum" in df_export.columns:
        s_export = df_export["sum"].rename("CB_export_MW")
    else:
        s_export = pd.Series(dtype=float, name="CB_export_MW")

    # Net import: εισαγωγές - εξαγωγές (θετικό => καθαρός εισαγωγέας)
    net_import = s_import.sub(s_export, fill_value=0.0)
    net_import.name = "CB_net_import_MW"

    return s_import, s_export, net_import


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    print("🔑 Using ENTSOE_API_KEY starting with:", API_KEY[:6], "...")
    client = EntsoePandasClient(api_key=API_KEY)

    # 1. Διαβάζουμε το merged dataset 
    print(f" Loading base dataset: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV, parse_dates=["DELIVERY_MTU"])
    df = df.set_index("DELIVERY_MTU")

    # ΔΙΟΡΘΩΜΕΝΟ tz_localize ΜΕ ambiguous + nonexistent
    df.index = df.index.tz_localize(
        TZ_LOCAL,
        ambiguous="infer",          # λύνει το DST ambiguity του Οκτωβρίου
        nonexistent="shift_forward" # λύνει το DST skip του Μαρτίου
    )

    print("   Base dataset shape:", df.shape)

    # 2. Ορίζουμε χρονικό εύρος για ENTSO-E (σε Europe/Brussels)
    start_local = df.index.min()
    end_local = df.index.max()

    start_entso = start_local.tz_convert(TZ_ENTSO)
    end_entso = end_local.tz_convert(TZ_ENTSO)

    print("⏱ ENTOS-E query range:", start_entso, "→", end_entso)


    # 3. Κατεβάζουμε Load (actual & DA forecast)
    load_actual, load_da = fetch_load_series(client, start_entso, end_entso)

    # 4. Κατεβάζουμε cross-border flows
    cb_import, cb_export, cb_net = fetch_crossborder_flows(client, start_entso, end_entso)

    # 5. Φέρνουμε όλα τα ENTSO-E series στο ίδιο timezone με το df και αφαιρούμε tz
    for s in [load_actual, load_da, cb_import, cb_export, cb_net]:
        if not s.empty:
            s.index = s.index.tz_convert(TZ_LOCAL)

    # 6. JOIN με βάση το index (ώρα)
    # Πρώτα αφαιρούμε timezone από όλα, για να έχουμε "καθαρά" datetime (naive)
    df.index = df.index.tz_localize(None)

    def align_series(s):
        if s.empty:
            # Επιστρέφουμε κενό series με ίδιο index με df
            return pd.Series(index=df.index, dtype=float, name=s.name)

        s = s.copy()
        # κάνουμε το index naive
        s.index = s.index.tz_localize(None)

        # αν υπάρχουν διπλότυπα timestamps, τα μαζεύουμε με μέσο όρο
        s = s.groupby(level=0).mean()

        # τώρα μπορούμε να κάνουμε reindex με ασφάλεια
        s = s.reindex(df.index)

        # κρατάμε και το name
        s.name = s.name or "tmp"
        return s


    load_actual = align_series(load_actual)
    load_da = align_series(load_da)
    cb_import = align_series(cb_import)
    cb_export = align_series(cb_export)
    cb_net = align_series(cb_net)

    # 7. Προσθέτουμε τα ENTSO-E features στο df
    df["SystemLoad_actual"] = load_actual
    df["SystemLoad_DA_forecast"] = load_da
    df["CB_import_MW"] = cb_import
    df["CB_export_MW"] = cb_export
    df["CB_net_import_MW"] = cb_net

    # -----------------------------------------------------
    # 8. Price spread features & ramping
    # -----------------------------------------------------
    print(" Computing price-based features...")

    if "MCP" in df.columns and "DAM_MCP" in df.columns:
        df["Spread_IDM_minus_DAM"] = df["MCP"] - df["DAM_MCP"]
    else:
        print("    MCP and/or DAM_MCP not found, skipping spread feature.")

    if "DAM_MCP" in df.columns:
        df["DAM_MCP_diff_1h"] = df["DAM_MCP"].diff()

    if "MCP" in df.columns:
        df["MCP_diff_1h"] = df["MCP"].diff()

    # -----------------------------------------------------
    # 9. Rolling statistical features (24h & 72h)
    # -----------------------------------------------------
    print(" Computing rolling stats...")

    rolling_targets = []
    if "MCP" in df.columns:
        rolling_targets.append("MCP")
    if "DAM_MCP" in df.columns:
        rolling_targets.append("DAM_MCP")
    if "Spread_IDM_minus_DAM" in df.columns:
        rolling_targets.append("Spread_IDM_minus_DAM")

    for col in rolling_targets:
        df[f"{col}_rollmean_24h"] = df[col].rolling(window=24, min_periods=1).mean()
        df[f"{col}_rollstd_24h"] = df[col].rolling(window=24, min_periods=1).std()
        df[f"{col}_rollmean_72h"] = df[col].rolling(window=72, min_periods=1).mean()
        df[f"{col}_rollstd_72h"] = df[col].rolling(window=72, min_periods=1).std()

    # -----------------------------------------------------
    # 10. Lag features (για καλύτερο convergence του transformer)
    # -----------------------------------------------------
    print("⏪ Computing lag features...")

    def add_lags(col_name: str, lags):
        if col_name not in df.columns:
            print(f"   ⚠️ Column {col_name} not in df, skipping lags.")
            return
        for l in lags:
            df[f"{col_name}_lag{l}"] = df[col_name].shift(l)

    # Lags για spread & load
    add_lags("Spread_IDM_minus_DAM", lags=[1, 2, 3, 24])
    add_lags("SystemLoad_actual", lags=[1, 2, 24])
    add_lags("SystemLoad_DA_forecast", lags=[1, 24])
    add_lags("MCP", lags=[1, 2, 3])
    add_lags("DAM_MCP", lags=[1, 2, 3])

    # -----------------------------------------------------
    # 11. Αποθήκευση σε ΝΕΟ CSV
    # -----------------------------------------------------
    print("💾 Saving feature-enriched dataset...")

    df.to_csv(OUTPUT_CSV, index_label="DELIVERY_MTU")
    print(f"✅ Saved: {OUTPUT_CSV}")
    print("   Final shape:", df.shape)


if __name__ == "__main__":
    main()
