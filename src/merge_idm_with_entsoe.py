import pandas as pd

# -------- 1. Φόρτωση HEnEx IDM dataset (ΒΑΣΗ) --------
df_idm = pd.read_csv("data/processed/idm_dataset_2024.csv", parse_dates=["DELIVERY_MTU"])

# Βάζουμε index την ώρα παράδοσης, ΧΩΡΙΣ να αλλάξουμε τιμές ή labels
df_idm = df_idm.set_index("DELIVERY_MTU")

# Κρατάμε αντίγραφο των στηλών για να ξέρουμε ποια ήταν τα original labels
idm_columns_original = df_idm.columns.tolist()

print("IDM shape:", df_idm.shape)

# -------- 2. Φόρτωση ENTSO-E actual generation --------
df_actual = pd.read_csv("data/processed/entsoe_actual_gen_2024_full.csv", parse_dates=[0], index_col=0)

# Το ENTSO-E έχει timezone. Το φέρνουμε σε Europe/Athens και μετά αφαιρούμε το tz,
# ώστε το index του να ταιριάζει με αυτό του HEnEx (που είναι naive timestamps).
df_actual.index = pd.to_datetime(df_actual.index, utc=True).tz_convert("Europe/Athens").tz_localize(None)

print("Actual generation shape:", df_actual.shape)

# -------- 3. Φόρτωση ENTSO-E wind/solar forecasts & deltas --------
df_forecasts = pd.read_csv("data/processed/entsoe_wind_solar_forecasts_delta_2024.csv", parse_dates=[0], index_col=0)
df_forecasts.index = pd.to_datetime(df_forecasts.index, utc=True).tz_convert("Europe/Athens").tz_localize(None)

print("Forecasts shape:", df_forecasts.shape)

# -------- 4. Merge: HEnEx ως βάση, ENTSO-E προσαρμόζεται --------
# LEFT JOIN => όλες οι ώρες του HEnEx μένουν, οι ENTSO-E μπαίνουν όπου ταιριάζει η ώρα
df_merged = df_idm.join(df_actual, how="left", rsuffix="_ACT")
df_merged = df_merged.join(df_forecasts, how="left", rsuffix="_FC")

print("Merged shape:", df_merged.shape)

# -------- 5. Optional: derived features (χωρίς να πειράξουμε HEnEx labels) --------
if "Solar" in df_merged.columns and "Solar_DA" in df_merged.columns:
    df_merged["Solar_error"] = df_merged["Solar"] - df_merged["Solar_DA"]

if "Wind Onshore" in df_merged.columns and "Wind Onshore_DA" in df_merged.columns:
    df_merged["Wind_error"] = df_merged["Wind Onshore"] - df_merged["Wind Onshore_DA"]

# -------- 6. Έλεγχος ότι τα HEnEx labels έμειναν ίδια --------
assert all(col in df_merged.columns for col in idm_columns_original), \
    "Κάποια original HEnEx στήλη λείπει μετά το merge!"

# βάζουμε τις HEnEx στήλες πρώτες και μετά τις ENTSO-E
other_cols = [c for c in df_merged.columns if c not in idm_columns_original]
df_merged = df_merged[idm_columns_original + other_cols]

out_path = "merged_idm_henex_entsoe_2024.csv"
df_merged.to_csv(out_path)
print("✅ Saved merged dataset to:", out_path)
