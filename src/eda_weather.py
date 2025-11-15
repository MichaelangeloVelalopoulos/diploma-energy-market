import pandas as pd
import matplotlib.pyplot as plt

# 📂 διάβασε τα δεδομένα που έβγαλες από το API
df = pd.read_csv("data/processed/weather_features_15min_2025-10-01_2025-10-07.csv", parse_dates=["time"])
df.set_index("time", inplace=True)

# 👀 δείξε τις πρώτες γραμμές
print("📄 Πρώτες γραμμές δεδομένων:")
print(df.head(3))
print("\nΣυνολικές στήλες:", len(df.columns))
print("Διάστημα ημερομηνιών:", df.index.min(), "→", df.index.max())

# 📈 γράφημα για να δείξεις στους καθηγητές
ax = df["AGG__mean__wind_speed_10m"].plot(
    figsize=(10, 4),
    title="Μέσος άνεμος στα RES-clusters (15')"
)
ax.set_xlabel("")
ax.set_ylabel("m/s")
plt.tight_layout()
plt.show()
