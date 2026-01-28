# src/fetch_entsoe_res.py

import os
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from entsoe import EntsoePandasClient

API_KEY = os.getenv("ENTSOE_API_KEY")
if not API_KEY:
    raise RuntimeError("ENTSOE_API_KEY environment variable is not set")

print("DEBUG API KEY STARTS WITH:", API_KEY[:6], "...")

AREA = "GR"
TZ = "Europe/Brussels"


def fetch_chunk(client, start, end):
    print(f" Chunk {start} → {end}")
    df = client.query_generation(
        country_code=AREA,
        start=start,
        end=end,
        psr_type=None,
    )
    return df


def main():
    client = EntsoePandasClient(api_key=API_KEY)

    overall_start = pd.Timestamp("2024-06-16 00:00", tz=TZ)
    overall_end   = pd.Timestamp("2024-12-31 23:00", tz=TZ)

    print(" Fetching ACTUAL GENERATION PER TYPE for", AREA)
    print("   Global range:", overall_start, "→", overall_end)

    pieces = []
    cur = overall_start
    while cur < overall_end:
        chunk_end = min(cur + relativedelta(months=1), overall_end)

        try:
            df_chunk = fetch_chunk(client, cur, chunk_end)
            pieces.append(df_chunk)
        except Exception as e:
            print(" Error on chunk", cur, "→", chunk_end)
            print(repr(e))
            if hasattr(e, "response") and e.response is not None:
                print("\n Raw response text from ENTSO-E:")
                print(e.response.text)
            break

        cur = chunk_end

    if not pieces:
        print("No data fetched at all — see errors above.")
        return

    all_df = pd.concat(pieces).sort_index()
    print(" Final shape:", all_df.shape)

    out_path = "data/processed/entsoe_actual_gen_2024_full.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    all_df.to_csv(out_path)
    print(f" Saved to: {out_path}")


if __name__ == "__main__":
    main()
