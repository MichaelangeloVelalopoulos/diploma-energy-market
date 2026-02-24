from __future__ import annotations

import re
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]  
MERGED_DIR = PROJECT_ROOT / "data" / "processed" / "MERGED"

FINAL_PATH = MERGED_DIR / "Final2026.csv"
OUT_PATH = MERGED_DIR / "Final2026_with_IDA2_IDA3.csv"

RAW_BASE = PROJECT_ROOT / "data" / "processed" / "henex_ida_results" / "raw"
IDA2_DIR = RAW_BASE / "IDA2"
IDA3_DIR = RAW_BASE / "IDA3"

KEY_COL = "DELIVERY_MTU"  
MCP_COL_CANDIDATES = ["MCP", "Market Clearing Price", "MARKET_CLEARING_PRICE"]

KEEP_PUBTIME = False


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    cols = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols:
            return cols[key]
    raise ValueError(f"Δεν βρέθηκε καμία από τις στήλες {candidates} μέσα στις: {list(df.columns)}")


def _normalize_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.tz_localize(None)


def load_ida_folder(folder: Path, auction_name: str) -> pd.DataFrame:
    """
    Διαβάζει όλα τα xlsx ενός auction και επιστρέφει DF με:
    [DELIVERY_MTU, MCP_<auction>] (και προαιρετικά PUB_TIME_<auction>)
    """
    if not folder.exists():
        raise FileNotFoundError(f"Δεν βρέθηκε folder: {folder}")

    files = sorted(folder.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"Δεν βρέθηκαν .xlsx στο: {folder}")

    out_rows = []
    for fp in files:
        df = pd.read_excel(fp)

        if KEY_COL not in df.columns:
            # Μερικά αρχεία μπορεί να έχουν ελαφρώς διαφορετικό όνομα
            # Δοκίμασε fallback
            alt = None
            for c in df.columns:
                if c.strip().lower() in ("delivery_mtu", "delivery", "deliverydatetime", "delivery_date"):
                    alt = c
                    break
            if alt is None:
                raise ValueError(f"[{auction_name}] Στο {fp.name} δεν βρέθηκε στήλη {KEY_COL}. Στήλες: {list(df.columns)}")
            key_col = alt
        else:
            key_col = KEY_COL

        mcp_col = _pick_col(df, MCP_COL_CANDIDATES)

        # pub_time αν υπάρχει
        pub_col = None
        for c in df.columns:
            if c.strip().lower() in ("pub_time", "publication_time", "published_time"):
                pub_col = c
                break

        tmp = df[[key_col, mcp_col]].copy()
        tmp.columns = [KEY_COL, "MCP"]

        tmp[KEY_COL] = _normalize_datetime(tmp[KEY_COL])

        # Drop invalid keys
        tmp = tmp.dropna(subset=[KEY_COL])

        # Σε περίπτωση που υπάρχουν duplicates μέσα στο ίδιο αρχείο:
        # κρατάμε το τελευταίο 
        tmp = tmp.sort_values(KEY_COL).drop_duplicates(subset=[KEY_COL], keep="last")

        if KEEP_PUBTIME and pub_col is not None:
            tmp["PUB_TIME"] = _normalize_datetime(df.loc[tmp.index, pub_col])
        else:
            tmp["PUB_TIME"] = pd.NaT

        tmp["SOURCE_FILE"] = fp.name
        out_rows.append(tmp)

    all_df = pd.concat(out_rows, ignore_index=True)

    # Αν ο ίδιος DELIVERY_MTU εμφανίζεται σε πολλά files (π.χ. revisions),
    # κρατάμε το τελευταίο με βάση PUB_TIME, αλλιώς με βάση σειρά αρχείων.
    if KEEP_PUBTIME and all_df["PUB_TIME"].notna().any():
        all_df = all_df.sort_values([KEY_COL, "PUB_TIME"]).drop_duplicates(subset=[KEY_COL], keep="last")
    else:
        # fallback: κρατάει το τελευταίο που διαβάστηκε
        all_df = all_df.drop_duplicates(subset=[KEY_COL], keep="last")

    all_df = all_df[[KEY_COL, "MCP"]].rename(columns={"MCP": f"MCP_{auction_name}"})

    return all_df


def main():
    # 1) Load base Final2026
    base = pd.read_csv(FINAL_PATH)
    if KEY_COL not in base.columns:
        raise ValueError(f"Το {FINAL_PATH} δεν έχει στήλη {KEY_COL}. Στήλες: {list(base.columns)}")

    base[KEY_COL] = _normalize_datetime(base[KEY_COL])
    base = base.dropna(subset=[KEY_COL])

    # 2) Load IDA2/IDA3 MCP time series
    ida2 = load_ida_folder(IDA2_DIR, "IDA2")
    ida3 = load_ida_folder(IDA3_DIR, "IDA3")

    # 3) Merge (left join για να μη χάσεις γραμμές του Final2026)
    merged = base.merge(ida2, on=KEY_COL, how="left").merge(ida3, on=KEY_COL, how="left")

    # 4) Save new file (χωρίς να πειράξεις το Final2026.csv)
    merged.to_csv(OUT_PATH, index=False)

    # 5) Mini-report
    print("✅ Done")
    print(f"Input base: {FINAL_PATH}")
    print(f"IDA2 files: {len(list(IDA2_DIR.glob('*.xlsx')))}")
    print(f"IDA3 files: {len(list(IDA3_DIR.glob('*.xlsx')))}")
    print(f"Output:     {OUT_PATH}")
    print("Coverage:")
    print("  IDA2 non-null:", merged["MCP_IDA2"].notna().mean())
    print("  IDA3 non-null:", merged["MCP_IDA3"].notna().mean())


if __name__ == "__main__":
    main()