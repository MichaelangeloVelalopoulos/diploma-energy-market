import os, glob, argparse
import pandas as pd
import numpy as np

def read_res_file(path: str) -> pd.DataFrame:
    """
    Διαβάζει RealTimeSCADARES .xls:
    Sheet με header "Date" και στη συνέχεια columns 1..96 (15λεπτα) με τιμές MWh.
    Επιστρέφει DataFrame [timestamp, res_mwh].
    """
    # Διαβάζουμε όλο το sheet χωρίς header για να βρούμε το header row μόνοι μας
    try:
        df = pd.read_excel(path, header=None, sheet_name=0)
    except Exception as e:
        raise RuntimeError(f"Αποτυχία ανάγνωσης {os.path.basename(path)}: {e}")

    # 1) Βρες τη γραμμή που έχει 'Date' στην πρώτη στήλη (case-insensitive)
    hdr_row = None
    for i in range(len(df)):
        v = str(df.iloc[i, 0]).strip().lower()
        if v == "date":
            hdr_row = i
            break
    if hdr_row is None:
        # fallback: ψάξε σε όλη τη γραμμή για 'date'
        for i in range(len(df)):
            row_vals = [str(x).strip().lower() for x in df.iloc[i, :].tolist()]
            if "date" in row_vals:
                hdr_row = i
                break
    if hdr_row is None:
        raise ValueError(f"Δεν βρέθηκε header 'Date' στο {os.path.basename(path)}")

    # 2) Τα labels των 15λέπτων βρίσκονται στη γραμμή hdr_row.
    #    Μαζεύουμε τις στήλες που έχουν labels 1..96 (ως αριθμούς ή strings).
    labels = df.iloc[hdr_row, :].to_list()
    step_cols = []
    step_ids  = []
    for j, lab in enumerate(labels):
        # αγνόησε την πρώτη στήλη (Date)
        if j == 0:
            continue
        if lab is None or (isinstance(lab, float) and np.isnan(lab)):
            continue
        s = str(lab).strip()
        if s.isdigit():
            k = int(s)
            if 1 <= k <= 96:
                step_cols.append(j)
                step_ids.append(k)

    if not step_cols:
        raise ValueError(f"Δεν βρέθηκαν στήλες 1..96 στο header ({os.path.basename(path)})")

    # Ταξινόμηση με βάση τον αριθμό βήματος
    order = np.argsort(step_ids)
    step_cols = [step_cols[i] for i in order]
    step_ids  = [step_ids[i] for i in order]

    # 3) Η γραμμή τιμών (ημερήσιο row) είναι η αμέσως επόμενη του header
    data_row = hdr_row + 1
    if data_row >= len(df):
        raise ValueError(f"Δεν βρέθηκε data row κάτω από το header στο {os.path.basename(path)}")

    # Η πρώτη στήλη της data_row είναι η ημερομηνία
    date_cell = df.iloc[data_row, 0]
    date = pd.to_datetime(date_cell, errors="coerce", dayfirst=True)
    if pd.isna(date):
        # μερικές φορές η ημερομηνία είναι ήδη timestamp/serial – δοκιμάζουμε χωρίς dayfirst
        date = pd.to_datetime(date_cell, errors="coerce")
    if pd.isna(date):
        raise ValueError(f"Μη αναγνώσιμη ημερομηνία στη γραμμή {data_row+1} του {os.path.basename(path)}")

    # 4) Πάρε τις τιμές MWh για τις step_cols
    vals = [pd.to_numeric(df.iloc[data_row, c], errors="coerce") for c in step_cols]
    vals = pd.Series(vals, index=step_ids).sort_index()

    # Φτιάξε ακριβώς 96 βήματα: κόψε ή συμπλήρωσε NaN
    if len(vals) < 96:
        # συμπλήρωση με NaN στα υπόλοιπα βήματα
        full = pd.Series(index=range(1, 97), dtype=float)
        full.loc[vals.index] = vals.values
        vals = full
    elif len(vals) > 96:
        vals = vals.iloc[:96]

    # 5) Δημιουργία χρονικών στιγμών ανά 15'
    times = pd.date_range(start=date.normalize(), periods=96, freq="15min")
    out = pd.DataFrame({"timestamp": times, "res_mwh": vals.values})
    return out


def main():
    ap = argparse.ArgumentParser(description="Parse ADMIE RealTimeSCADARES (.xls) -> 15' CSV (timestamp,res_mwh)")
    ap.add_argument("--raw_dir", default="data/raw/ipto")
    ap.add_argument("--out", default="data/processed/ipto_15min.csv")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.raw_dir, "*.xls")))
    if not files:
        print("❌ Δεν βρέθηκαν .xls στο", args.raw_dir)
        return
    print(f"🔎 Βρέθηκαν {len(files)} αρχεία")

    parts = []
    for fp in files:
        try:
            df = read_res_file(fp)
            parts.append(df)
            print(f"✅ {os.path.basename(fp)} -> {df.shape}")
        except Exception as e:
            print(f"⚠️ Παράλειψη {os.path.basename(fp)} -> {e}")

    if not parts:
        print("❌ Δεν προέκυψαν δεδομένα.")
        return

    out_df = pd.concat(parts).sort_values("timestamp").reset_index(drop=True)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out_df.to_csv(args.out, index=False, date_format="%Y-%m-%d %H:%M:%S")
    print(f"🎉 Saved: {args.out} ({out_df.shape[0]}, {out_df.shape[1]})")


if __name__ == "__main__":
    main()

