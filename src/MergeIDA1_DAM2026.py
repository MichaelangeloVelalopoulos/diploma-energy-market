import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

IDA1_NAME = "EL-IDA1_MASTER_20251001_20260127.csv"
DAM_NAME  = "EL-DAM_MASTER_20251001_20260127.csv"

OUT_CSV = PROJECT_ROOT / "data" / "processed" / "MERGED" / "EL-IDA1_WITH_DAM_20251001_20260127.csv"


def find_file(root: Path, filename: str) -> Path:
    hits = list(root.rglob(filename))
    if not hits:
        raise FileNotFoundError(f"Could not find '{filename}' anywhere under: {root}")
    # if multiple, take the newest
    hits.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return hits[0]


def parse_delivery_mtu(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    # normalize to 15-min grid for safe alignment
    dt = dt.dt.floor("15min")
    return dt


def find_delivery_col(df: pd.DataFrame) -> str:
    cols = {c.lower().strip(): c for c in df.columns}
    if "delivery_mtu" in cols:
        return cols["delivery_mtu"]
    for c in df.columns:
        cl = c.lower()
        if "delivery" in cl and "mtu" in cl:
            return c
    raise ValueError(f"Could not find DELIVERY_MTU column. Columns={df.columns.tolist()}")


def main():
    ida_path = find_file(PROJECT_ROOT, IDA1_NAME)
    dam_path = find_file(PROJECT_ROOT, DAM_NAME)

    print(f"IDA1: {ida_path}")
    print(f"DAM : {dam_path}")

    ida = pd.read_csv(ida_path)
    dam = pd.read_csv(dam_path)

    ida_mtu = find_delivery_col(ida)
    dam_mtu = find_delivery_col(dam)

    ida["DELIVERY_MTU"] = parse_delivery_mtu(ida[ida_mtu])
    dam["DELIVERY_MTU"] = parse_delivery_mtu(dam[dam_mtu])

    if "DAM_MCP" not in dam.columns:
        raise ValueError(f"DAM_MCP not found in DAM. Columns={dam.columns.tolist()}")

    # keep only what we need from DAM
    dam = dam[["DELIVERY_MTU", "DAM_MCP"]].copy()

    # clean
    ida = ida.dropna(subset=["DELIVERY_MTU"]).copy()
    dam = dam.dropna(subset=["DELIVERY_MTU"]).copy()

    ida = ida.sort_values("DELIVERY_MTU").drop_duplicates(subset=["DELIVERY_MTU"], keep="last")
    dam = dam.sort_values("DELIVERY_MTU").drop_duplicates(subset=["DELIVERY_MTU"], keep="last")

    merged = ida.merge(dam, on="DELIVERY_MTU", how="left").sort_values("DELIVERY_MTU").reset_index(drop=True)

    total = len(merged)
    matched = merged["DAM_MCP"].notna().sum()
    print(f"Rows: {total}")
    print(f"DAM matched: {matched} ({matched/total:.2%})")
    print(f"Range: {merged['DELIVERY_MTU'].min()} -> {merged['DELIVERY_MTU'].max()}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)
    print(f"Wrote: {OUT_CSV}")


if __name__ == "__main__":
    main()
