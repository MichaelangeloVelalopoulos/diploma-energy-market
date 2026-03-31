from __future__ import annotations

import re
import sys
import warnings
from pathlib import Path

import pandas as pd


warnings.filterwarnings("ignore", message="Workbook contains no default style")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = PROJECT_ROOT / "data" / "processed" / "HENEX" / "raw"
OUT_DIR = PROJECT_ROOT / "data" / "processed" / "HENEX" / "processed"

DATE_FROM = "20251001"
DATE_TO = "20260330"

IDA_TAGS = ["IDA1", "IDA2", "IDA3"]
IDA_PATTERNS = {
    ida_tag: re.compile(
        rf"^(?P<date>\d{{8}})_EL-{ida_tag}_Results_EN_v(?P<version>\d{{2}})\.xlsx$",
        re.IGNORECASE,
    )
    for ida_tag in IDA_TAGS
}
DAM_PATTERNS = [
    re.compile(r"^(?P<date>\d{8})_EL-DAM_Results_EN_v(?P<version>\d{2})\.xlsx$", re.IGNORECASE),
    re.compile(r"^(?P<date>\d{8})_EL-DAM_ResultsSummary_EN_v(?P<version>\d{2})\.xlsx$", re.IGNORECASE),
]


def within_range(date_text: str) -> bool:
    return DATE_FROM <= date_text <= DATE_TO


def pick_best_file_per_day(paths: list[Path], matcher) -> dict[str, Path]:
    best: dict[str, tuple[int, float, Path]] = {}

    for path in paths:
        match = matcher(path)
        if not match:
            continue

        date_text = match.group("date")
        if not within_range(date_text):
            continue

        version = int(match.group("version"))
        mtime = path.stat().st_mtime

        current = best.get(date_text)
        if current is None or (version, mtime) >= (current[0], current[1]):
            best[date_text] = (version, mtime, path)

    return {date_text: data[2] for date_text, data in best.items()}


def match_ida_filename(ida_tag: str, path: Path):
    return IDA_PATTERNS[ida_tag].match(path.name)


def match_dam_filename(path: Path):
    for pattern in DAM_PATTERNS:
        match = pattern.match(path.name)
        if match:
            return match
    return None


def read_market_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    df.columns = [str(column).strip() for column in df.columns]
    return df


def reshape_ida_results(df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "TARGET",
        "BIDDING_ZONE_DESCR",
        "DDAY",
        "DELIVERY_MTU",
        "DELIVERY_DURATION",
        "SORT",
        "PUB_TIME",
        "VER",
        "SIDE_DESCR",
        "ASSET_DESCR",
        "CLASSIFICATION",
        "MCP",
        "TOTAL_TRADES",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    df = df.copy()
    df["DDAY"] = pd.to_datetime(df["DDAY"].astype(str), format="%Y%m%d", errors="coerce")
    df["DELIVERY_MTU"] = pd.to_datetime(df["DELIVERY_MTU"], errors="coerce")

    side = df["SIDE_DESCR"].astype(str).str.strip()
    asset = df["ASSET_DESCR"].astype(str).str.strip()
    classification = df["CLASSIFICATION"].astype(str).str.strip()
    df["SERIES_ID"] = (
        side + "__" + asset + "__" + classification
    ).str.replace(r"[^A-Za-z0-9]+", "_", regex=True).str.strip("_")

    meta = [
        "TARGET",
        "BIDDING_ZONE_DESCR",
        "DDAY",
        "DELIVERY_MTU",
        "DELIVERY_DURATION",
        "SORT",
        "PUB_TIME",
        "VER",
    ]

    grouped = (
        df.groupby(meta + ["SERIES_ID"], as_index=False)
        .agg({"MCP": "mean", "TOTAL_TRADES": "sum"})
    )

    wide_mcp = grouped.pivot(index=meta, columns="SERIES_ID", values="MCP")
    wide_trades = grouped.pivot(index=meta, columns="SERIES_ID", values="TOTAL_TRADES")

    wide_mcp.columns = [f"MCP__{column}" for column in wide_mcp.columns]
    wide_trades.columns = [f"TRADES__{column}" for column in wide_trades.columns]

    combined = pd.concat([wide_mcp, wide_trades], axis=1).reset_index()

    mcp_columns = [column for column in combined.columns if column.startswith("MCP__")]
    combined["MCP"] = combined[mcp_columns].bfill(axis=1).iloc[:, 0]
    return combined


def merge_ida_market(ida_tag: str) -> None:
    market_dir = RAW_ROOT / ida_tag
    if not market_dir.exists():
        raise FileNotFoundError(f"Missing raw folder: {market_dir}")

    chosen = pick_best_file_per_day(
        sorted(market_dir.glob("*.xlsx")),
        lambda path: match_ida_filename(ida_tag, path),
    )
    if not chosen:
        raise FileNotFoundError(f"No files found for {ida_tag} in {market_dir}")

    frames = []
    for date_text in sorted(chosen):
        path = chosen[date_text]
        try:
            wide = reshape_ida_results(read_market_xlsx(path))
            wide["SOURCE_FILE"] = path.name
            frames.append(wide)
            print(f"[{ida_tag}] merged {date_text} <- {path.name}")
        except Exception as exc:
            print(f"[{ida_tag}] failed {date_text} <- {path.name} | {exc}", file=sys.stderr)

    if not frames:
        raise RuntimeError(f"No {ida_tag} files were merged successfully.")

    full = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["DDAY", "DELIVERY_MTU", "SORT"])
        .drop_duplicates(subset=["DELIVERY_MTU"], keep="last")
        .reset_index(drop=True)
    )

    out_path = OUT_DIR / f"EL-{ida_tag}_Results_{DATE_FROM}_{DATE_TO}.csv"
    full.to_csv(out_path, index=False)
    print(f"[{ida_tag}] wrote {out_path}")


def find_mkt_sheet_and_row(xl: pd.ExcelFile) -> tuple[str, int]:
    def normalize(value) -> str:
        return re.sub(r"\s+", " ", str(value).strip()).lower()

    if "MKT_Coupling" in xl.sheet_names:
        df0 = pd.read_excel(xl, sheet_name="MKT_Coupling", header=None, engine="openpyxl")
        first_col = df0.iloc[:, 0].astype(str).map(normalize)
        hits = first_col[first_col.str.contains("15min mcp", na=False)]
        if len(hits) > 0:
            return "MKT_Coupling", int(hits.index[0])

    for sheet_name in xl.sheet_names:
        df0 = pd.read_excel(xl, sheet_name=sheet_name, header=None, engine="openpyxl")
        first_col = df0.iloc[:, 0].astype(str).map(normalize)
        hits = first_col[first_col.str.contains("15min mcp", na=False)]
        if len(hits) > 0:
            return sheet_name, int(hits.index[0])

    raise ValueError("Cannot find '(15min MCP)' row in DAM workbook.")


def parse_dam_mcp(path: Path) -> pd.DataFrame:
    match = match_dam_filename(path)
    if not match:
        raise ValueError(f"Bad DAM filename: {path.name}")

    day = pd.to_datetime(match.group("date"), format="%Y%m%d", errors="raise")
    table = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    table.columns = [str(column).strip() for column in table.columns]
    lower_cols = {column.lower(): column for column in table.columns}

    if "delivery_mtu" in lower_cols and "mcp" in lower_cols:
        out = table[[lower_cols["delivery_mtu"], lower_cols["mcp"]]].copy()
        out.columns = ["DELIVERY_MTU", "DAM_MCP"]
        out["DELIVERY_MTU"] = pd.to_datetime(out["DELIVERY_MTU"], errors="coerce")
        out["DAM_MCP"] = pd.to_numeric(out["DAM_MCP"], errors="coerce")
        out = out.dropna(subset=["DELIVERY_MTU", "DAM_MCP"]).copy()
        out = out[out["DELIVERY_MTU"].dt.normalize() == day].copy()
        if not out.empty:
            return out.reset_index(drop=True)

    xl = pd.ExcelFile(path)
    sheet_name, row_index = find_mkt_sheet_and_row(xl)
    df0 = pd.read_excel(xl, sheet_name=sheet_name, header=None, engine="openpyxl")

    values = pd.to_numeric(pd.Series(df0.iloc[row_index, 1:].tolist()), errors="coerce").dropna().reset_index(drop=True)
    if len(values) < 96:
        raise ValueError(f"Expected 96 DAM MCP values, got {len(values)}")

    timestamps = [day + pd.Timedelta(minutes=15 * i) for i in range(96)]
    return pd.DataFrame({"DELIVERY_MTU": timestamps, "DAM_MCP": values.iloc[:96].to_numpy(dtype=float)})


def merge_dam_market() -> None:
    market_dir = RAW_ROOT / "DAM"
    if not market_dir.exists():
        raise FileNotFoundError(f"Missing raw folder: {market_dir}")

    chosen = pick_best_file_per_day(sorted(market_dir.glob("*.xlsx")), match_dam_filename)
    if not chosen:
        raise FileNotFoundError(f"No DAM files found in {market_dir}")

    frames = []
    for date_text in sorted(chosen):
        path = chosen[date_text]
        try:
            df = parse_dam_mcp(path)
            df["SOURCE_FILE"] = path.name
            frames.append(df)
            print(f"[DAM] merged {date_text} <- {path.name}")
        except Exception as exc:
            print(f"[DAM] failed {date_text} <- {path.name} | {exc}", file=sys.stderr)

    if not frames:
        raise RuntimeError("No DAM files were merged successfully.")

    full = (
        pd.concat(frames, ignore_index=True)
        .sort_values("DELIVERY_MTU")
        .drop_duplicates(subset=["DELIVERY_MTU"], keep="last")
        .reset_index(drop=True)
    )

    out_path = OUT_DIR / f"EL-DAM_Results_{DATE_FROM}_{DATE_TO}.csv"
    full.to_csv(out_path, index=False)
    print(f"[DAM] wrote {out_path}")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for ida_tag in IDA_TAGS:
        merge_ida_market(ida_tag)
    merge_dam_market()


if __name__ == "__main__":
    main()
