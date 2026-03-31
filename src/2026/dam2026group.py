import re
import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", message="Workbook contains no default style")

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]  # diploma-energy-market
RAW_DIR = BASE_DIR / "data" / "processed" / "HENEX" / "raw" / "DAM"
OUT_CSV = BASE_DIR / "data" / "processed" / "DAM2026" / "EL-DAM_20251001_20260330.csv"


DATE_FROM = "20251001"
DATE_TO   = "20260330"

DAM_PATTERNS = [
    re.compile(r"^(?P<d>\d{8})_EL-DAM_Results_EN_v(?P<v>\d{2})\.xlsx$", re.IGNORECASE),
    re.compile(r"^(?P<d>\d{8})_EL-DAM_ResultsSummary_EN_v(?P<v>\d{2})\.xlsx$", re.IGNORECASE),
]

# =========================
# HELPERS
# =========================
def within_range(d: str) -> bool:
    return int(DATE_FROM) <= int(d) <= int(DATE_TO)


def match_dam_filename(path: Path) -> re.Match[str] | None:
    for pattern in DAM_PATTERNS:
        match = pattern.match(path.name)
        if match:
            return match
    return None

def pick_best_file_per_day(paths: list[Path]) -> dict[str, Path]:
    """
    If multiple versions exist for same day, keep highest v##.
    If tie, keep newest mtime.
    """
    best: dict[str, tuple[int, float, Path]] = {}
    for p in paths:
        m = match_dam_filename(p)
        if not m:
            continue
        d = m.group("d")
        if not within_range(d):
            continue

        v = int(m.group("v"))
        mtime = p.stat().st_mtime

        if d not in best:
            best[d] = (v, mtime, p)
        else:
            v0, t0, _ = best[d]
            if (v > v0) or (v == v0 and mtime > t0):
                best[d] = (v, mtime, p)

    return {d: tup[2] for d, tup in best.items()}

def norm(x) -> str:
    return re.sub(r"\s+", " ", str(x).strip()).lower()

def find_mkt_sheet_and_row(xl: pd.ExcelFile) -> tuple[str, int]:
    """
    Prefer sheet named 'MKT_Coupling'.
    Else search any sheet for a cell containing '15min mcp'.
    Returns (sheet_name, row_index_in_headerless_df0).
    """
    # 1) Prefer known sheet
    if "MKT_Coupling" in xl.sheet_names:
        df0 = pd.read_excel(xl, sheet_name="MKT_Coupling", header=None, engine="openpyxl")
        col0 = df0.iloc[:, 0].astype(str).map(norm)
        hits = col0[col0.str.contains("15min mcp", na=False)]
        if len(hits) > 0:
            return "MKT_Coupling", int(hits.index[0])

    # 2) Fallback: search all sheets
    for sh in xl.sheet_names:
        df0 = pd.read_excel(xl, sheet_name=sh, header=None, engine="openpyxl")
        # scan first column (fast and enough for this file type)
        col0 = df0.iloc[:, 0].astype(str).map(norm)
        hits = col0[col0.str.contains("15min mcp", na=False)]
        if len(hits) > 0:
            return sh, int(hits.index[0])

    raise ValueError("Cannot find row containing '(15min MCP)' in any sheet.")

def parse_day_15min_mcp(path: Path) -> pd.DataFrame:
    """
    Supports two DAM workbook layouts:
    1. Tabular results file with DELIVERY_MTU and MCP columns
    2. Summary workbook with a '(15min MCP)' row inside MKT_Coupling
    """
    m = match_dam_filename(path)
    if not m:
        raise ValueError(f"Bad filename: {path.name}")
    dday = pd.to_datetime(m.group("d"), format="%Y%m%d", errors="raise")

    table = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    table.columns = [str(column).strip() for column in table.columns]
    lower_cols = {column.lower(): column for column in table.columns}

    if "delivery_mtu" in lower_cols and "mcp" in lower_cols:
        out = table[[lower_cols["delivery_mtu"], lower_cols["mcp"]]].copy()
        out.columns = ["DELIVERY_MTU", "DAM_MCP"]
        out["DELIVERY_MTU"] = pd.to_datetime(out["DELIVERY_MTU"], errors="coerce")
        out["DAM_MCP"] = pd.to_numeric(out["DAM_MCP"], errors="coerce")
        out = out.dropna(subset=["DELIVERY_MTU", "DAM_MCP"]).copy()
        out = out[out["DELIVERY_MTU"].dt.normalize() == dday].copy()
        if not out.empty:
            return out.reset_index(drop=True)

    xl = pd.ExcelFile(path)
    sheet, mcp_row = find_mkt_sheet_and_row(xl)
    df0 = pd.read_excel(xl, sheet_name=sheet, header=None, engine="openpyxl")

    raw_vals = df0.iloc[mcp_row, 1:].tolist()
    vals = pd.to_numeric(pd.Series(raw_vals), errors="coerce").dropna().reset_index(drop=True)
    if len(vals) < 96:
        raise ValueError(f"15min MCP row found, but only {len(vals)} numeric values (expected 96).")

    times = [dday + pd.Timedelta(minutes=15 * i) for i in range(96)]
    return pd.DataFrame({"DELIVERY_MTU": times, "DAM_MCP": vals.iloc[:96].to_numpy(dtype=float)})

# =========================
# MAIN
# =========================
def main():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"RAW_DIR not found: {RAW_DIR}")

    files = sorted(RAW_DIR.glob("*.xlsx"))
    chosen = pick_best_file_per_day(files)

    if not chosen:
        print(f"No DAM files found in range {DATE_FROM}-{DATE_TO} under {RAW_DIR}")
        return

    days = sorted(chosen.keys())
    print(f"Days selected: {len(days)} (min={days[0]} max={days[-1]})")

    frames = []
    for d in days:
        p = chosen[d]
        try:
            day_df = parse_day_15min_mcp(p)
            frames.append(day_df)
            print(f"[OK] {d} <- {p.name} | rows={len(day_df)}")
        except Exception as e:
            print(f"[FAIL] {d} <- {p.name} | {e}", file=sys.stderr)

    if not frames:
        print("Nothing parsed successfully.")
        return

    full = (
        pd.concat(frames, ignore_index=True)
          .sort_values("DELIVERY_MTU")
          .drop_duplicates(subset=["DELIVERY_MTU"], keep="last")
          .reset_index(drop=True)
    )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    full.to_csv(OUT_CSV, index=False)

    print(f"\nWrote: {OUT_CSV}")
    print(f"Rows: {len(full)}")
    print(f"Range: {full['DELIVERY_MTU'].min()} -> {full['DELIVERY_MTU'].max()}")

if __name__ == "__main__":
    main()
