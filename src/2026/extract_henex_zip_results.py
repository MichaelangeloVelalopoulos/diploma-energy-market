from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZipFile


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT_DIR = PROJECT_ROOT / "data" / "processed" / "HENEX" / "raw"

DATE_FROM = "20251001"
DATE_TO = "20251231"

DEFAULT_ZIP_CANDIDATES = [
    PROJECT_ROOT / "2025_EL-DAM-IDAs_Results.zip",
    Path.home() / "Downloads" / "2025_EL-DAM-IDAs_Results.zip",
]

ENTRY_PATTERNS = {
    "DAM": re.compile(
        r"Results/DAM/(?P<date>\d{8})_EL-DAM_Results_EN_v(?P<version>\d{2})\.xlsx$",
        re.IGNORECASE,
    ),
    "IDA1": re.compile(
        r"Results/IDAs/IDA1/(?P<date>\d{8})_EL-IDA1_Results_EN_v(?P<version>\d{2})\.xlsx$",
        re.IGNORECASE,
    ),
    "IDA2": re.compile(
        r"Results/IDAs/IDA2/(?P<date>\d{8})_EL-IDA2_Results_EN_v(?P<version>\d{2})\.xlsx$",
        re.IGNORECASE,
    ),
    "IDA3": re.compile(
        r"Results/IDAs/IDA3/(?P<date>\d{8})_EL-IDA3_Results_EN_v(?P<version>\d{2})\.xlsx$",
        re.IGNORECASE,
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract HEnEx DAM and IDA files from the provided ZIP into data/processed/HENEX/raw."
    )
    parser.add_argument(
        "--zip-path",
        type=Path,
        default=None,
        help="Path to 2025_EL-DAM-IDAs_Results.zip. If omitted, common locations are checked.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Destination root directory. Defaults to data/processed/HENEX/raw.",
    )
    parser.add_argument(
        "--date-from",
        default=DATE_FROM,
        help="Inclusive start date in YYYYMMDD format.",
    )
    parser.add_argument(
        "--date-to",
        default=DATE_TO,
        help="Inclusive end date in YYYYMMDD format.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite already extracted files.",
    )
    return parser.parse_args()


def resolve_zip_path(explicit_path: Path | None) -> Path:
    if explicit_path is not None:
        if explicit_path.exists():
            return explicit_path
        raise FileNotFoundError(f"ZIP file not found: {explicit_path}")

    for candidate in DEFAULT_ZIP_CANDIDATES:
        if candidate.exists():
            return candidate

    checked = "\n".join(f"- {candidate}" for candidate in DEFAULT_ZIP_CANDIDATES)
    raise FileNotFoundError(
        "Could not find the HEnEx ZIP automatically. Checked:\n"
        f"{checked}\n"
        "Pass it explicitly with --zip-path."
    )


def iter_dates(start_yyyymmdd: str, end_yyyymmdd: str):
    start = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def in_range(date_text: str, start_yyyymmdd: str, end_yyyymmdd: str) -> bool:
    return start_yyyymmdd <= date_text <= end_yyyymmdd


def select_best_entries(
    zip_file: ZipFile,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> dict[str, dict[str, str]]:
    selected: dict[str, dict[str, tuple[int, str]]] = {key: {} for key in ENTRY_PATTERNS}

    for entry_name in zip_file.namelist():
        normalized = entry_name.replace("\\", "/")

        for market, pattern in ENTRY_PATTERNS.items():
            match = pattern.search(normalized)
            if not match:
                continue

            date_text = match.group("date")
            if not in_range(date_text, start_yyyymmdd, end_yyyymmdd):
                continue

            version = int(match.group("version"))
            current = selected[market].get(date_text)
            if current is None or version >= current[0]:
                selected[market][date_text] = (version, entry_name)
            break

    return {
        market: {date_text: entry_name for date_text, (_, entry_name) in by_day.items()}
        for market, by_day in selected.items()
    }


def extract_selected(
    zip_file: ZipFile,
    selected_entries: dict[str, dict[str, str]],
    out_dir: Path,
    overwrite: bool,
) -> dict[str, int]:
    counts = {market: 0 for market in selected_entries}

    for market, by_day in selected_entries.items():
        market_dir = out_dir / market
        market_dir.mkdir(parents=True, exist_ok=True)

        for _, entry_name in sorted(by_day.items()):
            destination = market_dir / Path(entry_name).name
            if destination.exists() and not overwrite:
                continue

            with zip_file.open(entry_name) as src, destination.open("wb") as dst:
                dst.write(src.read())
            counts[market] += 1

    return counts


def report_missing(selected_entries: dict[str, dict[str, str]], start_yyyymmdd: str, end_yyyymmdd: str) -> None:
    expected_dates = list(iter_dates(start_yyyymmdd, end_yyyymmdd))
    for market, by_day in selected_entries.items():
        missing = [date_text for date_text in expected_dates if date_text not in by_day]
        print(f"[{market}] files selected: {len(by_day)}")
        if missing:
            preview = ", ".join(missing[:8])
            suffix = " ..." if len(missing) > 8 else ""
            print(f"[{market}] missing dates in ZIP: {preview}{suffix}")


def main() -> None:
    args = parse_args()
    zip_path = resolve_zip_path(args.zip_path)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"ZIP: {zip_path}")
    print(f"Output root: {out_dir}")
    print(f"Date range: {args.date_from} -> {args.date_to}")

    with ZipFile(zip_path) as zip_file:
        selected_entries = select_best_entries(zip_file, args.date_from, args.date_to)
        extracted = extract_selected(zip_file, selected_entries, out_dir, overwrite=args.overwrite)

    for market, count in extracted.items():
        print(f"[{market}] extracted or refreshed: {count}")
    report_missing(selected_entries, args.date_from, args.date_to)


if __name__ == "__main__":
    main()
