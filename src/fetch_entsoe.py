import os
import argparse
from datetime import datetime, timedelta, timezone
import requests
import xmltodict
import pandas as pd

# ΝΕΟ endpoint
ENTSOE_API = "https://web-api.tp.entsoe.eu/api"
GR_BZN = "10YGR-HTSO-----Y"

# Doc/Process types
DOC_TOTAL_LOAD = "A65"     # Actual Total Load
DOC_GEN_PER_TYPE = "A75"   # Actual Generation per Type
DOC_DAM_PRICE = "A44"      # Day-Ahead Prices
PROC_REALIZED = "A16"
PROC_DAYAHEAD = "A01"


def yyyymmddhhmm(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M")


def chunk_period(start_utc, end_utc, hours_per_call=168):
    cur = start_utc
    while cur < end_utc:
        nxt = min(cur + timedelta(hours=hours_per_call), end_utc)
        yield cur, nxt
        cur = nxt


def call_entsoe(token: str, params: dict) -> dict:
    """Κλήση API → xmltodict + χειρισμός acknowledgements με καθαρό μήνυμα."""
    p = dict(params)
    p["securityToken"] = token
    r = requests.get(ENTSOE_API, params=p, timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise SystemExit(f"HTTP {r.status_code} for {r.url}\n{r.text}") from e

    js = xmltodict.parse(r.text)

    # Αν είναι Acknowledgement, εμφάνισε το reason
    if "Acknowledgement_MarketDocument" in js:
        ack = js["Acknowledgement_MarketDocument"]
        reasons = ack.get("Reason") or []
        if not isinstance(reasons, list):
            reasons = [reasons]
        msgs = []
        for rr in reasons:
            code = (rr or {}).get("code", "")
            text = (rr or {}).get("text", "")
            if text:
                msgs.append(f"[{code}] {text}")
        msg = "; ".join(msgs) or "No data / acknowledgement returned."
        pretty_url = requests.Request("GET", ENTSOE_API, params=p).prepare().url
        raise SystemExit(f"ENTSO-E Acknowledgement\nURL: {pretty_url}\n→ {msg}")

    return js


def extract_timeseries(js: dict):
    """Επιστρέφει (TimeSeries list ή []), ανεξαρτήτως root (GL_ ή Publication_)."""
    root = None
    for k in ("GL_MarketDocument", "Publication_MarketDocument"):
        if k in js:
            root = js[k]
            break
    if not root:
        # δεν είναι ούτε GL ούτε Publication (π.χ. άδειο ή άλλο schema)
        return []
    ts = root.get("TimeSeries", [])
    return ts


def parse_time_series(ts) -> pd.DataFrame:
    """Μετατρέπει XML TimeSeries σε DataFrame με timestamp, quantity/price, psrType."""
    if not ts:
        return pd.DataFrame()
    rows = []
    series = ts if isinstance(ts, list) else [ts]
    for s in series:
        psr = None
        ident = s.get("MktPSRType")
        if ident and "psrType" in ident:
            psr = ident["psrType"]
        period = s.get("Period") or {}
        ti = period.get("timeInterval") or {}
        start = pd.to_datetime(ti.get("start"))
        resolution = period.get("resolution")
        points = period.get("Point", [])
        if not isinstance(points, list):
            points = [points]
        for p in points:
            if not p:
                continue
            qty = p.get("quantity")
            price = p.get("price.amount")
            pos = int(p.get("position", 1))
            rows.append({
                "start": start,
                "resolution": resolution,
                "position": pos,
                "quantity": float(qty) if qty is not None else None,
                "price": float(price) if price is not None else None,
                "psrType": psr
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # βήμα χρόνου από resolution
    step = 60
    res0 = df["resolution"].dropna().astype(str).iloc[0] if "resolution" in df and not df["resolution"].dropna().empty else "PT60M"
    if "PT15M" in res0:
        step = 15
    df["timestamp"] = df["start"] + pd.to_timedelta((df["position"] - 1) * step, unit="m")
    df = df.drop(columns=["start", "position"], errors="ignore")
    return df


def fetch_total_load(token, start_utc, end_utc, bidding_zone=GR_BZN):
    """Actual Total Load (A65): χρησιμοποιεί outBiddingZone_Domain."""
    frames = []
    for s, e in chunk_period(start_utc, end_utc):
        params = {
            "documentType": DOC_TOTAL_LOAD,
            "processType": PROC_REALIZED,
            "outBiddingZone_Domain": bidding_zone,
            "periodStart": yyyymmddhhmm(s),
            "periodEnd": yyyymmddhhmm(e),
        }
        js = call_entsoe(token, params)
        ts = extract_timeseries(js)
        df = parse_time_series(ts)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not out.empty:
        out = out[["timestamp", "quantity"]].rename(columns={"quantity": "load_mw"}).sort_values("timestamp")
    return out


def fetch_gen_per_type(token, start_utc, end_utc, bidding_zone=GR_BZN):
    """Actual Generation per Type (A75): χρησιμοποιεί in_Domain."""
    frames = []
    for s, e in chunk_period(start_utc, end_utc):
        params = {
            "documentType": DOC_GEN_PER_TYPE,
            "processType": PROC_REALIZED,
            "in_Domain": bidding_zone,
            "periodStart": yyyymmddhhmm(s),
            "periodEnd": yyyymmddhhmm(e),
        }
        js = call_entsoe(token, params)
        ts = extract_timeseries(js)
        df = parse_time_series(ts)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return df
    # Map τεχνολογιών RES (όσες μας ενδιαφέρουν)
    map_names = {"B11": "wind_onshore", "B12": "wind_offshore", "B16": "solar"}
    df["tech"] = df["psrType"].map(map_names)
    df = df.dropna(subset=["tech"])
    pivot = df.pivot_table(index="timestamp", columns="tech", values="quantity", aggfunc="sum")
    return pivot.reset_index().sort_values("timestamp")


def fetch_day_ahead_prices(token, start_utc, end_utc, bidding_zone=GR_BZN):
    """Day-Ahead Market Prices (A44): θέλει ΚΑΙ in_Domain ΚΑΙ out_Domain (ίδια ζώνη)."""
    frames = []
    for s, e in chunk_period(start_utc, end_utc, hours_per_call=744):
        params = {
            "documentType": DOC_DAM_PRICE,
            "processType": PROC_DAYAHEAD,
            "in_Domain": bidding_zone,
            "out_Domain": bidding_zone,
            "periodStart": yyyymmddhhmm(s),
            "periodEnd": yyyymmddhhmm(e),
        }
        js = call_entsoe(token, params)
        ts = extract_timeseries(js)
        df = parse_time_series(ts)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not out.empty:
        out = out[["timestamp", "price"]].rename(columns={"price": "dam_eur_mwh"}).sort_values("timestamp")
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default=os.getenv("ENTSOE_TOKEN"), required=False)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--outdir", default="data/processed")
    args = parser.parse_args()

    if not args.token:
        raise SystemExit("❌ Missing ENTSOE_TOKEN. Use export ENTSOE_TOKEN=...")

    start_utc = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end_utc = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    os.makedirs(args.outdir, exist_ok=True)

    print("⬇️ Fetching Total Load...")
    load = fetch_total_load(args.token, start_utc, end_utc)
    if not load.empty:
        load.to_csv(os.path.join(args.outdir, "entsoe_total_load.csv"), index=False)

    print("⬇️ Fetching Generation per Type...")
    gen = fetch_gen_per_type(args.token, start_utc, end_utc)
    if not gen.empty:
        gen.to_csv(os.path.join(args.outdir, "entsoe_generation_per_type.csv"), index=False)

    print("⬇️ Fetching Day-Ahead Prices...")
    dam = fetch_day_ahead_prices(args.token, start_utc, end_utc)
    if not dam.empty:
        dam.to_csv(os.path.join(args.outdir, "entsoe_dam_prices.csv"), index=False)

    print("✅ Saved all CSVs in", args.outdir)


if __name__ == "__main__":
    main()
