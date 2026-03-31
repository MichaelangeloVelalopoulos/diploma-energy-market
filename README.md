# Diploma - Forecasting Intraday Electricity Prices in Greece

This project collects and analyzes data from:
- **ENTSOE** (forecasted features)
- **IPTO / HEnEx** (system & market data)

Goal: build a forecasting model for intraday electricity prices based on weather and system forecasted conditions.

## Installation/Environment Setup

**Prerequisites:** Python 3.10 or newer and `git`.

### Quick Setup (One-Liner)

**macOS / Linux:**

```bash
git clone https://github.com/MichaelangeloVelalopoulos/diploma-energy-market.git && cd diploma-energy-market && python3 -m venv .venv && source .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
```

**Windows (PowerShell):**

```powershell
git clone https://github.com/MichaelangeloVelalopoulos/diploma-energy-market.git; cd diploma-energy-market; python3 -m venv .venv; .venv\Scripts\Activate.ps1; pip install --upgrade pip; pip install -r requirements.txt
```

### Step-by-Step Setup

- **Clone the repo:**

```bash
git clone https://github.com/MichaelangeloVelalopoulos/diploma-energy-market.git
cd diploma-energy-market
```

- **Create and activate a virtual environment (macOS / zsh):**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

- **Create and activate a virtual environment (Windows / PowerShell):**

```powershell
python3 -m venv .venv
.venv\Scripts\Activate.ps1
```

- **Install Python dependencies:**

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

- **Notes:**
	- Some fetch scripts rely on external APIs; check individual script docstrings for configuration or API keys.
	- If you run into missing package errors, verify your active virtual environment and re-run `pip install -r requirements.txt`.

## Data Processing Pipeline
### Required HEnEx ZIP Files

Before running the pipeline, download the required HEnEx archive files manually.

HEnEx archive page:

- [HEnEx XBID Archive](https://www.enexgroup.gr/el/xbid-archive)

Download:

- `2025_EL-XBID_Results.zip`

Recommended usage is to pass the full path explicitly.

### Step 1: Run Main Pipeline (One-Liner)

Before running the pipeline, place the HEnEx archive somewhere accessible, for example:

- `/Users/<your-user>/Downloads/2025_EL-DAM-IDAs_Results.zip`

The pipeline now uses two HEnEx sources:

- The ZIP file for `2025-10-01` through `2025-12-31`
- The HEnEx website/API for `2026-01-01` through `2026-03-30`

**macOS / Linux:**

```bash
python src/2026/extract_henex_zip_results.py --zip-path "/Users/<your-user>/Downloads/2025_EL-DAM-IDAs_Results.zip" && python src/2026/fetch_henex_ida_prices.py && python src/2026/fetch_henex_dam_results.py && python src/2026/merge_henex_results.py && python src/2026/admie2026.py && python src/2026/download_isp_forecasts.py && python src/2026/Build2026FINAL.PY
```

**Windows (PowerShell):**

```powershell
python src/2026/extract_henex_zip_results.py --zip-path "C:\Users\<your-user>\Downloads\2025_EL-DAM-IDAs_Results.zip"; python src/2026/fetch_henex_ida_prices.py; python src/2026/fetch_henex_dam_results.py; python src/2026/merge_henex_results.py; python src/2026/admie2026.py; python src/2026/download_isp_forecasts.py; python src/2026/Build2026FINAL.PY
```

**Pipeline Overview:**
1. `extract_henex_zip_results.py` - Extract DAM, IDA1, IDA2, and IDA3 Excel files from the HEnEx ZIP for `2025-10-01` to `2025-12-31` → `data/processed/HENEX/raw/`
2. `fetch_henex_ida_prices.py` - Download IDA1, IDA2, and IDA3 Excel files from HEnEx for `2026-01-01` to `2026-03-30` → `data/processed/HENEX/raw/IDA{1,2,3}/`
3. `fetch_henex_dam_results.py` - Download DAM Excel files from HEnEx for `2026-01-01` to `2026-03-30` → `data/processed/HENEX/raw/DAM/`
4. `merge_henex_results.py` - Merge ZIP-extracted and API-downloaded HEnEx files into unified raw folders → `data/processed/HENEX/raw/`
5. `admie2026.py` - Download ADMIE balancing and imbalance data → `data/processed/admie_downloads/`
6. `download_isp_forecasts.py` - Download ADMIE ISP forecasts → `data/processed/admie_downloadsForecasts/`
7. `Build2026FINAL.PY` - Merge in ADMIE balancing market data → `data/processed/MERGED/EL-IDA1_WITH_DAM_BM_20251001_20260330.csv`

### Step 2: Fetch ENTSOE Forecasts (Requires API Token)

**Important:** Before running this step, you must:

1. **Get API Token from ENTSOE:**
   - Go to https://transparency.entsoe.eu/
   - Register for a free account
   - Navigate to: Settings → API tokens → Generate New Token
   - Copy the token

2. **Export Token in Terminal:**

**macOS / Linux:**
```bash
export ENTSOE_API_TOKEN="your_api_token_here"
```

**Windows (PowerShell):**
```powershell
$env:ENTSOE_API_TOKEN = "your_api_token_here"
```

3. **Run ENTSOE Forecasts:**
```bash
python src/entsoe_2026forecasts.py
```

**What it does:**
- Fetches ENTSOE wind and solar generation forecasts
- Merges forecasts with previous IDA/DAM/ADMIE data
- Generates the final enriched dataset for model training

**Output:**
- Final merged dataset in `data/processed/MERGED/`

### Individual Commands with Details

#### 1. Extract Historical HEnEx ZIP Data

**One-Liner:**
```bash
python src/2026/extract_henex_zip_results.py --zip-path "/Users/<your-user>/Downloads/2025_EL-DAM-IDAs_Results.zip"
```

**What it does:**
- Extracts HEnEx DAM, IDA1, IDA2, and IDA3 Excel files from the provided archive
- Keeps only the target historical window from `2025-10-01` through `2025-12-31`
- Places the extracted raw files into one unified folder structure used by the rest of the pipeline

**Data sources:**
- HEnEx archive: `2025_EL-DAM-IDAs_Results.zip`

**Output locations:**
- `data/processed/HENEX/raw/DAM/`
- `data/processed/HENEX/raw/IDA1/`
- `data/processed/HENEX/raw/IDA2/`
- `data/processed/HENEX/raw/IDA3/`

**Key details:**
- Selects files only for `2025-10-01` to `2025-12-31`
- Preserves the original HEnEx filenames
- Reports any missing dates that are not present in the ZIP

---

#### 2. Fetch HEnEx IDA Results For 2026

**One-Liner:**
```bash
python src/2026/fetch_henex_ida_prices.py
```

**What it does:**
- Downloads IDA1, IDA2, and IDA3 Excel files from HEnEx for the 2026 segment
- Stores them in the same raw HEnEx folders as the ZIP-extracted files
- Completes the HEnEx IDA coverage for `2026-01-01` through `2026-03-30`

**Data sources:**
- HEnEx public documents / website

**Output locations:**
- `data/processed/HENEX/raw/IDA1/`
- `data/processed/HENEX/raw/IDA2/`
- `data/processed/HENEX/raw/IDA3/`

**Key details:**
- Uses the highest available file version for each day
- Skips already downloaded files
- Date range: Jan 1, 2026 - Mar 30, 2026

---

#### 3. Fetch HEnEx DAM Results For 2026

**One-Liner:**
```bash
python src/2026/fetch_henex_dam_results.py
```

**What it does:**
- Downloads DAM Excel files for the 2026 segment
- Supports both `EL-DAM_Results_EN_*.xlsx` and `EL-DAM_ResultsSummary_EN_*.xlsx` naming patterns
- Stores them in the same raw HEnEx DAM folder as the ZIP-extracted historical files

**Data sources:**
- HEnEx public documents / website

**Output locations:**
- `data/processed/HENEX/raw/DAM/`

**Key details:**
- Uses the highest available file version for each day
- Skips already downloaded files
- Date range: Jan 1, 2026 - Mar 30, 2026

---

#### 4. Fetch ADMIE 2026 Data

**One-Liner:**
```bash
python src/2026/merge_henex_results.py
```

**What it does:**	
- Merges ZIP-extracted and API-downloaded HEnEx files into unified raw folders

#### 5. Build IDA1 Master Dataset

**One-Liner:**
```bash
python src/2026/merge_henex_price_timeseries.py
```

**What it does:**
- Reads all raw IDA and DAM files from the unified HEnEx raw folder
- Picks the best version per day
- Reshapes the auction data and builds one master dataset with a unified `MCP` column


**Key details:**
- Automatically merges ZIP and API periods because both land in the same raw folder
- Drops the detailed `TRADES__*` columns after building the master dataset
- Date range: Oct 1, 2025 - Mar 30, 2026

---

#### 6. Build DAM MCP Series

**One-Liner:**
```bash
python src/2026/admie2026.py
```

**What it does:**
- Downloads ADMIE balancing and imbalance data for the target period
- Extracts the relevant balancing market settlement price series
- Prepares it for merging with the IDA1 master dataset

#### 7. Download ADMIE ISP Forecasts
**One-Liner:**
```bash
python src/2026/download_isp_forecasts.py
```
**What it does:**
- Downloads ADMIE ISP forecasts for the target period
- Prepares them for merging with the IDA1 master dataset