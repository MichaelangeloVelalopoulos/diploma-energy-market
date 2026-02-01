# Diploma - Forecasting Intraday Electricity Prices in Greece

This project collects and analyzes data from:
- **Open-Meteo API** (weather features)
- **IPTO / HEnEx** (system & market data)

Goal: build a forecasting model for intraday electricity prices based on weather and system conditions.

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

- **Install Python dependencies:**

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

- **Prepare data:**
	- Provided processed sample data is in `data/processed/`.
	- Raw IPTO files (if needed) go into `data/raw/ipto/`.

- **Fetch or rebuild datasets (examples):**
	- Fetch weather data: `python src/fetch_weather.py`
	- Fetch IPTO files: `python src/fetch_ipto_files.py`
	- Merge datasets: `python src/merge_weather_ipto.py`

- **Run exploratory analysis:**
	- Open notebooks in the `notebooks/` folder with Jupyter or JupyterLab:

```bash
jupyter lab notebooks/
```

- **Notes:**
	- Some fetch scripts rely on external APIs; check individual script docstrings for configuration or API keys.
	- If you run into missing package errors, verify your active virtual environment and re-run `pip install -r requirements.txt`.

## Data Processing Pipeline

### Step 1: Run Main Pipeline (One-Liner)

**macOS / Linux:**

```bash
python src/2026/fetch_henex_ida_results.py && python src/2026/admie2026.py && python src/2026/HENEX2026DAMPRICES.PY && python src/2026/build2026ida1.py && python src/2026/dam2026group.py && python src/2026/MergeIDA1_DAM2026.py && python src/2026/Build2026FINAL.PY
```

**Windows (PowerShell):**

```powershell
python src/2026/fetch_henex_ida_results.py; python src/2026/admie2026.py; python src/2026/HENEX2026DAMPRICES.PY; python src/2026/build2026ida1.py; python src/2026/dam2026group.py; python src/2026/MergeIDA1_DAM2026.py; python src/2026/Build2026FINAL.PY
```

**Pipeline Overview:**
1. `fetch_henex_ida_results.py` - Download IDA1/IDA2/IDA3 auction data → `data/processed/henex_ida_results/`
2. `admie2026.py` - Download ADMIE balancing & imbalance data → `data/processed/admie_downloads/`
3. `HENEX2026DAMPRICES.PY` - Extract DAM clearing prices → `data/processed/DAM2026/DAM/`
4. `build2026ida1.py` - Build IDA1 master dataset with features → `data/processed/henex_ida_results/processed/`
5. `dam2026group.py` - Aggregate DAM data by bidding zone → `data/processed/DAM2026/`
6. `MergeIDA1_DAM2026.py` - Merge IDA1 + DAM prices → `data/processed/MERGED/`
7. `Build2026FINAL.PY` - Integrate all data (IDA, DAM, ADMIE, weather) → `data/processed/MERGED/Final2026.csv`

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
python src/2026/entsoe_2026forecasts.py
```

**What it does:**
- Fetches ENTSOE wind and solar generation forecasts
- Merges forecasts with previous IDA/DAM/ADMIE data
- Generates final dataset: `data/processed/MERGED/Final2026.csv`

**Output:**
- Final merged dataset: `data/processed/MERGED/Final2026.csv` (ready for model training)

### Individual Commands with Details

#### 1. Fetch HEnEx IDA Results

**One-Liner:**
```bash
python src/2026/fetch_henex_ida_results.py
```

**What it does:**
- Downloads intraday auction (IDA) market results from HEnEx (Greek power exchange)
- Fetches IDA1, IDA2, and IDA3 auction data
- Extracts clearing prices and volume information for each auction

**Data sources:**
- HEnEx public API / website

**Output locations:**
- Raw data: `data/processed/henex_ida_results/raw/IDA{1,2,3}/`
- Processed data: `data/processed/henex_ida_results/processed/`
  - `EL-IDA1_Results_*.csv`
  - `EL-IDA2_Results_*.csv`
  - `EL-IDA3_Results_*.csv`

**Key details:**
- Handles multiple file versions (v01, v02, etc.)
- Automatically skips already downloaded files
- Parses hourly clearing prices and traded volumes
- Date range: Oct 1, 2025 - Jan 27, 2026

---

#### 2. Fetch ADMIE 2026 Data

**One-Liner:**
```bash
python src/2026/admie2026.py
```

**What it does:**
- Retrieves balancing energy and capacity data from ADMIE (Greek Transmission System Operator)
- Downloads three product types:
  - **Balancing Energy Product** (BRP): Energy deployed for system balancing
  - **Balancing Capacity Product** (BCP): Capacity reserved for system balancing
  - **IMBABE**: Imbalance data (system deviation from schedule)

**Data sources:**
- ADMIE XML API / data portal
- Maintains manifest files for efficient incremental updates

**Output locations:**
- `data/processed/admie_downloads/balancingenergyproduct/` (JSON files by date)
- `data/processed/admie_downloads/balancingcapacityproduct/` (JSON files by date)
- `data/processed/admie_downloads/imbabe/` (JSON files by date)
- Manifest files for tracking downloaded date ranges

**Key details:**
- Uses manifest files to avoid re-downloading existing data
- Parses 15-minute interval data
- Extracts prices, volumes, and imbalance directions
- Date range: Oct 1, 2025 - Jan 27, 2026

---

#### 3. Process DAM Prices

**One-Liner:**
```bash
python src/2026/HENEX2026DAMPRICES.PY
```

**What it does:**
- Extracts Day-Ahead Market (DAM) clearing prices from HEnEx ResultsSummary Excel files
- Parses Market Clearing Price (MCP) for Greece Mainland
- Creates hourly time series (00:00 - 23:00 UTC)
- Combines all daily data into a single master dataset

**Data sources:**
- HEnEx public documents: `EL-DAM_ResultsSummary_EN_v*.xlsx` files
- Sheet: "MKT_Coupling"
- Extracts 15-minute MCP data

**Output locations:**
- Raw Excel files: `data/processed/DAM2026/raw/`
- Processed data: `data/processed/DAM2026/DAM/`
  - `EL-DAM_MASTER_20251001_20260127.csv` (hourly aggregated)
  - `EL-DAM_MASTER_20251001_20260127.parquet` (Parquet format, if pyarrow installed)

**Key details:**
- Automatically detects best file version (highest v number)
- Handles multi-line Excel headers
- Converts 15-minute MCP to hourly format
- Tracks data source in `SOURCE_FILE` column
- Date range: Oct 1, 2025 - Jan 27, 2026

---
