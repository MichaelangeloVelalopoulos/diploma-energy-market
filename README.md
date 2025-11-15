# Diploma - Forecasting Intraday Electricity Prices in Greece

This project collects and analyzes data from:
- **Open-Meteo API** (weather features)
- **IPTO / HEnEx** (system & market data)

Goal: build a forecasting model for intraday electricity prices based on weather and system conditions.

## Installation

- **Prerequisites:** Python 3.10 or newer and `git`.
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

If you'd like, I can also add a short `CONTRIBUTING.md` or a small shell helper script to automate the environment setup.
