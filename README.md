# ☀️ Solstice

[![Tests](https://github.com/Alsyahrir/data-aggregator/actions/workflows/tests.yml/badge.svg)](https://github.com/Alsyahrir/data-aggregator/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

A Python library for aggregating solar panel data from multiple sources — with LLM-assisted schema detection, weather enrichment, anomaly detection, and energy forecasting.

## Features

- **Auto-detect column mappings** using keyword pattern matching
- **LLM-powered detection** for unusual column names (Groq API)
- **Merge** inverter data with environmental and irradiance data
- **Time alignment** to regular intervals
- **Proper aggregation** (SUM for energy, MEAN for temperature)
- **Anomaly detection** (IQR or Z-score based) with flag/drop/clip strategies
- **Weather enrichment** via Open-Meteo (free, no API key)
- **Energy forecasting** with Random Forest + weather scenario analysis
- **Chart helpers** built on matplotlib (production, monthly, quality, weekly pattern, distribution)
- **Schema extensibility** — register custom fields at runtime

## Architecture

```
Load → Detect Columns → Standardise → Merge → Align → Aggregate
                                                        ↓
                                        Anomaly Detection (flag/drop/clip)
                                                        ↓
                                          Weather Enrichment (Open-Meteo)
                                                        ↓
                                            Forecasting (Random Forest)
```

## Installation

The `solstice` package installs with a lean core (pandas/numpy/openpyxl). LLM
detection, forecasting, and plotting are optional extras — installing without
them still works, those features just print a friendly "pip install X" note
when called.

```bash
pip install -e .              # core only
pip install -e ".[all]"       # core + LLM detection + forecasting + plotting
pip install -e ".[llm]"       # just Groq LLM column detection
pip install -e ".[forecast]"  # just scikit-learn forecasting
pip install -e ".[viz]"       # just matplotlib charts
```

## Quick Start

### Basic Usage

```python
from solstice import Solstice

agg = Solstice()
agg.add_file("data.xlsx", mapping={
    "Date": "timestamp",
    "Value (Graph Scale : 1.000000 )": "energy"
})
df = agg.aggregate(freq="1D")
agg.save("output.csv")
```

### LLM-Powered (Automatic Detection)

```python
from solstice import LLMAnalyzer

analyzer = LLMAnalyzer(api_key="your-groq-key")
analyzer.add_file("data.xlsx")
analyzer.analyze()

agg = analyzer.create_aggregator()
df = agg.aggregate(freq="1D")
```

### Weather Enrichment

```python
from solstice.weather import enrich_with_weather

df_enriched = enrich_with_weather(
    df,
    latitude=1.3521,   # Singapore
    longitude=103.8198,
)
```

### Energy Forecasting

```python
from solstice.forecasting import SolarForecaster

forecaster = SolarForecaster()
forecaster.fit(df_enriched)

metrics = forecaster.get_metrics()
print(f"R²: {metrics.r2:.4f}, MAE: {metrics.mae:.1f} kWh")

scenarios = forecaster.predict_scenarios()
for s in scenarios:
    print(f"{s.name}: {s.predicted_energy:,.0f} kWh")
```

### Anomaly Detection

```python
from solstice import detect_anomalies, auto_clean

# Flag anomalies (keeps all rows, adds is_anomaly column)
df_flagged = detect_anomalies(df, method="iqr")

# Or auto-clean: 'flag', 'drop', or 'clip'
df_clean = auto_clean(df, strategy="drop")
```

### Visualization

```python
from solstice import create_all_charts

create_all_charts(df, output_folder="outputs")
```

### Custom Schema Fields

```python
from solstice import register_field

register_field("efficiency", keywords=["eff", "eta"], unit="%")
```

## File Structure

```
solstice/
├── __init__.py          # Package exports
├── py.typed              # PEP 561 marker — type hints are part of the public API
├── schema.py             # Schema definitions + custom exceptions
├── detection.py          # Column detection (keyword + LLM)
├── processing.py         # Data processing + anomaly detection
├── aggregator.py         # Main aggregation class (Solstice)
├── llm_integration.py    # Groq LLM integration
├── visualization.py      # Matplotlib plotting functions
├── weather.py             # Open-Meteo weather enrichment
└── forecasting.py        # Random Forest forecasting
examples/
├── example_usage.py       # Basic keyword-detection usage
├── example_llm_usage.py   # LLM-powered detection
├── example_llm_OEDI.py    # LLM detection against the OEDI sample dataset
└── visualization.py       # Chart generation walkthrough
tests/
├── test_schema.py         # Schema unit tests
├── test_detection.py      # Detection unit tests
├── test_processing.py     # Processing + anomaly tests
└── test_aggregator.py     # End-to-end integration tests
.github/workflows/
└── tests.yml              # CI — runs pytest on Python 3.10/3.11/3.12
pyproject.toml              # Package metadata + optional extras
LICENSE                     # MIT
```

## Schema

| Field | Required | Aggregation | Unit | Description |
|-------|----------|-------------|------|-------------|
| timestamp | Yes | — | datetime | Date/time |
| source_id | Yes | — | — | Data source ID |
| energy | Yes | SUM | kWh | Energy generated |
| ambient_temp | No | MEAN | °C | Air temperature |
| irradiance | No | MEAN | W/m² | Solar radiation (GHI) |
| wind_speed | No | MEAN | m/s | Wind speed |
| module_temp | No | MEAN | °C | PV module temperature |
| humidity | No | MEAN | % | Relative humidity |
| power | No | MEAN | W | Power output |
| voltage | No | MEAN | V | Voltage |
| current | No | MEAN | A | Current |

## Running Tests

```bash
pip install -e ".[all,dev]"
pytest tests/ -v
```

Every push and pull request runs the same suite on Python 3.10, 3.11 and 3.12
via GitHub Actions (see the badge at the top of this file).

## Get Groq API Key

1. Go to https://console.groq.com
2. Sign up (free)
3. Create API key
4. Use in your code

## License

MIT
