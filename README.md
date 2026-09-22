# ☀️ Solstice

[![Tests](https://github.com/Alsyahrir/data-aggregator/actions/workflows/tests.yml/badge.svg)](https://github.com/Alsyahrir/data-aggregator/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

A Python library — with a Next.js/FastAPI web app and a Streamlit interface — for aggregating solar panel data from multiple sources with LLM-assisted schema detection, weather enrichment, anomaly detection, and energy forecasting.

## Features

- **Auto-detect column mappings** using keyword pattern matching
- **LLM-powered detection** for unusual column names — bring any provider (Groq, OpenAI, Anthropic, a local model, or your own)
- **Merge** inverter data with environmental and irradiance data
- **Time alignment** to regular intervals
- **Proper aggregation** (SUM for energy, MEAN for temperature)
- **Anomaly detection** (IQR or Z-score based) with flag/drop/clip strategies
- **Weather enrichment** via Open-Meteo (free, no API key)
- **Energy forecasting** with Random Forest + weather scenario analysis
- **Interactive dashboard** with Plotly / Chart.js charts
- **Schema extensibility** — register custom fields at runtime

## Architecture

```
Upload → Detect Columns → Standardise → Merge → Align → Aggregate
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
pip install -e .                 # core only
pip install -e ".[all]"          # core + LLM detection (all providers) + forecasting + plotting
pip install -e ".[llm]"          # Groq / OpenAI / any OpenAI-compatible endpoint
pip install -e ".[llm-anthropic]" # Anthropic (Claude) specifically
pip install -e ".[forecast]"     # just scikit-learn forecasting
pip install -e ".[viz]"          # just matplotlib charts
```

To also run the web apps (Next.js/FastAPI, Streamlit) or the test suite:

```bash
pip install -r requirements.txt   # everything, including app-only deps
npm install                       # Next.js frontend deps
```

## Quick Start

### Web Interface (Next.js + FastAPI, deployed on Vercel)

```bash
npm install
npm run dev            # Next.js frontend on http://localhost:3000
uvicorn api.index:app --reload --port 8000   # FastAPI backend, in a second terminal
```

### Web Interface (Streamlit, local/legacy)

```bash
streamlit run app.py
```

### Basic Usage (Python)

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

Any LLM backend works — column detection only ever needs a text-in,
text-out completion, so the provider is fully pluggable (`solstice.llm_providers`).

```python
from solstice import LLMAnalyzer
from solstice.llm_providers import GroqProvider, OpenAIProvider, AnthropicProvider, OpenAICompatibleProvider

# Groq (free tier) — or the api_key= shorthand below does this for you
analyzer = LLMAnalyzer(provider=GroqProvider(api_key="your-groq-key"))

# OpenAI
analyzer = LLMAnalyzer(provider=OpenAIProvider(api_key="sk-..."))

# Anthropic (Claude)
analyzer = LLMAnalyzer(provider=AnthropicProvider(api_key="sk-ant-..."))

# Anything OpenAI-compatible — local Ollama/vLLM, Together, Fireworks, etc.
analyzer = LLMAnalyzer(provider=OpenAICompatibleProvider(
    api_key="not-needed-for-local",
    base_url="http://localhost:11434/v1",
    models=["llama3"],
))

# Shorthand: api_key= alone still defaults to Groq, unchanged from before
analyzer = LLMAnalyzer(api_key="your-groq-key")

analyzer.add_file("data.xlsx")
analyzer.analyze()

agg = analyzer.create_aggregator()
df = agg.aggregate(freq="1D")
```

Nothing built in fits? Wrap any callable with `CustomProvider`:

```python
from solstice.llm_providers import CustomProvider

def my_llm(system_prompt: str, user_prompt: str) -> str:
    ...  # call your own model however you like
    return response_text

analyzer = LLMAnalyzer(provider=CustomProvider(my_llm))
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
├── llm_integration.py    # Two-tier column detection (keyword, then LLM)
├── llm_providers.py       # Pluggable LLM backends (Groq/OpenAI/Anthropic/custom)
├── visualization.py      # Matplotlib plotting functions
├── weather.py             # Open-Meteo weather enrichment
└── forecasting.py        # Random Forest forecasting
api/
└── index.py               # FastAPI backend (Vercel serverless function)
src/app/
├── page.jsx               # Next.js frontend (upload/detect/aggregate/forecast UI)
├── layout.jsx
└── globals.css
tests/
├── conftest.py             # Shared fixtures (e.g. blocks real LLM calls)
├── test_schema.py          # Schema unit tests
├── test_detection.py       # Detection unit tests
├── test_processing.py      # Processing + anomaly tests
├── test_aggregator.py      # End-to-end integration tests
├── test_weather.py         # Weather enrichment tests (mocked HTTP)
├── test_forecasting.py     # Forecasting tests
├── test_llm_integration.py # LLMAnalyzer tests (mocked provider)
├── test_llm_providers.py   # Per-provider tests (mocked SDK clients)
└── test_visualization.py   # Chart rendering tests
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

## Get an LLM API Key

Any of these work with the LLM detection feature — pick one:

| Provider | Free tier | Get a key |
|---|---|---|
| Groq (default) | Yes | https://console.groq.com |
| OpenAI | No | https://platform.openai.com/api-keys |
| Anthropic | No | https://console.anthropic.com |
| Local (Ollama, vLLM, etc.) | N/A — no key needed | — |

## License

MIT
