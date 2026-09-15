# ☀️ Solar Data Aggregator

A Python library and Streamlit web app for aggregating solar panel data from multiple sources with LLM-assisted schema detection, weather enrichment, anomaly detection, and energy forecasting.

## Features

- **Auto-detect column mappings** using keyword pattern matching
- **LLM-powered detection** for unusual column names (Groq API)
- **Merge** inverter data with environmental and irradiance data
- **Time alignment** to regular intervals
- **Proper aggregation** (SUM for energy, MEAN for temperature)
- **Anomaly detection** (IQR or Z-score based) with flag/drop/clip strategies
- **Weather enrichment** via Open-Meteo (free, no API key)
- **Energy forecasting** with Random Forest + weather scenario analysis
- **Interactive dashboard** with Plotly charts
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

```bash
pip install -r requirements.txt
```

## Quick Start

### Web Interface (Streamlit)

```bash
streamlit run app.py
```

### Basic Usage (Python)

```python
from solar_aggregator import SolarAggregator

agg = SolarAggregator()
agg.add_file("data.xlsx", mapping={
    "Date": "timestamp",
    "Value (Graph Scale : 1.000000 )": "energy"
})
df = agg.aggregate(freq="1D")
agg.save("output.csv")
```

### LLM-Powered (Automatic Detection)

```python
from solar_aggregator import LLMAnalyzer

analyzer = LLMAnalyzer(api_key="your-groq-key")
analyzer.add_file("data.xlsx")
analyzer.analyze()

agg = analyzer.create_aggregator()
df = agg.aggregate(freq="1D")
```

### Weather Enrichment

```python
from solar_aggregator.weather import enrich_with_weather

df_enriched = enrich_with_weather(
    df,
    latitude=1.3521,   # Singapore
    longitude=103.8198,
)
```

### Energy Forecasting

```python
from solar_aggregator.forecasting import SolarForecaster

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
from solar_aggregator import detect_anomalies, auto_clean

# Flag anomalies (keeps all rows, adds is_anomaly column)
df_flagged = detect_anomalies(df, method="iqr")

# Or auto-clean: 'flag', 'drop', or 'clip'
df_clean = auto_clean(df, strategy="drop")
```

### Custom Schema Fields

```python
from solar_aggregator import register_field

register_field("efficiency", keywords=["eff", "eta"], unit="%")
```

## File Structure

```
solar_aggregator/
├── __init__.py          # Package exports
├── schema.py            # Schema definitions + custom exceptions
├── detection.py         # Column detection (keyword + LLM)
├── processing.py        # Data processing + anomaly detection
├── aggregator.py        # Main aggregation class
├── llm_integration.py   # Groq LLM integration
├── visualization.py     # Matplotlib plotting functions
├── weather.py           # Open-Meteo weather enrichment
└── forecasting.py       # Random Forest forecasting
tests/
├── test_schema.py       # Schema unit tests
├── test_detection.py    # Detection unit tests
├── test_processing.py   # Processing + anomaly tests
└── test_aggregator.py   # End-to-end integration tests
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
python -m pytest tests/ -v
```

## Get Groq API Key

1. Go to https://console.groq.com
2. Sign up (free)
3. Create API key
4. Use in your code

## License

MIT
