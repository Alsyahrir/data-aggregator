# Solar Data Aggregator

A Python library for aggregating solar panel data from multiple sources with LLM-assisted schema detection.

## Features

- Auto-detect column mappings using keywords
- LLM-powered detection for unusual column names (Groq API)
- Merge inverter data with environmental data
- Time alignment to regular intervals
- Proper aggregation (SUM for energy, MEAN for temperature)
- Visualization tools

## Installation

```bash
pip install pandas openpyxl groq
```

## Quick Start

### Basic Usage (Manual Mapping)

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

## File Structure

```
solar_aggregator/
├── __init__.py          # Package exports
├── schema.py            # Schema definitions
├── detection.py         # Column detection
├── processing.py        # Data processing
├── aggregator.py        # Main class
├── llm_integration.py   # Groq LLM integration
└── visualization.py     # Plotting functions
```

## Schema

| Field | Required | Aggregation | Description |
|-------|----------|-------------|-------------|
| timestamp | Yes | - | Date/time |
| source_id | Yes | - | Data source ID |
| energy | Yes | SUM | Energy in kWh |
| ambient_temp | No | MEAN | Air temperature |
| irradiance | No | MEAN | Solar radiation |
| wind_speed | No | MEAN | Wind speed |
| humidity | No | MEAN | Relative humidity |

## Get Groq API Key

1. Go to https://console.groq.com
2. Sign up (free)
3. Create API key
4. Use in your code

## License

MIT
