"""
Solstice — Solar Data Aggregation Library

A Python library for aggregating solar data from multiple sources
with LLM-assisted schema detection, weather enrichment, and forecasting.

Quick Start:
    from solstice import Solstice

    agg = Solstice()
    agg.add_file("inverter.csv")
    df = agg.aggregate(freq="1D")
    agg.save("output.csv")

With LLM:
    from solstice import LLMAnalyzer

    analyzer = LLMAnalyzer(api_key="your-groq-key")
    analyzer.add_file("data.xlsx")
    analyzer.analyze()
    agg = analyzer.create_aggregator()
    df = agg.aggregate(freq="1D")

With Weather Enrichment:
    from solstice.weather import enrich_with_weather

    df_enriched = enrich_with_weather(df, latitude=1.35, longitude=103.82)

With Forecasting:
    from solstice.forecasting import SolarForecaster

    forecaster = SolarForecaster()
    forecaster.fit(df_enriched)
    metrics = forecaster.get_metrics()
    scenarios = forecaster.predict_scenarios()
"""

__version__ = "2.0.0"

from .aggregator import Solstice, quick_aggregate
from .llm_integration import LLMAnalyzer, analyze_and_aggregate, get_prompt_for_manual_llm
from .schema import (
    SCHEMA, SchemaField, AggregationMethod,
    get_aggregation_rules, get_required_fields, get_optional_fields, print_schema,
    register_field,
    SolsticeError, SchemaValidationError, DetectionError,
    AggregationError, WeatherEnrichmentError,
)
from .detection import auto_detect_columns, generate_llm_prompt, parse_llm_response, format_llm_result_for_review
from .processing import (
    load_file, standardise_dataframe, merge_with_environment,
    align_timestamps, aggregate_to_period, validate_dataframe,
    detect_anomalies, auto_clean,
)
from .visualization import plot_time_alignment, print_time_alignment_report, plot_energy_production, plot_data_quality

__all__ = [
    # Core
    "Solstice", "LLMAnalyzer", "quick_aggregate", "analyze_and_aggregate",
    # Schema
    "SCHEMA", "SchemaField", "AggregationMethod",
    "get_aggregation_rules", "print_schema", "register_field",
    # Exceptions
    "SolsticeError", "SchemaValidationError", "DetectionError",
    "AggregationError", "WeatherEnrichmentError",
    # Detection
    "auto_detect_columns", "generate_llm_prompt",
    # Processing
    "load_file", "standardise_dataframe", "merge_with_environment",
    "align_timestamps", "aggregate_to_period",
    "detect_anomalies", "auto_clean",
    # Visualization
    "plot_time_alignment", "print_time_alignment_report",
    "plot_energy_production", "plot_data_quality",
]
