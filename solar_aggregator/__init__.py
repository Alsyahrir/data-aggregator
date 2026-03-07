"""
Solar Data Aggregator Library

A Python library for aggregating solar data from multiple sources
with LLM-assisted schema detection.

Quick Start:
    from solar_aggregator import SolarAggregator
    
    agg = SolarAggregator()
    agg.add_file("inverter.csv")
    df = agg.aggregate(freq="1D")
    agg.save("output.csv")

With LLM:
    from solar_aggregator import LLMAnalyzer
    
    analyzer = LLMAnalyzer(api_key="your-groq-key")
    analyzer.add_file("data.xlsx")
    analyzer.analyze()
    agg = analyzer.create_aggregator()
    df = agg.aggregate(freq="1D")
"""

__version__ = "1.0.0"

from .aggregator import SolarAggregator, quick_aggregate
from .llm_integration import LLMAnalyzer, analyze_and_aggregate, get_prompt_for_manual_llm
from .schema import SCHEMA, SchemaField, AggregationMethod, get_aggregation_rules, get_required_fields, get_optional_fields, print_schema
from .detection import auto_detect_columns, generate_llm_prompt, parse_llm_response, format_llm_result_for_review
from .processing import load_file, standardise_dataframe, merge_with_environment, align_timestamps, aggregate_to_period, validate_dataframe
from .visualization import plot_time_alignment, print_time_alignment_report, plot_energy_production, plot_data_quality

__all__ = [
    "SolarAggregator", "LLMAnalyzer", "quick_aggregate", "analyze_and_aggregate",
    "SCHEMA", "get_aggregation_rules", "print_schema",
    "auto_detect_columns", "generate_llm_prompt",
    "load_file", "standardise_dataframe", "merge_with_environment", "align_timestamps", "aggregate_to_period",
    "plot_time_alignment", "print_time_alignment_report", "plot_energy_production", "plot_data_quality",
]
