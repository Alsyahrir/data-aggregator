"""
Example: LLM-Powered Analysis (Groq)

Uses Groq's free LLM API to automatically detect column mappings.
Outputs both aggregated and non-aggregated (standardized) data.

Setup:
1. Get API key from https://console.groq.com
2. pip install groq pandas openpyxl
3. Update API_KEY below
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from solar_aggregator import LLMAnalyzer


def main():
    API_KEY = os.environ.get("REDACTED_API_KEY")

    if not API_KEY:
        print("Error: No API key. Get one from https://console.groq.com")
        return

    data_files = [
        "Data/Excel/SolarPanel01.xlsx",
        "Data/Excel/SolarPanel02.xlsx",
        "Data/Excel/bayan_lepas_weather_2025.csv",
    ]

    existing_files = [f for f in data_files if os.path.exists(f)]
    if not existing_files:
        print("No data files found. Update the file paths.")
        return

    # Analyze files with LLM
    analyzer = LLMAnalyzer(api_key=API_KEY)
    for filepath in existing_files:
        analyzer.add_file(filepath)

    analyzer.analyze()
    print(analyzer.get_analysis_summary())

    # Aggregate
    agg = analyzer.create_aggregator()
    agg.aggregate(freq="1D")
    print(agg.get_summary())

    # Save outputs
    os.makedirs("outputs/EXCEL", exist_ok=True)
    agg.get_dataframe("aggregated").to_csv("outputs/EXCEL/daily_aggregated.csv", index=False)
    agg.get_dataframe("merged").to_csv("outputs/EXCEL/standardized_raw.csv", index=False)
    print("\nSaved to outputs/EXCEL/")


if __name__ == "__main__":
    main()
