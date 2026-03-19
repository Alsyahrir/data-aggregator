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

from solar_aggregator import LLMAnalyzer, print_schema


def main():
    # Put your Groq API key here
    API_KEY = "YOUR_GROQ_API_KEY_HERE"

    # Or use environment variable
    if API_KEY == "YOUR_GROQ_API_KEY_HERE":
        API_KEY = os.environ.get("GROQ_API_KEY")

    if not API_KEY:
        print("=" * 60)
        print("ERROR: No API key!")
        print("=" * 60)
        print("\n1. Get API key from: https://console.groq.com")
        print('2. Edit this file and set API_KEY = "your-key"')
        return

    # Your data files (use forward slashes!)
    data_files = [
        "Data/Excel/SolarPanel01.xlsx",
        "Data/Excel/SolarPanel02.xlsx",
        "Data/Excel/bayan_lepas_weather_2025.csv",
    ]

    existing_files = [f for f in data_files if os.path.exists(f)]

    if not existing_files:
        print("No data files found. Update the paths.")
        return

    print("=" * 70)
    print("SCHEMA")
    print("=" * 70)
    print_schema()

    print("\n" + "=" * 70)
    print("SCANNING FILES")
    print("=" * 70)

    analyzer = LLMAnalyzer(api_key=API_KEY)

    for filepath in existing_files:
        analyzer.add_file(filepath)

    print("\n" + "=" * 70)
    print("LLM ANALYSIS")
    print("=" * 70)

    result = analyzer.analyze()
    print(analyzer.get_analysis_summary())

    print("\n" + "=" * 70)
    print("CREATING AGGREGATOR")
    print("=" * 70)

    agg = analyzer.create_aggregator()

    print("\n" + "=" * 70)
    print("AGGREGATING")
    print("=" * 70)

    df_daily = agg.aggregate(freq="1D")

    df_aggregated = agg.get_dataframe("aggregated")  # Daily aggregated
    df_merged = agg.get_dataframe("merged")          # Non-aggregated, standardized

    print("\n" + "=" * 70)
    print("AGGREGATED DATA (Daily)")
    print("=" * 70)
    print(f"Rows: {len(df_aggregated)}")
    print(f"Columns: {list(df_aggregated.columns)}")
    print("\nFirst 10 rows:")
    print(df_aggregated.head(10).to_string())

    print("\n" + "=" * 70)
    print("STANDARDIZED DATA (Non-Aggregated)")
    print("=" * 70)
    print(f"Rows: {len(df_merged)}")
    print(f"Columns: {list(df_merged.columns)}")
    print("\nFirst 10 rows:")
    print(df_merged.head(10).to_string())

    print(agg.get_summary())

    os.makedirs("outputs", exist_ok=True)

    df_aggregated.to_csv("outputs/EXCEL/daily_aggregated.csv", index=False)
    print(f"\nSaved: outputs/EXCEL/daily_aggregated.csv ({len(df_aggregated)} rows)")

    df_merged.to_csv("outputs/EXCEL/standardized_raw.csv", index=False)
    print(f"Saved: outputs/EXCEL/standardized_raw.csv ({len(df_merged)} rows)")

    print("\n" + "=" * 70)
    print("OUTPUT FILES")
    print("=" * 70)
    print("""
1. daily_aggregated.csv
   - Daily totals
   - Energy is SUMMED per day
   - Good for: Daily reports, trends, charts

2. standardized_raw.csv
   - Original timestamps preserved
   - Columns mapped to standard schema
   - Good for: Detailed analysis, hourly patterns
""")

    print("Done!")


if __name__ == "__main__":
    main()
