"""
Example: Basic Usage (No LLM)

Use this when your files have standard column names that can be auto-detected,
or when you want to provide manual column mappings.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from solar_aggregator import SolarAggregator, print_schema


def main():
    # Your data files (use forward slashes!)
    data_files = [
        "Data/Excel/SolarPanel01.xlsx",
        "Data/Excel/SolarPanel02.xlsx",
    ]
    
    # Manual mapping for Bayan Lepas data format
    # Your columns: Date, Timestamp, Device Name, Parameter, Meter reading, Value (Graph Scale...)
    column_mapping = {
        "Date": "timestamp",
        "Value (Graph Scale : 1.000000 )": "energy"
    }
    
    # Filter existing files
    existing_files = [f for f in data_files if os.path.exists(f)]
    
    if not existing_files:
        print("No data files found!")
        print("Expected files at:")
        for f in data_files:
            print(f"  {os.path.abspath(f)}")
        return
    
    # Show schema
    print("=" * 70)
    print("SCHEMA")
    print("=" * 70)
    print_schema()
    
    # Create aggregator
    print("\n" + "=" * 70)
    print("LOADING FILES")
    print("=" * 70)
    
    agg = SolarAggregator(verbose=True)
    
    for i, filepath in enumerate(existing_files, 1):
        source_id = f"Panel{i:02d}"
        agg.add_file(filepath, source_id=source_id, mapping=column_mapping)
    
    # Aggregate
    print("\n" + "=" * 70)
    print("AGGREGATING")
    print("=" * 70)
    
    df_daily = agg.aggregate(freq='1D')
    
    # Results
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    
    print(f"\nDate range: {df_daily['timestamp'].min()} to {df_daily['timestamp'].max()}")
    print(f"Total rows: {len(df_daily)}")
    
    print("\nFirst 10 rows:")
    print(df_daily.head(10).to_string())
    
    print(agg.get_summary())
    
    # Save
    os.makedirs("outputs", exist_ok=True)
    agg.save("outputs/daily_aggregated.csv")
    
    print("\nDone!")


if __name__ == "__main__":
    main()
