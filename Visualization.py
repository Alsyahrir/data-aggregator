"""
Example: Visualization Functions

Creates useful charts for your solar panel data analysis.

Requirements:
    pip install matplotlib pandas openpyxl
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from solar_aggregator import SolarAggregator
from solar_aggregator.visualization import (
    plot_energy_production,
    plot_monthly_summary,
    plot_panel_comparison,
    plot_data_quality,
    plot_weekly_pattern,
    plot_distribution,
    create_all_charts
)


def main():
    # Your data files
    data_files = [
        "Data/Excel/SolarPanel01.xlsx",
        "Data/Excel/SolarPanel02.xlsx",
    ]
    
    # Manual mapping for Bayan Lepas data
    column_mapping = {
        "Date": "timestamp",
        "Value (Graph Scale : 1.000000 )": "energy"
    }
    
    # Filter existing files
    existing_files = [f for f in data_files if os.path.exists(f)]
    
    if not existing_files:
        print("No data files found!")
        return
    
    # Load and aggregate data
    print("=" * 70)
    print("LOADING DATA")
    print("=" * 70)
    
    agg = SolarAggregator(verbose=True)
    
    for i, filepath in enumerate(existing_files, 1):
        source_id = f"Panel{i:02d}"
        agg.add_file(filepath, source_id=source_id, mapping=column_mapping)
    
    df_daily = agg.aggregate(freq="1D")
    
    # Create output folder
    os.makedirs("outputs", exist_ok=True)
    
    # CREATE ALL CHARTS
    print("\n" + "=" * 70)
    print("CREATING VISUALIZATIONS")
    print("=" * 70)
    
    create_all_charts(df_daily, output_folder="outputs")
    
    # SUMMARY
    print("\n" + "=" * 70)
    print("CHARTS CREATED")
    print("=" * 70)
    print("""
1. energy_production.png  - Daily energy over time
2. monthly_summary.png    - Monthly totals bar chart
3. panel_comparison.png   - Total energy by panel
4. data_quality.png       - Missing data analysis
5. weekly_pattern.png     - Average by day of week
6. distribution.png       - Energy distribution histogram
""")
    
    print(agg.get_summary())
    print("\nDone!")


if __name__ == "__main__":
    main()