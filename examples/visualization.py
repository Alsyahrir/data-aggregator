"""
Example: Visualization Functions

Creates useful charts for your solar panel data analysis.

Requirements:
    pip install matplotlib pandas
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

import pandas as pd
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
    # Your aggregated data file
    data_file = "outputs/EXCEL/daily_aggregated.csv"

    if not os.path.exists(data_file):
        print(f"Data file not found: {data_file}")
        return

    print("=" * 70)
    print("LOADING DATA")
    print("=" * 70)

    df_daily = pd.read_csv(data_file)
    df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])

    print(f"Loaded {len(df_daily)} rows")
    print(f"Columns: {df_daily.columns.tolist()}")
    print(f"Panels: {df_daily['source_id'].unique().tolist()}")

    os.makedirs("outputs", exist_ok=True)

    print("\n" + "=" * 70)
    print("CREATING VISUALIZATIONS")
    print("=" * 70)

    create_all_charts(df_daily, output_folder="outputs")

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

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for panel in df_daily['source_id'].unique():
        panel_data = df_daily[df_daily['source_id'] == panel]
        total = panel_data['energy'].sum()
        print(f"  {panel}: {total:,.2f} kWh")
    print(f"  TOTAL: {df_daily['energy'].sum():,.2f} kWh")

    print("\nDone!")


if __name__ == "__main__":
    main()
