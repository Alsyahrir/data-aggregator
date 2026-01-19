import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from solar_aggregator import (
    load_excel,
    add_timestamp,
    pivot_parameters,
    aggregate_solar
)

FILES = {
    "Panel01": "Data/Excel/SolarPanel01.xlsx",
    "Panel02": "Data/Excel/SolarPanel02.xlsx"
}

VALUE_COLUMN = "Value (Graph Scale : 1.000000 )"

all_panels = []

for panel_id, path in FILES.items():
    df = load_excel(path)
    df = add_timestamp(df)
    wide = pivot_parameters(df, VALUE_COLUMN)
    wide["panel_id"] = panel_id
    all_panels.append(wide)

combined = pd.concat(all_panels, ignore_index=True)

# 🔑 Use the general solar aggregator
daily = aggregate_solar(
    df=combined,
    time_col="timestamp",
    source_col="panel_id",
    energy_col="Net Energy", 
    freq="D"
)

daily.to_csv("daily_aggregation_excel.csv", index=False)

print("Aggregation completed successfully.")
