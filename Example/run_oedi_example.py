import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from solar_aggregator import aggregate_solar


# -------------------------------------------------
# HELPER: load OEDI CSV (robust datetime parsing)
# -------------------------------------------------
def load_oedi_csv(path):
    return (
        pd.read_csv(path)
        .assign(
            measured_on=lambda x: pd.to_datetime(
                x["measured_on"],
                format="mixed",
                dayfirst=True,
                errors="coerce"
            )
        )
        .dropna(subset=["measured_on"])
        .sort_values("measured_on")
    )

inv01 = load_oedi_csv("Data/OEDI/2105_inv01_data.csv")
inv11 = load_oedi_csv("Data/OEDI/2105_inv11_data.csv")

env = load_oedi_csv("Data/OEDI/2105_environment_1_data.csv")
irr = load_oedi_csv("Data/OEDI//2105_irradiance_data.csv")



# -------------------------------------------------
# CLEAN / RENAME CONTEXT DATA
# -------------------------------------------------
env = env.rename(columns={
    "ambient_temp_(c)_o_150228": "ambient_temp"
})
env = env[["measured_on", "ambient_temp"]]

irr = irr.rename(columns={
    "irradiance_ghi_o_150230": "ghi"
})
irr = irr[["measured_on", "ghi"]]


# -------------------------------------------------
# MERGE CONTEXT INTO EACH INVERTER
# -------------------------------------------------
def prepare_inverter(inv_df, inverter_id):
    df = pd.merge_asof(inv_df, env, on="measured_on", direction="nearest")
    df = pd.merge_asof(df, irr, on="measured_on", direction="nearest")
    df["system_id"] = inverter_id
    return df


inv01_merged = prepare_inverter(inv01, "inv01")
inv11_merged = prepare_inverter(inv11, "inv11")


# -------------------------------------------------
# COMBINE BOTH INVERTERS
# -------------------------------------------------
combined = pd.concat([inv01_merged, inv11_merged], ignore_index=True)

# Rename energy column ONCE
combined = combined.rename(columns={
    "inv_string01_ac_output_(kwh)_inv_150164": "energy_kwh"
})


# -------------------------------------------------
# AGGREGATE (ENERGY + ENV + IRRADIANCE)
# -------------------------------------------------
daily = aggregate_solar(
    df=combined,
    time_col="measured_on",
    source_col="system_id",
    energy_col="energy_kwh",
    temp_col="ambient_temp",
    irradiance_col="ghi",
    freq="D"
)

daily.to_csv("oedi_daily_aggregation.csv", index=False)

print("OEDI daily aggregation completed successfully.")