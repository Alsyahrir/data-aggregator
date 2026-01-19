import pandas as pd

def aggregate_solar(
    df,
    time_col,
    source_col,
    energy_col,
    power_col=None,
    temp_col=None,
    irradiance_col=None,
    freq="D"
):
    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.set_index(time_col)

    agg_map = {energy_col: "sum"}

    if power_col:
        agg_map[power_col] = "mean"
    if temp_col:
        agg_map[temp_col] = "mean"
    if irradiance_col:
        agg_map[irradiance_col] = "mean"

    aggregated = (
        df
        .groupby(source_col)
        .resample(freq)
        .agg(agg_map)
        .reset_index()
    )

    return aggregated
