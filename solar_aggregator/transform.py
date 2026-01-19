import pandas as pd

def add_timestamp(df):
    df["timestamp"] = pd.to_datetime(
        df["Date"].astype(str) + " " + df["Timestamp"].astype(str),
        dayfirst=True,
        errors="coerce"
    )
    return df.dropna(subset=["timestamp"])

def pivot_parameters(df, value_col):
    df = df.rename(columns={value_col: "value"})
    df = df[["timestamp", "Parameter", "value"]]

    return (
        df.pivot_table(
            index="timestamp",
            columns="Parameter",
            values="value",
            aggfunc="mean"
        )
        .reset_index()
    )
