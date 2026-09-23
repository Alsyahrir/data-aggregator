import os
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union

from .schema import SCHEMA, get_aggregation_rules

logger = logging.getLogger(__name__)


# ── Reasonable physical bounds for solar data ────────────────────────────────

PHYSICAL_BOUNDS = {
    "energy":       (0, 100_000),  # kWh per period
    "irradiance":   (0, 1500),     # W/m²
    "ambient_temp": (-40, 60),     # °C
    "module_temp":  (-40, 100),    # °C
    "humidity":     (0, 100),      # %
    "wind_speed":   (0, 100),      # m/s
    "power":        (0, 1_000_000),# W
    "voltage":      (0, 2000),     # V
    "current":      (0, 5000),     # A
}


def load_file(filepath: str) -> pd.DataFrame:
    """Load data from a CSV or Excel file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    ext = os.path.splitext(filepath)[1].lower()
    
    if ext == '.csv':
        return pd.read_csv(filepath)
    elif ext in ['.xlsx', '.xls']:
        return pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def standardise_dataframe(
    df: pd.DataFrame,
    mapping: Dict[str, str],
    source_id: str,
    dayfirst: Optional[bool] = None,
) -> pd.DataFrame:
    """Standardise DataFrame - keeps all columns, renames mapped ones.

    Args:
        dayfirst: How to read ambiguous dates like 03/11/2025.
            True  -> day first (11 March), the European/Asian convention.
            False -> month first (3 November), the US convention.
            None  -> let pandas infer (default). Ambiguous all-numeric dates
                     are guessed, so pass this explicitly when you know the
                     source format — silently reading US data as day-first
                     shifts every date into the wrong month.
    """
    df_out = df.copy()

    rename_dict = {src: tgt for src, tgt in mapping.items() if src in df_out.columns}
    df_out = df_out.rename(columns=rename_dict)

    if "timestamp" in df_out.columns:
        kwargs = {"format": "mixed"}
        if dayfirst is not None:
            kwargs["dayfirst"] = dayfirst
        df_out["timestamp"] = pd.to_datetime(df_out["timestamp"], **kwargs)

    if "source_id" not in df_out.columns:
        df_out["source_id"] = source_id

    for field in ["energy", "ambient_temp", "irradiance", "wind_speed", "module_temp", "humidity"]:
        if field in df_out.columns:
            df_out[field] = pd.to_numeric(df_out[field], errors='coerce')
    
    return df_out


def merge_with_environment(
    inverter_df: pd.DataFrame,
    environment_dfs: Optional[List[pd.DataFrame]] = None,
    irradiance_df: Optional[Union[pd.DataFrame, List[pd.DataFrame]]] = None,
    tolerance: str = '15min'
) -> pd.DataFrame:
    """Merge inverter data with environmental data using nearest timestamp.

    `irradiance_df` accepts a single DataFrame or a list of them (multiple
    sensors); the first one carrying an `irradiance` column is used.
    """
    df = inverter_df.copy().sort_values('timestamp').reset_index(drop=True)
    
    if environment_dfs:
        for env_df in environment_dfs:
            cols_to_merge = [c for c in env_df.columns if c not in ['timestamp', 'source_id'] and c not in df.columns]
            
            if cols_to_merge:
                env_sorted = (
                    env_df[['timestamp'] + cols_to_merge]
                    .drop_duplicates(subset='timestamp')
                    .sort_values('timestamp')
                    .reset_index(drop=True)
                )
                df = pd.merge_asof(
                    df, env_sorted,
                    on='timestamp',
                    tolerance=pd.Timedelta(tolerance),
                    direction='nearest'
                )
    
    if irradiance_df is not None and 'irradiance' not in df.columns:
        irradiance_dfs = (
            irradiance_df if isinstance(irradiance_df, list) else [irradiance_df]
        )
        for irr_df in irradiance_dfs:
            if irr_df is None or 'irradiance' not in irr_df.columns:
                continue
            irr_sorted = (
                irr_df[['timestamp', 'irradiance']]
                .drop_duplicates(subset='timestamp')
                .sort_values('timestamp')
                .reset_index(drop=True)
            )
            df = pd.merge_asof(
                df, irr_sorted,
                on='timestamp',
                tolerance=pd.Timedelta(tolerance),
                direction='nearest'
            )
            break
    
    return df


def align_timestamps(df: pd.DataFrame, freq: str = '15min') -> pd.DataFrame:
    """Align timestamps to a regular grid."""
    df = df.copy()
    df['timestamp'] = df['timestamp'].dt.floor(freq)
    
    agg_dict = {}
    for col in df.columns:
        if col in ['timestamp', 'source_id']:
            continue
        if col in SCHEMA:
            agg_dict[col] = SCHEMA[col].aggregation.value
        elif pd.api.types.is_numeric_dtype(df[col]):
            agg_dict[col] = 'mean'
        else:
            agg_dict[col] = 'first'
    
    return df.groupby(['source_id', 'timestamp']).agg(agg_dict).reset_index()


def aggregate_to_period(df: pd.DataFrame, freq: str = '1D') -> pd.DataFrame:
    """Aggregate data to a target time period."""
    df = df.copy()
    df['_period'] = df['timestamp'].dt.to_period(freq)
    
    agg_funcs = {}
    for col in df.columns:
        if col in ['timestamp', 'source_id', '_period']:
            continue
        if col in SCHEMA:
            agg_funcs[col] = (col, SCHEMA[col].aggregation.value)
        elif pd.api.types.is_numeric_dtype(df[col]):
            agg_funcs[col] = (col, 'mean')
        else:
            agg_funcs[col] = (col, 'first')
    
    agg_funcs['observation_count'] = ('timestamp', 'count')
    
    df_agg = df.groupby(['source_id', '_period']).agg(**agg_funcs).reset_index()
    df_agg['timestamp'] = df_agg['_period'].dt.to_timestamp()
    df_agg = df_agg.drop(columns=['_period'])
    
    cols = ['timestamp', 'source_id'] + [c for c in df_agg.columns if c not in ['timestamp', 'source_id']]
    return df_agg[cols]


def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[Dict[str, str]]]:
    """Validate a DataFrame against the schema.

    Returns (is_valid, issues) where each issue is a dict with
    keys: 'severity' ('error' | 'warning'), 'message'.
    """
    issues: List[Dict[str, str]] = []

    def _err(msg: str):
        issues.append({"severity": "error", "message": msg})

    def _warn(msg: str):
        issues.append({"severity": "warning", "message": msg})

    # Required columns
    required = [name for name, field in SCHEMA.items() if field.required]
    for col in required:
        if col not in df.columns:
            _err(f"Missing required column: {col}")

    # Timestamp checks
    if 'timestamp' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            _err("'timestamp' is not datetime type")
        null_ts = df['timestamp'].isna().sum()
        if null_ts > 0:
            _err(f"Found {null_ts} null timestamps")

        # Check for large time gaps
        if 'source_id' in df.columns:
            for src in df['source_id'].unique():
                src_df = df[df['source_id'] == src].sort_values('timestamp')
                if len(src_df) > 1:
                    gaps = src_df['timestamp'].diff().dt.days
                    large_gaps = gaps[gaps > 7]
                    if len(large_gaps) > 0:
                        _warn(f"Source '{src}': {len(large_gaps)} gap(s) > 7 days found")

        # Check for duplicate timestamps per source
        if 'source_id' in df.columns:
            dupes = df.groupby('source_id')['timestamp'].apply(
                lambda ts: ts.duplicated().sum()
            )
            for src, count in dupes.items():
                if count > 0:
                    _warn(f"Source '{src}': {count} duplicate timestamp(s)")

    # Energy checks
    if 'energy' in df.columns:
        neg = (df['energy'] < 0).sum()
        if neg > 0:
            _err(f"Found {neg} negative energy values")

    # Physical bounds checks
    for col, (lo, hi) in PHYSICAL_BOUNDS.items():
        if col in df.columns:
            out_of_range = ((df[col] < lo) | (df[col] > hi)).sum()
            if out_of_range > 0:
                _warn(
                    f"'{col}': {out_of_range} value(s) outside expected range "
                    f"[{lo}, {hi}] {SCHEMA.get(col, None) and SCHEMA[col].unit or ''}"
                )

    has_errors = any(i['severity'] == 'error' for i in issues)
    return not has_errors, issues


# ── Anomaly Detection ─────────────────────────────────────────────────────────

def detect_anomalies(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    method: str = "iqr",
    iqr_factor: float = 1.5,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """Detect anomalies in numeric columns.

    Args:
        df: Input DataFrame.
        columns: Columns to check. Defaults to all numeric schema fields present.
        method: 'iqr' (Interquartile Range) or 'zscore'.
        iqr_factor: Multiplier for IQR bounds (default 1.5).
        z_threshold: Z-score threshold (default 3.0).

    Returns:
        A copy of df with boolean `is_anomaly` column and per-column
        anomaly flag columns named `_anomaly_{col}`.
    """
    df = df.copy()
    target_cols = columns or [
        c for c in df.columns
        if c in SCHEMA and pd.api.types.is_numeric_dtype(df[c])
    ]

    anomaly_mask = pd.Series(False, index=df.index)

    for col in target_cols:
        series = df[col].dropna()
        if len(series) < 10:
            df[f"_anomaly_{col}"] = False
            continue

        if method == "iqr":
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - iqr_factor * iqr
            upper = q3 + iqr_factor * iqr
            col_anomaly = (df[col] < lower) | (df[col] > upper)
        elif method == "zscore":
            mean = series.mean()
            std = series.std()
            if std == 0:
                col_anomaly = pd.Series(False, index=df.index)
            else:
                col_anomaly = ((df[col] - mean).abs() / std) > z_threshold
        else:
            raise ValueError(f"Unknown method: {method}. Use 'iqr' or 'zscore'.")

        # Also flag values outside physical bounds
        if col in PHYSICAL_BOUNDS:
            lo, hi = PHYSICAL_BOUNDS[col]
            col_anomaly = col_anomaly | (df[col] < lo) | (df[col] > hi)

        col_anomaly = col_anomaly.fillna(False)
        df[f"_anomaly_{col}"] = col_anomaly
        anomaly_mask = anomaly_mask | col_anomaly

    df["is_anomaly"] = anomaly_mask
    n_anomalies = anomaly_mask.sum()
    logger.info(
        "Anomaly detection (%s): %d / %d rows flagged (%.1f%%)",
        method, n_anomalies, len(df),
        n_anomalies / len(df) * 100 if len(df) > 0 else 0,
    )
    return df


def auto_clean(
    df: pd.DataFrame,
    strategy: str = "flag",
    columns: Optional[List[str]] = None,
    method: str = "iqr",
    iqr_factor: float = 1.5,
) -> pd.DataFrame:
    """Detect anomalies and optionally clean them.

    Args:
        df: Input DataFrame.
        strategy:
            'flag'  — add is_anomaly column, keep all rows (default).
            'drop'  — remove anomalous rows.
            'clip'  — clip values to IQR bounds.
        columns: Columns to check.
        method: Detection method ('iqr' or 'zscore').
        iqr_factor: IQR multiplier.

    Returns:
        Cleaned DataFrame.
    """
    df = detect_anomalies(df, columns=columns, method=method, iqr_factor=iqr_factor)

    if strategy == "flag":
        return df
    elif strategy == "drop":
        before = len(df)
        df = df[~df["is_anomaly"]].copy()
        logger.info("auto_clean(drop): removed %d rows", before - len(df))
        # Drop anomaly columns
        df = df.drop(columns=[c for c in df.columns if c.startswith("_anomaly_") or c == "is_anomaly"])
        return df
    elif strategy == "clip":
        target_cols = columns or [
            c for c in df.columns
            if c in SCHEMA and pd.api.types.is_numeric_dtype(df[c])
        ]
        for col in target_cols:
            series = df[col].dropna()
            if len(series) < 10:
                continue
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - iqr_factor * iqr
            upper = q3 + iqr_factor * iqr
            df[col] = df[col].clip(lower=lower, upper=upper)
        logger.info("auto_clean(clip): clipped values to IQR bounds")
        # Drop anomaly columns
        df = df.drop(columns=[c for c in df.columns if c.startswith("_anomaly_") or c == "is_anomaly"])
        return df
    else:
        raise ValueError(f"Unknown strategy: {strategy}. Use 'flag', 'drop', or 'clip'.")