"""
Part 3: Data Processing Pipeline
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from .schema import SCHEMA, get_aggregation_rules


def load_file(filepath: str) -> pd.DataFrame:
    """Load CSV or Excel file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    ext = os.path.splitext(filepath)[1].lower()
    if ext == '.csv':
        return pd.read_csv(filepath)
    elif ext in ['.xlsx', '.xls']:
        return pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported: {ext}")


def standardise_dataframe(df: pd.DataFrame, mapping: Dict[str, str], source_id: str) -> pd.DataFrame:
    """Transform DataFrame to standard schema."""
    df_out = pd.DataFrame()
    
    for source_col, target_field in mapping.items():
        if source_col in df.columns:
            df_out[target_field] = df[source_col].copy()
    
    if "timestamp" in df_out.columns:
        df_out["timestamp"] = pd.to_datetime(df_out["timestamp"], format='mixed', dayfirst=True)
    
    if "source_id" not in df_out.columns:
        df_out["source_id"] = source_id
    
    for field in ["energy", "ambient_temp", "irradiance", "wind_speed", "module_temp", "humidity"]:
        if field in df_out.columns:
            df_out[field] = pd.to_numeric(df_out[field], errors='coerce')
    
    return df_out


def merge_with_environment(
    inverter_df: pd.DataFrame,
    environment_dfs: Optional[List[pd.DataFrame]] = None,
    irradiance_df: Optional[pd.DataFrame] = None,
    tolerance: str = '15min'
) -> pd.DataFrame:
    """Merge inverter with environment data using nearest timestamp."""
    df = inverter_df.copy().sort_values('timestamp').reset_index(drop=True)
    
    if environment_dfs:
        for env_df in environment_dfs:
            env_cols = ['ambient_temp', 'wind_speed', 'humidity', 'module_temp']
            cols = [c for c in env_cols if c in env_df.columns]
            if cols:
                env_sorted = env_df[['timestamp'] + cols].drop_duplicates(subset='timestamp').sort_values('timestamp').reset_index(drop=True)
                df = pd.merge_asof(df, env_sorted, on='timestamp', tolerance=pd.Timedelta(tolerance), direction='nearest')
    
    if irradiance_df is not None and 'irradiance' in irradiance_df.columns:
        irr = irradiance_df[['timestamp', 'irradiance']].drop_duplicates(subset='timestamp').sort_values('timestamp').reset_index(drop=True)
        df = pd.merge_asof(df, irr, on='timestamp', tolerance=pd.Timedelta(tolerance), direction='nearest')
    
    return df


def align_timestamps(df: pd.DataFrame, freq: str = '15min') -> pd.DataFrame:
    """Align timestamps to regular grid."""
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
    
    return df.groupby(['source_id', 'timestamp']).agg(agg_dict).reset_index()


def aggregate_to_period(df: pd.DataFrame, freq: str = '1D') -> pd.DataFrame:
    """Aggregate to daily/weekly/monthly."""
    df = df.copy()
    df['_period'] = df['timestamp'].dt.to_period(freq)
    
    agg_dict = {}
    for col in df.columns:
        if col in ['timestamp', 'source_id', '_period']:
            continue
        if col in SCHEMA:
            agg_dict[col] = SCHEMA[col].aggregation.value
        elif pd.api.types.is_numeric_dtype(df[col]):
            agg_dict[col] = 'mean'
    
    agg_dict['observation_count'] = ('timestamp', 'count')
    
    df_agg = df.groupby(['source_id', '_period']).agg(**{
        col: (col, method) if col != 'observation_count' else method
        for col, method in agg_dict.items()
    }).reset_index()
    
    df_agg['timestamp'] = df_agg['_period'].dt.to_timestamp()
    df_agg = df_agg.drop(columns=['_period'])
    
    cols = ['timestamp', 'source_id'] + [c for c in df_agg.columns if c not in ['timestamp', 'source_id']]
    return df_agg[cols]


def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Validate DataFrame."""
    errors = []
    for name, field in SCHEMA.items():
        if field.required and name not in df.columns:
            errors.append(f"Missing: {name}")
    if 'energy' in df.columns and (df['energy'] < 0).sum() > 0:
        errors.append("Negative energy values")
    return len(errors) == 0, errors
