import os
import pandas as pd
from typing import Dict, List, Optional, Tuple

from .schema import SCHEMA, get_aggregation_rules, print_schema
from .detection import auto_detect_columns, generate_llm_prompt, parse_llm_response, format_llm_result_for_review
from .processing import load_file, standardise_dataframe, merge_with_environment, align_timestamps, aggregate_to_period, validate_dataframe


class SolarAggregator:
    """
    Main class for solar data aggregation.
    
    Usage:
        agg = SolarAggregator()
        agg.add_file("inverter1.csv")
        agg.add_file("weather.csv")
        df = agg.aggregate(freq="1D")
        agg.save("output.csv")
    """
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._inverter_data: List[pd.DataFrame] = []
        self._environment_data: List[pd.DataFrame] = []
        self._irradiance_data: Optional[pd.DataFrame] = None
        self._files_loaded: List[str] = []
        self._mappings: Dict[str, Dict] = {}
        self._merged_df: Optional[pd.DataFrame] = None
        self._aligned_df: Optional[pd.DataFrame] = None
        self._aggregated_df: Optional[pd.DataFrame] = None
    
    def _log(self, msg: str):
        if self.verbose:
            print(msg)
    
    def _clear_cache(self):
        self._merged_df = None
        self._aligned_df = None
        self._aggregated_df = None
    
    def add_file(self, filepath: str, source_id: Optional[str] = None, mapping: Optional[Dict[str, str]] = None) -> 'SolarAggregator':
        """Add a data file."""
        filename = os.path.basename(filepath)
        self._log(f"\nAdding: {filename}")
        self._log("-" * 50)
        
        df = load_file(filepath)
        self._log(f"  Rows: {len(df):,}")
        
        if mapping is None:
            mapping, file_type = auto_detect_columns(df)
            self._log(f"  Auto-detected type: {file_type}")
        else:
            file_type = "unknown"
        
        self._log(f"  Mapping: {mapping}")
        
        if source_id is None:
            source_id = filename.replace(".csv", "").replace(".xlsx", "").replace("_data", "").upper()
        
        df_std = standardise_dataframe(df, mapping, source_id)
        self._log(f"  Columns: {list(df_std.columns)}")
        
        self._store_data(df_std, file_type)
        self._files_loaded.append(filename)
        self._mappings[filename] = mapping
        self._clear_cache()
        
        return self
    
    def _store_data(self, df: pd.DataFrame, file_type: str):
        has_energy = 'energy' in df.columns
        
        if file_type == "inverter" or has_energy:
            self._inverter_data.append(df)
            self._log(f"  Stored as: INVERTER")
        elif file_type == "irradiance":
            self._irradiance_data = df
            self._log(f"  Stored as: IRRADIANCE")
        else:
            self._environment_data.append(df)
            self._log(f"  Stored as: ENVIRONMENT")
    
    def aggregate(self, freq: str = '1D', align_freq: str = '15min') -> pd.DataFrame:
        """Run aggregation pipeline."""
        if not self._inverter_data:
            raise ValueError("No inverter data. Add files with add_file() first.")
        
        self._log("\n" + "=" * 60)
        self._log("AGGREGATION PIPELINE")
        self._log("=" * 60)
        
        self._log("\nStep 1: MERGING")
        self._log("-" * 40)

        merged_list = []
        for inv_df in self._inverter_data:
            merged = merge_with_environment(inv_df, self._environment_data or None, self._irradiance_data)
            merged_list.append(merged)

        self._merged_df = pd.concat(merged_list, ignore_index=True).sort_values(['timestamp', 'source_id'])
        self._log(f"  Total: {len(self._merged_df):,} rows")

        self._log(f"\nStep 2: ALIGNING to {align_freq}")
        self._log("-" * 40)
        self._aligned_df = align_timestamps(self._merged_df, freq=align_freq)
        self._log(f"  Before: {len(self._merged_df):,}")
        self._log(f"  After: {len(self._aligned_df):,}")

        self._log(f"\nStep 3: AGGREGATING to {freq}")
        self._log("-" * 40)
        rules = get_aggregation_rules()
        self._log("  Rules:")
        for field, rule in rules.items():
            if field in self._aligned_df.columns:
                self._log(f"    {field}: {rule.upper()}")
        
        self._aggregated_df = aggregate_to_period(self._aligned_df, freq=freq)
        self._log(f"  Output: {len(self._aggregated_df):,} rows")
        
        return self._aggregated_df
    
    def save(self, filepath: str, data: str = "aggregated"):
        """Save to CSV."""
        if data == "aggregated":
            if self._aggregated_df is None:
                self.aggregate()
            df = self._aggregated_df
        elif data == "aligned":
            df = self._aligned_df
        elif data == "merged":
            df = self._merged_df
        else:
            raise ValueError(f"Unknown: {data}")
        
        df.to_csv(filepath, index=False)
        self._log(f"\nSaved {len(df):,} rows to: {filepath}")
    
    def get_summary(self) -> str:
        """Get summary of results."""
        if self._aggregated_df is None:
            self.aggregate()
        
        df = self._aggregated_df
        lines = [
            "", "=" * 60, "SUMMARY", "=" * 60,
            f"Files: {len(self._files_loaded)}",
            f"  {', '.join(self._files_loaded)}",
            "",
            f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}",
            f"Rows: {len(df):,}",
            "",
            "ENERGY BY SOURCE:",
            "-" * 40
        ]
        
        if 'energy' in df.columns:
            for src in sorted(df['source_id'].unique()):
                total = df[df['source_id'] == src]['energy'].sum()
                lines.append(f"  {src}: {total:,.2f} kWh")
            lines.append("-" * 40)
            lines.append(f"  TOTAL: {df['energy'].sum():,.2f} kWh")
        
        lines.append("=" * 60)
        return "\n".join(lines)
    
    def get_dataframe(self, data: str = "aggregated") -> pd.DataFrame:
        """Get DataFrame."""
        if data == "aggregated":
            if self._aggregated_df is None:
                self.aggregate()
            return self._aggregated_df.copy()
        elif data == "aligned":
            return self._aligned_df.copy() if self._aligned_df is not None else None
        elif data == "merged":
            return self._merged_df.copy() if self._merged_df is not None else None
        raise ValueError(f"Unknown: {data}")
    
    @staticmethod
    def print_schema():
        print_schema()


def quick_aggregate(files: List[str], freq: str = '1D', output: Optional[str] = None) -> pd.DataFrame:
    """Quick one-liner aggregation."""
    agg = SolarAggregator()
    for f in files:
        agg.add_file(f)
    df = agg.aggregate(freq=freq)
    if output:
        agg.save(output)
    print(agg.get_summary())
    return df
