"""
Part 6: Visualization Functions

Charts for solar data analysis - optimized for daily data.
"""

import pandas as pd
import numpy as np
from typing import Optional


def plot_energy_production(df: pd.DataFrame, save_path: Optional[str] = None):
    """Plot daily energy production over time."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("matplotlib not installed. Run: pip install matplotlib")
        return
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    colors = {'Panel01': '#2ecc71', 'Panel02': '#e74c3c', 'PANEL01': '#2ecc71', 'PANEL02': '#e74c3c'}
    
    for source in sorted(df['source_id'].unique()):
        source_df = df[df['source_id'] == source].sort_values('timestamp')
        # Filter out zero/null values for cleaner chart
        source_df = source_df[source_df['energy'] > 0]
        
        color = colors.get(source, '#3498db')
        ax.plot(source_df['timestamp'], source_df['energy'], 
               label=source, alpha=0.8, linewidth=1.5, color=color)
    
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Energy (kWh/day)', fontsize=12)
    ax.set_title('Daily Solar Energy Production', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=11)
    ax.grid(True, alpha=0.3)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.xticks(rotation=45)
    
    # Add total annotation
    total = df['energy'].sum()
    ax.annotate(f'Total: {total:,.0f} kWh', 
               xy=(0.98, 0.98), xycoords='axes fraction',
               fontsize=12, fontweight='bold',
               horizontalalignment='right', verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {save_path}")
    plt.close()


def plot_monthly_summary(df: pd.DataFrame, save_path: Optional[str] = None):
    """Plot monthly energy totals as bar chart."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed")
        return
    
    df = df.copy()
    df['month'] = df['timestamp'].dt.to_period('M')
    
    monthly = df.groupby(['month', 'source_id'])['energy'].sum().unstack(fill_value=0)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    colors = ['#2ecc71', '#e74c3c', '#3498db', '#f39c12']
    monthly.plot(kind='bar', ax=ax, width=0.8, color=colors[:len(monthly.columns)])
    
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Total Energy (kWh)', fontsize=12)
    ax.set_title('Monthly Energy Production by Panel', fontsize=14, fontweight='bold')
    ax.legend(title='Panel', fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Format x-axis labels
    ax.set_xticklabels([str(x) for x in monthly.index], rotation=45, ha='right')
    
    # Add value labels on bars
    for container in ax.containers:
        ax.bar_label(container, fmt='%.0f', fontsize=8, rotation=90, padding=3)
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {save_path}")
    plt.close()


def plot_panel_comparison(df: pd.DataFrame, save_path: Optional[str] = None):
    """Compare total energy between panels."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed")
        return
    
    totals = df.groupby('source_id')['energy'].sum().sort_values(ascending=True)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = ['#2ecc71', '#e74c3c', '#3498db', '#f39c12']
    bars = ax.barh(totals.index, totals.values, color=colors[:len(totals)])
    
    ax.set_xlabel('Total Energy (kWh)', fontsize=12)
    ax.set_ylabel('Panel', fontsize=12)
    ax.set_title('Total Energy Production by Panel', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='x')
    
    # Add value labels
    for bar, val in zip(bars, totals.values):
        ax.text(val + totals.max()*0.01, bar.get_y() + bar.get_height()/2,
               f'{val:,.0f} kWh', va='center', fontsize=11, fontweight='bold')
    
    # Add percentage labels
    total = totals.sum()
    for bar, val in zip(bars, totals.values):
        pct = val / total * 100
        ax.text(val/2, bar.get_y() + bar.get_height()/2,
               f'{pct:.1f}%', va='center', ha='center', fontsize=12, 
               color='white', fontweight='bold')
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {save_path}")
    plt.close()


def plot_data_quality(df: pd.DataFrame, save_path: Optional[str] = None):
    """Plot data quality - missing values and data coverage."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed")
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Left: Missing data by column
    ax1 = axes[0]
    null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=True)
    
    colors = ['#2ecc71' if pct < 10 else '#f39c12' if pct < 50 else '#e74c3c' 
              for pct in null_pct.values]
    
    bars = ax1.barh(null_pct.index, null_pct.values, color=colors)
    ax1.set_xlabel('Missing Data (%)', fontsize=11)
    ax1.set_title('Missing Values by Column', fontsize=12, fontweight='bold')
    ax1.set_xlim(0, 100)
    ax1.grid(True, alpha=0.3, axis='x')
    
    for bar, pct in zip(bars, null_pct.values):
        if pct > 0:
            ax1.text(pct + 2, bar.get_y() + bar.get_height()/2, 
                   f'{pct:.1f}%', va='center', fontsize=10)
    
    # Right: Data coverage by source
    ax2 = axes[1]
    
    coverage = df.groupby('source_id').agg({
        'timestamp': ['min', 'max', 'count'],
        'energy': lambda x: x.notna().sum()
    })
    coverage.columns = ['start', 'end', 'total_rows', 'valid_energy']
    coverage['coverage_pct'] = coverage['valid_energy'] / coverage['total_rows'] * 100
    
    bars2 = ax2.barh(coverage.index, coverage['coverage_pct'], color=['#2ecc71', '#e74c3c'])
    ax2.set_xlabel('Data Coverage (%)', fontsize=11)
    ax2.set_title('Valid Energy Readings by Panel', fontsize=12, fontweight='bold')
    ax2.set_xlim(0, 100)
    ax2.grid(True, alpha=0.3, axis='x')
    
    for bar, row in zip(bars2, coverage.itertuples()):
        ax2.text(row.coverage_pct + 2, bar.get_y() + bar.get_height()/2,
               f'{row.coverage_pct:.1f}% ({row.valid_energy}/{row.total_rows})',
               va='center', fontsize=10)
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {save_path}")
    plt.close()


def plot_weekly_pattern(df: pd.DataFrame, save_path: Optional[str] = None):
    """Plot average energy by day of week."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed")
        return
    
    df = df.copy()
    df = df[df['energy'] > 0]  # Only valid readings
    df['dayofweek'] = df['timestamp'].dt.dayofweek
    
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = ['#2ecc71', '#e74c3c']
    
    for i, source in enumerate(sorted(df['source_id'].unique())):
        source_df = df[df['source_id'] == source]
        daily_avg = source_df.groupby('dayofweek')['energy'].mean()
        
        x = [d + i*0.35 - 0.175 for d in daily_avg.index]
        ax.bar(x, daily_avg.values, width=0.35, label=source, color=colors[i % 2], alpha=0.8)
    
    ax.set_xlabel('Day of Week', fontsize=12)
    ax.set_ylabel('Average Energy (kWh)', fontsize=12)
    ax.set_title('Average Daily Production by Day of Week', fontsize=14, fontweight='bold')
    ax.set_xticks(range(7))
    ax.set_xticklabels(days)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {save_path}")
    plt.close()


def plot_distribution(df: pd.DataFrame, save_path: Optional[str] = None):
    """Plot energy distribution histogram."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed")
        return
    
    df = df[df['energy'] > 0].copy()  # Only valid readings
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    colors = ['#2ecc71', '#e74c3c']
    
    for i, source in enumerate(sorted(df['source_id'].unique())):
        source_df = df[df['source_id'] == source]
        ax.hist(source_df['energy'], bins=30, alpha=0.6, label=source, 
               color=colors[i % 2], edgecolor='white')
    
    ax.set_xlabel('Daily Energy (kWh)', fontsize=12)
    ax.set_ylabel('Frequency (days)', fontsize=12)
    ax.set_title('Distribution of Daily Energy Production', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add mean lines
    for i, source in enumerate(sorted(df['source_id'].unique())):
        source_df = df[df['source_id'] == source]
        mean_val = source_df['energy'].mean()
        ax.axvline(mean_val, color=colors[i % 2], linestyle='--', linewidth=2)
        ax.text(mean_val, ax.get_ylim()[1]*0.9, f'{source}\nmean: {mean_val:,.0f}',
               ha='center', fontsize=9, color=colors[i % 2])
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {save_path}")
    plt.close()


def create_all_charts(df: pd.DataFrame, output_folder: str = "outputs"):
    """Create all visualization charts at once."""
    import os
    os.makedirs(output_folder, exist_ok=True)
    
    print("Creating charts...")
    
    plot_energy_production(df, f"{output_folder}/energy_production.png")
    plot_monthly_summary(df, f"{output_folder}/monthly_summary.png")
    plot_panel_comparison(df, f"{output_folder}/panel_comparison.png")
    plot_data_quality(df, f"{output_folder}/data_quality.png")
    plot_weekly_pattern(df, f"{output_folder}/weekly_pattern.png")
    plot_distribution(df, f"{output_folder}/distribution.png")
    
    print(f"\nAll charts saved to {output_folder}/")


# Keep old functions for compatibility
def plot_time_alignment(df_before, df_after, source_id=None, save_path=None):
    """Plot time alignment (for sub-hourly data only)."""
    print("Note: Time alignment chart is for sub-hourly data. Skipping.")


def print_time_alignment_report(df_before, df_after):
    """Print data summary."""
    print("=" * 60)
    print("DATA SUMMARY")
    print("=" * 60)
    print(f"Total rows: {len(df_after):,}")
    print(f"Date range: {df_after['timestamp'].min()} to {df_after['timestamp'].max()}")
    if 'energy' in df_after.columns:
        print(f"Total energy: {df_after['energy'].sum():,.2f} kWh")
    print("=" * 60)