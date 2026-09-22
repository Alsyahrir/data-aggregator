"""Tests for solstice.visualization module.

Each plot function is exercised at 1, 2, and 4 source_ids — the 4-source
case is a regression test for a real bug this module used to have
(hardcoded 2-color palettes that silently collided, or bar-offset math
only correct for exactly 2 sources) — see git history on visualization.py.
"""

import numpy as np
import pandas as pd
import pytest

from solstice.visualization import (
    create_all_charts,
    plot_data_quality,
    plot_distribution,
    plot_energy_production,
    plot_monthly_summary,
    plot_panel_comparison,
    plot_weekly_pattern,
)


def _make_multi_source_df(n_sources=4, n_days=60, seed=1):
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="D")
    rows = []
    for i in range(n_sources):
        for d in dates:
            rows.append({
                "timestamp": d,
                "source_id": f"PANEL{i}",
                "energy": max(rng.normal(100, 20), 0.0),
            })
    return pd.DataFrame(rows)


PLOT_FUNCS = [
    plot_energy_production,
    plot_monthly_summary,
    plot_panel_comparison,
    plot_data_quality,
    plot_weekly_pattern,
    plot_distribution,
]
PLOT_FUNC_IDS = [f.__name__ for f in PLOT_FUNCS]


class TestPlotFunctionsRender:
    @pytest.mark.parametrize("n_sources", [1, 2, 4])
    @pytest.mark.parametrize("plot_fn", PLOT_FUNCS, ids=PLOT_FUNC_IDS)
    def test_renders_and_saves_a_nonempty_file(self, tmp_path, plot_fn, n_sources):
        df = _make_multi_source_df(n_sources=n_sources)
        out = tmp_path / f"{plot_fn.__name__}_{n_sources}.png"
        plot_fn(df, str(out))

        assert out.exists()
        assert out.stat().st_size > 0


class TestCreateAllCharts:
    def test_creates_all_expected_files(self, tmp_path):
        df = _make_multi_source_df(n_sources=3)
        out_dir = tmp_path / "charts"
        create_all_charts(df, output_folder=str(out_dir))

        expected = {
            "energy_production.png", "monthly_summary.png", "panel_comparison.png",
            "data_quality.png", "weekly_pattern.png", "distribution.png",
        }
        actual = {p.name for p in out_dir.iterdir()}
        assert expected == actual

    def test_creates_output_folder_if_missing(self, tmp_path):
        df = _make_multi_source_df(n_sources=1)
        out_dir = tmp_path / "nested" / "charts"
        assert not out_dir.exists()
        create_all_charts(df, output_folder=str(out_dir))
        assert out_dir.exists()
