"""Tests for solstice.forecasting module.

param_grid is kept to a single combination throughout so GridSearchCV stays
fast — these tests check structural correctness (shapes, types, invariants),
not model quality, so a tiny forest is fine.
"""

import numpy as np
import pandas as pd
import pytest

from solstice.forecasting import ModelMetrics, ScenarioPrediction, SolarForecaster

FAST_GRID = {"n_estimators": [20], "max_depth": [5], "min_samples_leaf": [1]}


def _make_daily_data(n_days=80, seed=0, n_sources=1):
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="D")
    rows = []
    for d in dates:
        ambient_temp = 25 + 5 * np.sin(d.dayofyear / 365 * 2 * np.pi) + rng.normal(0, 1)
        base_energy = 100 + 3 * ambient_temp
        for s in range(n_sources):
            rows.append({
                "timestamp": d,
                "source_id": f"SRC{s}",
                "energy": max(base_energy + rng.normal(0, 5), 1.0),
                "ambient_temp": ambient_temp,
                "humidity": 60 + rng.normal(0, 5),
            })
    return pd.DataFrame(rows)


class TestPrepareData:
    def test_requires_timestamp_column(self):
        df = pd.DataFrame({"energy": [1, 2, 3]})
        with pytest.raises(ValueError, match="timestamp"):
            SolarForecaster().fit(df)

    def test_requires_energy_column(self):
        df = pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=3)})
        with pytest.raises(ValueError, match="energy"):
            SolarForecaster().fit(df)

    def test_sums_energy_across_sources_per_day(self):
        df = _make_daily_data(n_days=40, n_sources=3)
        forecaster = SolarForecaster(test_ratio=0.2)
        forecaster.fit(df, param_grid=FAST_GRID)

        # 3 sources/day collapse into 1 row/day via the sum-energy groupby.
        assert len(forecaster.df_daily) == 40
        assert "day_of_year" in forecaster.df_daily.columns
        assert "month" in forecaster.df_daily.columns


class TestFit:
    def test_fit_populates_metrics(self):
        df = _make_daily_data(n_days=80)
        forecaster = SolarForecaster(test_ratio=0.2)
        forecaster.fit(df, param_grid=FAST_GRID)

        metrics = forecaster.get_metrics()
        assert isinstance(metrics, ModelMetrics)
        assert metrics.n_train + metrics.n_test == len(forecaster.df_daily)
        assert metrics.mae >= 0
        assert metrics.rmse >= 0
        assert "ambient_temp" in metrics.features_used

    def test_get_metrics_raises_before_fit(self):
        with pytest.raises(ValueError, match="not fitted"):
            SolarForecaster().get_metrics()

    def test_raises_without_usable_features(self):
        # NOTE: _prepare_data() always adds day_of_year/month, and both are
        # in DEFAULT_FEATURES, so the features=None auto-detect path can
        # never actually hit "no usable features" — even bare timestamp+
        # energy data "succeeds" using only calendar features. The only way
        # to trigger this guard today is an explicit features= list that
        # matches nothing, which is what's tested here.
        df = _make_daily_data(n_days=40)
        with pytest.raises(ValueError, match="No usable features"):
            SolarForecaster().fit(df, features=["does_not_exist"], param_grid=FAST_GRID)

    def test_custom_features_subset_is_respected(self):
        df = _make_daily_data(n_days=60)
        forecaster = SolarForecaster()
        forecaster.fit(df, features=["ambient_temp"], param_grid=FAST_GRID)
        assert forecaster.feature_cols == ["ambient_temp"]


class TestFeatureImportance:
    def test_returns_sorted_dataframe(self):
        df = _make_daily_data(n_days=60)
        forecaster = SolarForecaster()
        forecaster.fit(df, param_grid=FAST_GRID)

        importance = forecaster.get_feature_importance()
        assert list(importance.columns) == ["feature", "importance"]
        assert len(importance) == len(forecaster.feature_cols)
        assert (importance["importance"].diff().dropna() <= 1e-9).all()  # descending

    def test_raises_before_fit(self):
        with pytest.raises(ValueError, match="not fitted"):
            SolarForecaster().get_feature_importance()


class TestPredictScenarios:
    def test_default_scenarios_include_average(self):
        df = _make_daily_data(n_days=60)
        forecaster = SolarForecaster()
        forecaster.fit(df, param_grid=FAST_GRID)

        scenarios = forecaster.predict_scenarios()
        assert len(scenarios) > 0
        assert any("Average" in s.name for s in scenarios)
        for s in scenarios:
            assert isinstance(s, ScenarioPrediction)
            assert s.predicted_energy >= 0

    def test_custom_scenarios_override_defaults(self):
        df = _make_daily_data(n_days=60)
        forecaster = SolarForecaster()
        forecaster.fit(df, param_grid=FAST_GRID)

        custom = {"Hot day": {"ambient_temp": 40.0}}
        scenarios = forecaster.predict_scenarios(custom_scenarios=custom)
        assert len(scenarios) == 1
        assert scenarios[0].name == "Hot day"
        assert scenarios[0].conditions == {"ambient_temp": 40.0}

    def test_raises_before_fit(self):
        with pytest.raises(ValueError, match="not fitted"):
            SolarForecaster().predict_scenarios()


class TestActualVsPredicted:
    def test_returns_expected_shape(self):
        df = _make_daily_data(n_days=60)
        forecaster = SolarForecaster(test_ratio=0.25)
        forecaster.fit(df, param_grid=FAST_GRID)

        curve = forecaster.get_actual_vs_predicted()
        assert len(curve) == forecaster.metrics.n_test
        assert list(curve.columns) == ["timestamp", "actual", "predicted", "residual"]

    def test_raises_before_fit(self):
        with pytest.raises(ValueError, match="not fitted"):
            SolarForecaster().get_actual_vs_predicted()
