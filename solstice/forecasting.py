"""
Solar Energy Forecasting Module

Extracts the forecasting logic from the Colab notebook into a reusable
module that can be called from the Streamlit app or standalone scripts.

Usage:
    from solstice.forecasting import SolarForecaster

    forecaster = SolarForecaster()
    forecaster.fit(df)
    metrics = forecaster.get_metrics()
    scenario_preds = forecaster.predict_scenarios()
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# All potential feature columns (from weather enrichment + time)
DEFAULT_FEATURES = [
    "irradiance",
    "sunshine_hours",
    "temperature_2m_max",
    "temperature_2m_min",
    "ambient_temp",
    "humidity",
    "relative_humidity_2m_mean",
    "precipitation_sum",
    "cloud_cover_mean",
    "wind_speed",
    "windspeed_10m_max",
    "clearness_index",
    "temp_range",
    "day_of_year",
    "month",
    "module_temp",
]


@dataclass
class ModelMetrics:
    """Container for model evaluation metrics."""
    mae: float = 0.0
    rmse: float = 0.0
    r2: float = 0.0
    mape: float = 0.0
    n_train: int = 0
    n_test: int = 0
    best_params: Dict = field(default_factory=dict)
    features_used: List[str] = field(default_factory=list)


@dataclass
class ScenarioPrediction:
    """A single scenario prediction result."""
    name: str
    predicted_energy: float
    conditions: Dict[str, float]


class SolarForecaster:
    """Random Forest forecaster for daily solar energy production.

    Wraps sklearn's RandomForestRegressor with solar-domain logic:
    time-series split, feature engineering, and weather scenario analysis.
    """

    def __init__(self, test_ratio: float = 0.2, random_state: int = 42):
        self.test_ratio = test_ratio
        self.random_state = random_state

        self.model = None
        self.metrics: Optional[ModelMetrics] = None
        self.feature_cols: List[str] = []
        self.df_daily: Optional[pd.DataFrame] = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.y_pred = None
        self.dates_test = None

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare daily data for modelling."""
        df = df.copy()
        if "timestamp" not in df.columns:
            raise ValueError("DataFrame must have a 'timestamp' column")
        if "energy" not in df.columns:
            raise ValueError("DataFrame must have an 'energy' column")

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["energy"] > 0].copy()

        # Group by date (sum energy across sources per day)
        df_daily = df.groupby(df["timestamp"].dt.normalize()).agg(
            {col: "sum" if col == "energy" else "first"
             for col in df.columns if col != "timestamp"}
        ).reset_index()
        df_daily = df_daily.rename(columns={"timestamp": "timestamp"})

        # Add time features
        df_daily["day_of_year"] = df_daily["timestamp"].dt.dayofyear
        df_daily["month"] = df_daily["timestamp"].dt.month

        return df_daily.sort_values("timestamp").reset_index(drop=True)

    def fit(
        self,
        df: pd.DataFrame,
        features: Optional[List[str]] = None,
        param_grid: Optional[Dict] = None,
    ) -> "SolarForecaster":
        """Fit the forecasting model.

        Args:
            df: Aggregated (and optionally weather-enriched) DataFrame.
            features: Feature column names. Defaults to auto-detecting
                      available columns from DEFAULT_FEATURES.
            param_grid: Hyperparameter grid for GridSearchCV.
                        Defaults to a sensible preset.

        Returns:
            self (for chaining).
        """
        try:
            from sklearn.ensemble import RandomForestRegressor
            from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
        except ImportError:
            raise ImportError(
                "scikit-learn is required for forecasting. "
                "Run: pip install scikit-learn"
            )

        self.df_daily = self._prepare_data(df)

        # Select features
        if features is None:
            self.feature_cols = [
                c for c in DEFAULT_FEATURES
                if c in self.df_daily.columns
                and pd.api.types.is_numeric_dtype(self.df_daily[c])
            ]
        else:
            self.feature_cols = [c for c in features if c in self.df_daily.columns]

        if not self.feature_cols:
            raise ValueError(
                "No usable features found. Consider enriching with weather data first. "
                f"Available columns: {list(self.df_daily.columns)}"
            )

        logger.info("Forecasting with %d features: %s", len(self.feature_cols), self.feature_cols)

        X = self.df_daily[self.feature_cols].fillna(
            self.df_daily[self.feature_cols].median()
        )
        y = self.df_daily["energy"]

        # Chronological train/test split
        split_idx = int(len(self.df_daily) * (1 - self.test_ratio))
        self.X_train, self.X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        self.y_train, self.y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        self.dates_test = self.df_daily["timestamp"].iloc[split_idx:]

        logger.info("Train: %d samples, Test: %d samples", len(self.X_train), len(self.X_test))

        # Default hyperparameter grid
        if param_grid is None:
            param_grid = {
                "n_estimators": [100, 200],
                "max_depth": [10, 20],
                "min_samples_leaf": [1, 2],
            }

        rf = RandomForestRegressor(random_state=self.random_state)
        grid = GridSearchCV(
            rf, param_grid,
            cv=TimeSeriesSplit(n_splits=min(3, max(2, split_idx // 10))),
            scoring="neg_mean_absolute_error",
            n_jobs=-1,
        )
        grid.fit(self.X_train, self.y_train)
        self.model = grid.best_estimator_

        # Evaluate
        self.y_pred = self.model.predict(self.X_test)
        mae = mean_absolute_error(self.y_test, self.y_pred)
        rmse = np.sqrt(mean_squared_error(self.y_test, self.y_pred))
        r2 = r2_score(self.y_test, self.y_pred)
        mape = np.mean(np.abs((self.y_test - self.y_pred) / self.y_test)) * 100

        self.metrics = ModelMetrics(
            mae=mae, rmse=rmse, r2=r2, mape=mape,
            n_train=len(self.X_train), n_test=len(self.X_test),
            best_params=grid.best_params_,
            features_used=self.feature_cols,
        )

        logger.info("Model trained — MAE: %.2f, R²: %.4f, MAPE: %.2f%%", mae, r2, mape)
        return self

    def get_metrics(self) -> ModelMetrics:
        """Return model evaluation metrics."""
        if self.metrics is None:
            raise ValueError("Model not fitted yet. Call fit() first.")
        return self.metrics

    def get_feature_importance(self) -> pd.DataFrame:
        """Return feature importance as a sorted DataFrame."""
        if self.model is None:
            raise ValueError("Model not fitted yet. Call fit() first.")
        importance = pd.DataFrame({
            "feature": self.feature_cols,
            "importance": self.model.feature_importances_,
        }).sort_values("importance", ascending=False).reset_index(drop=True)
        return importance

    def predict_scenarios(
        self,
        custom_scenarios: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> List[ScenarioPrediction]:
        """Predict energy for different weather scenarios.

        Args:
            custom_scenarios: Dict of {scenario_name: {feature: value}}.
                              If None, uses sensible defaults.

        Returns:
            List of ScenarioPrediction objects.
        """
        if self.model is None or self.df_daily is None:
            raise ValueError("Model not fitted yet. Call fit() first.")

        baseline = self.df_daily[self.feature_cols].median().to_dict()

        if custom_scenarios is None:
            df = self.df_daily
            custom_scenarios = {}

            # Build scenarios dynamically based on available features
            ideal = {}
            if "irradiance" in self.feature_cols:
                ideal["irradiance"] = df["irradiance"].quantile(0.9)
            if "sunshine_hours" in self.feature_cols:
                ideal["sunshine_hours"] = df["sunshine_hours"].quantile(0.9)
            if "cloud_cover_mean" in self.feature_cols:
                ideal["cloud_cover_mean"] = df["cloud_cover_mean"].quantile(0.1)
            if "precipitation_sum" in self.feature_cols:
                ideal["precipitation_sum"] = 0
            if ideal:
                custom_scenarios["☀️ Ideal (Clear sky, high radiation)"] = ideal

            custom_scenarios["📊 Average conditions"] = {}

            cloudy = {}
            if "cloud_cover_mean" in self.feature_cols:
                cloudy["cloud_cover_mean"] = df["cloud_cover_mean"].quantile(0.9)
            if "irradiance" in self.feature_cols:
                cloudy["irradiance"] = df["irradiance"].quantile(0.3)
            if "sunshine_hours" in self.feature_cols:
                cloudy["sunshine_hours"] = df["sunshine_hours"].quantile(0.3)
            if cloudy:
                custom_scenarios["☁️ Cloudy day"] = cloudy

            rainy = {}
            if "precipitation_sum" in self.feature_cols:
                rainy["precipitation_sum"] = df["precipitation_sum"].quantile(0.9)
            if "cloud_cover_mean" in self.feature_cols:
                rainy["cloud_cover_mean"] = df["cloud_cover_mean"].quantile(0.9)
            if "irradiance" in self.feature_cols:
                rainy["irradiance"] = df["irradiance"].quantile(0.2)
            if rainy:
                custom_scenarios["🌧️ Rainy day"] = rainy

            hot_humid = {}
            if "ambient_temp" in self.feature_cols:
                hot_humid["ambient_temp"] = df["ambient_temp"].quantile(0.95)
            if "humidity" in self.feature_cols:
                hot_humid["humidity"] = df["humidity"].quantile(0.9)
            if hot_humid:
                custom_scenarios["🌡️ Hot & humid"] = hot_humid

        results = []
        for name, modifications in custom_scenarios.items():
            scenario_data = baseline.copy()
            for k, v in modifications.items():
                if k in scenario_data and v is not None:
                    scenario_data[k] = v
            X_scenario = pd.DataFrame([scenario_data])
            prediction = self.model.predict(X_scenario)[0]
            results.append(ScenarioPrediction(
                name=name,
                predicted_energy=prediction,
                conditions=modifications,
            ))

        return results

    def get_actual_vs_predicted(self) -> pd.DataFrame:
        """Return a DataFrame with actual, predicted, and dates for plotting."""
        if self.y_test is None or self.y_pred is None:
            raise ValueError("Model not fitted yet. Call fit() first.")
        return pd.DataFrame({
            "timestamp": self.dates_test.values,
            "actual": self.y_test.values,
            "predicted": self.y_pred,
            "residual": self.y_test.values - self.y_pred,
        })
