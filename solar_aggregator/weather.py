"""
Weather Data Enrichment via Open-Meteo (free, no API key required).

Fetches historical weather data for a location and merges it with
aggregated solar data to produce ML-ready features for forecasting.

Usage:
    from solar_aggregator.weather import enrich_with_weather

    df_enriched = enrich_with_weather(
        df,                         # aggregated solar DataFrame
        latitude=1.3521,            # Singapore
        longitude=103.8198,
    )
"""

import logging
import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from urllib.request import urlopen
from urllib.parse import urlencode
import json

from .schema import WeatherEnrichmentError

logger = logging.getLogger(__name__)

# Open-Meteo free historical weather API
_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Variables we request from the API
_DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "apparent_temperature_max",
    "precipitation_sum",
    "rain_sum",
    "sunshine_duration",          # seconds → converted to hours
    "shortwave_radiation_sum",   # MJ/m² → can derive clearness index
    "windspeed_10m_max",
    "relative_humidity_2m_mean",
    "cloud_cover_mean",
]


def fetch_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    timezone: str = "auto",
) -> pd.DataFrame:
    """Fetch historical daily weather from Open-Meteo.

    Args:
        latitude: Location latitude.
        longitude: Location longitude.
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.
        timezone: Timezone string or 'auto' (default).

    Returns:
        DataFrame with one row per day and weather columns.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(_DAILY_VARIABLES),
        "timezone": timezone,
    }
    url = f"{_BASE_URL}?{urlencode(params)}"
    logger.info("Fetching weather data: %s to %s (%.4f, %.4f)",
                start_date, end_date, latitude, longitude)

    try:
        with urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        raise WeatherEnrichmentError(
            f"Failed to fetch weather data from Open-Meteo: {e}"
        ) from e

    if "error" in data and data["error"]:
        raise WeatherEnrichmentError(
            f"Open-Meteo API error: {data.get('reason', 'Unknown error')}"
        )

    daily = data.get("daily", {})
    if not daily or "time" not in daily:
        raise WeatherEnrichmentError("No daily data returned from Open-Meteo")

    df = pd.DataFrame(daily)
    df["time"] = pd.to_datetime(df["time"])
    df = df.rename(columns={"time": "date"})

    # Convert sunshine_duration from seconds to hours
    if "sunshine_duration" in df.columns:
        df["sunshine_hours"] = df["sunshine_duration"] / 3600.0
        df = df.drop(columns=["sunshine_duration"])

    # Compute derived features
    if "temperature_2m_max" in df.columns and "temperature_2m_min" in df.columns:
        df["temp_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]

    logger.info("Fetched %d days of weather data", len(df))
    return df


def compute_clearness_index(
    df: pd.DataFrame,
    radiation_col: str = "shortwave_radiation_sum",
    latitude: float = 0.0,
) -> pd.DataFrame:
    """Compute clearness index (Kt = measured / theoretical max radiation).

    A simple solar geometry approximation is used for the theoretical max.
    Clearness index ranges from 0 (overcast) to ~1.0 (perfectly clear).
    """
    df = df.copy()
    if radiation_col not in df.columns:
        logger.warning("Cannot compute clearness index: '%s' not in DataFrame", radiation_col)
        return df

    if "date" in df.columns:
        day_of_year = df["date"].dt.dayofyear
    elif "timestamp" in df.columns:
        day_of_year = df["timestamp"].dt.dayofyear
    else:
        logger.warning("Cannot compute clearness index: no date column found")
        return df

    # Approximate extraterrestrial daily radiation (MJ/m²)
    lat_rad = np.radians(latitude)
    solar_const = 1361  # W/m², solar constant
    days_in_year = 365.25

    # Solar declination (simplified)
    declination = 23.45 * np.sin(np.radians(360 / days_in_year * (day_of_year - 81)))
    decl_rad = np.radians(declination)

    # Sunset hour angle
    cos_omega = -np.tan(lat_rad) * np.tan(decl_rad)
    cos_omega = cos_omega.clip(-1, 1)
    omega_s = np.arccos(cos_omega)

    # Daily extraterrestrial radiation (MJ/m²/day)
    dr = 1 + 0.033 * np.cos(2 * np.pi * day_of_year / days_in_year)
    ra = (24 * 60 / np.pi) * (solar_const / 1e6) * dr * (
        omega_s * np.sin(lat_rad) * np.sin(decl_rad)
        + np.cos(lat_rad) * np.cos(decl_rad) * np.sin(omega_s)
    )

    # Clearness index
    df["clearness_index"] = np.where(
        ra > 0,
        (df[radiation_col] / ra).clip(0, 1.2),
        np.nan
    )

    return df


def enrich_with_weather(
    df: pd.DataFrame,
    latitude: float,
    longitude: float,
    timestamp_col: str = "timestamp",
    timezone: str = "auto",
) -> pd.DataFrame:
    """Enrich an aggregated solar DataFrame with historical weather data.

    Fetches weather for the date range of the DataFrame, computes derived
    features (sunshine_hours, clearness_index, temp_range), and merges
    by date.

    Args:
        df: Aggregated solar DataFrame (must have a datetime column).
        latitude: Site latitude.
        longitude: Site longitude.
        timestamp_col: Name of the datetime column in df.
        timezone: Timezone for the weather API.

    Returns:
        Enriched DataFrame with weather columns joined by date.
    """
    if timestamp_col not in df.columns:
        raise WeatherEnrichmentError(
            f"Column '{timestamp_col}' not found in DataFrame"
        )

    df = df.copy()
    ts = pd.to_datetime(df[timestamp_col])
    start_date = ts.min().strftime("%Y-%m-%d")
    end_date = ts.max().strftime("%Y-%m-%d")

    weather_df = fetch_weather(latitude, longitude, start_date, end_date, timezone)
    weather_df = compute_clearness_index(weather_df, latitude=latitude)

    # Prepare merge key
    df["_merge_date"] = ts.dt.normalize()
    weather_df["_merge_date"] = weather_df["date"].dt.normalize()

    # Avoid overwriting existing columns
    existing_cols = set(df.columns)
    weather_cols = [c for c in weather_df.columns if c not in existing_cols and c != "date"]
    merge_df = weather_df[["_merge_date"] + weather_cols]

    # Merge
    df = df.merge(merge_df, on="_merge_date", how="left")
    df = df.drop(columns=["_merge_date"])

    matched = df[weather_cols[0]].notna().sum() if weather_cols else 0
    logger.info(
        "Weather enrichment: matched %d / %d rows (%.1f%%)",
        matched, len(df), matched / len(df) * 100 if len(df) > 0 else 0,
    )

    return df


def get_weather_columns() -> List[str]:
    """Return the list of weather columns that enrichment adds."""
    return [
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "apparent_temperature_max",
        "precipitation_sum",
        "rain_sum",
        "sunshine_hours",
        "shortwave_radiation_sum",
        "windspeed_10m_max",
        "relative_humidity_2m_mean",
        "cloud_cover_mean",
        "temp_range",
        "clearness_index",
    ]
