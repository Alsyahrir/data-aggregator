"""
Part 1: Schema Definition
"""

from dataclasses import dataclass
from typing import Dict, List
from enum import Enum


class AggregationMethod(Enum):
    SUM = "sum"
    MEAN = "mean"
    COUNT = "count"


@dataclass
class SchemaField:
    name: str
    required: bool
    aggregation: AggregationMethod
    keywords: List[str]
    description: str
    unit: str = ""


SCHEMA: Dict[str, SchemaField] = {
    "timestamp": SchemaField(
        name="timestamp",
        required=True,
        aggregation=AggregationMethod.MEAN,
        keywords=["time", "date", "datetime", "timestamp", "measured", "measured_on"],
        description="Date and time of measurement",
        unit="datetime"
    ),
    "source_id": SchemaField(
        name="source_id",
        required=True,
        aggregation=AggregationMethod.MEAN,
        keywords=["id", "source", "plant", "site", "inverter", "device"],
        description="Unique identifier for the data source",
        unit=""
    ),
    "energy": SchemaField(
        name="energy",
        required=True,
        aggregation=AggregationMethod.SUM,
        keywords=["energy", "kwh", "kw_h", "ac_output", "output_kwh", "generation", "value"],
        description="Energy generated in the time period",
        unit="kWh"
    ),
    "ambient_temp": SchemaField(
        name="ambient_temp",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["ambient", "ambient_temp", "ambient_temperature", "air_temp"],
        description="Ambient air temperature",
        unit="C"
    ),
    "irradiance": SchemaField(
        name="irradiance",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["irradiance", "ghi", "global_horizontal", "solar_radiation"],
        description="Solar irradiance (GHI)",
        unit="W/m2"
    ),
    "wind_speed": SchemaField(
        name="wind_speed",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["wind", "wind_speed", "windspeed"],
        description="Wind speed",
        unit="m/s"
    ),
    "module_temp": SchemaField(
        name="module_temp",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["module_temp", "panel_temp", "cell_temp", "pv_temp"],
        description="PV module temperature",
        unit="C"
    ),
    "humidity": SchemaField(
        name="humidity",
        required=False,
        aggregation=AggregationMethod.MEAN,
        keywords=["humidity", "relative_humidity", "rh"],
        description="Relative humidity",
        unit="%"
    ),
}


def get_aggregation_rules() -> Dict[str, str]:
    return {
        name: field.aggregation.value
        for name, field in SCHEMA.items()
        if name not in ["timestamp", "source_id"]
    }


def get_required_fields() -> List[str]:
    return [name for name, field in SCHEMA.items() if field.required]


def get_optional_fields() -> List[str]:
    return [name for name, field in SCHEMA.items() if not field.required]


def print_schema():
    print("=" * 70)
    print("SOLAR DATA AGGREGATOR - STANDARD SCHEMA")
    print("=" * 70)
    print("\nREQUIRED FIELDS:")
    print("-" * 40)
    for name, field in SCHEMA.items():
        if field.required:
            unit_str = f" ({field.unit})" if field.unit else ""
            print(f"  {name}{unit_str}")
            print(f"    Aggregation: {field.aggregation.value.upper()}")
            print(f"    Description: {field.description}\n")
    print("OPTIONAL FIELDS:")
    print("-" * 40)
    for name, field in SCHEMA.items():
        if not field.required:
            unit_str = f" ({field.unit})" if field.unit else ""
            print(f"  {name}{unit_str}")
            print(f"    Aggregation: {field.aggregation.value.upper()}")
            print(f"    Description: {field.description}\n")
    print("=" * 70)
    print("KEY: Energy = SUM, Temperature/Irradiance = MEAN")
    print("=" * 70)
