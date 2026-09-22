"""
Solar Data Aggregator — Vercel Serverless API Backend
Built with FastAPI, interfacing with the solar_aggregator core engine.
"""

import sys
import os
import io
import json
import tempfile
import traceback
from typing import Dict, List, Optional, Any
import numpy as np
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

# Ensure root directory is in sys.path so solar_aggregator can be imported
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from solar_aggregator import SolarAggregator, LLMAnalyzer
from solar_aggregator.schema import SCHEMA
from solar_aggregator.detection import auto_detect_columns
from solar_aggregator.processing import (
    load_file,
    standardise_dataframe,
    merge_with_environment,
    align_timestamps,
    aggregate_to_period,
    detect_anomalies,
    auto_clean,
    validate_dataframe,
)
from solar_aggregator.weather import enrich_with_weather
from solar_aggregator.forecasting import SolarForecaster

app = FastAPI(
    title="Solar Data Aggregator API",
    description="Native Vercel backend for solar data processing, schema mapping, weather enrichment, and forecasting.",
    version="2.0.0",
)

# Enable CORS for Next.js frontend (local dev & production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def sanitize_val(val: Any) -> Any:
    """Helper to convert numpy/pandas types to JSON-safe primitives."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, (np.floating, float)):
        if np.isneginf(val) or np.isposinf(val) or np.isnan(val):
            return None
        return round(float(val), 4)
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (pd.Timestamp, np.datetime64)):
        return pd.to_datetime(val).isoformat()
    return str(val)


def df_to_records(df: pd.DataFrame, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    """Convert dataframe to JSON-safe list of dictionaries."""
    target_df = df.head(max_rows) if max_rows else df
    records = []
    for row in target_df.to_dict(orient="records"):
        cleaned_row = {}
        for k, v in row.items():
            cleaned_row[str(k)] = sanitize_val(v)
        records.append(cleaned_row)
    return records


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "service": "Solar Data Aggregator API",
        "version": "2.0.0",
        "python_version": sys.version,
    }


@app.get("/api/schema")
def get_schema():
    """Return standard schema fields specification."""
    fields = []
    for name, field in SCHEMA.items():
        fields.append({
            "name": name,
            "required": field.required,
            "aggregation": field.aggregation.value,
            "unit": field.unit or "",
            "description": field.description or "",
        })
    return {"fields": fields}


@app.post("/api/detect")
async def detect_columns_endpoint(
    files: List[UploadFile] = File(...),
    groq_api_key: Optional[str] = Form(None),
):
    """
    Accepts uploaded files, parses headers and sample rows, and performs
    automated column-to-schema detection with optional Groq LLM assistance.
    """
    results = []

    for uf in files:
        contents = await uf.read()
        filename = uf.filename or "uploaded_file.csv"
        ext = os.path.splitext(filename)[1].lower()

        try:
            if ext in [".xlsx", ".xls"]:
                df = pd.read_excel(io.BytesIO(contents))
            else:
                df = pd.read_csv(io.BytesIO(contents))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Could not read '{filename}': {str(e)}"
            )

        if df.empty:
            continue

        sample_df = df.head(5)
        mapping, file_type = auto_detect_columns(sample_df)

        mapped_targets = set(mapping.values())
        has_timestamp = "timestamp" in mapped_targets
        has_data = bool(mapped_targets & {"energy", "ambient_temp", "irradiance", "wind_speed", "module_temp", "humidity"})
        sufficient = has_timestamp and has_data and file_type != "unknown"

        detection_method = "keyword"
        confidence = "high" if sufficient else "medium"

        # If confidence is low and Groq key is provided, attempt LLM analysis
        if not sufficient and groq_api_key and groq_api_key.strip():
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                    tmp.write(contents)
                    tmp_path = tmp.name
                
                analyzer = LLMAnalyzer(api_key=groq_api_key.strip(), verbose=False)
                analyzer.add_file(tmp_path)
                llm_res = analyzer.analyze()
                if llm_res.files:
                    fa = llm_res.files[0]
                    mapping = fa.column_mapping
                    file_type = fa.file_type
                    detection_method = "llm"
                    confidence = fa.confidence
                
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception as e:
                # Fallback to keyword detection gracefully
                pass

        results.append({
            "filename": filename,
            "row_count": len(df),
            "columns": list(df.columns),
            "file_type": file_type,
            "detection_method": detection_method,
            "confidence": confidence,
            "mapping": mapping,
            "sample_rows": df_to_records(sample_df),
        })

    return {"files": results}


@app.post("/api/process")
async def process_data_endpoint(
    files: List[UploadFile] = File(...),
    mappings_json: str = Form(...),
    freq: str = Form("1D"),
    anomaly_enabled: bool = Form(True),
    anomaly_method: str = Form("iqr"),
    anomaly_strategy: str = Form("flag"),
):
    """
    Takes data files and user-confirmed column mappings, performs standardisation,
    merging across inverters and environment sources, timestamp alignment,
    aggregation, and anomaly detection.
    """
    try:
        mappings_dict = json.loads(mappings_json)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid mappings JSON: {str(e)}")

    temp_files = []

    try:
        agg = SolarAggregator(verbose=False)

        for uf in files:
            contents = await uf.read()
            filename = uf.filename or "file.csv"
            ext = os.path.splitext(filename)[1].lower()

            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp.write(contents)
                tmp_path = tmp.name
                temp_files.append(tmp_path)

            file_mapping = mappings_dict.get(filename, {})
            source_id = filename.replace(".csv", "").replace(".xlsx", "").replace(".xls", "").replace("_data", "").upper()
            agg.add_file(filepath=tmp_path, source_id=source_id, mapping=file_mapping)

        # Run aggregation
        aggregated_df = agg.aggregate(freq=freq)

        # Anomaly detection & handling
        anomaly_count = 0
        if anomaly_enabled:
            pre_len = len(aggregated_df)
            aggregated_df = auto_clean(aggregated_df, strategy=anomaly_strategy, method=anomaly_method)
            if "is_anomaly" in aggregated_df.columns:
                anomaly_count = int(aggregated_df["is_anomaly"].sum())
            elif anomaly_strategy == "drop":
                anomaly_count = pre_len - len(aggregated_df)

        # Summary calculations
        total_energy = None
        if "energy" in aggregated_df.columns:
            total_energy = sanitize_val(aggregated_df["energy"].sum())

        avg_irradiance = None
        if "irradiance" in aggregated_df.columns:
            avg_irradiance = sanitize_val(aggregated_df["irradiance"].mean())

        avg_temp = None
        if "ambient_temp" in aggregated_df.columns:
            avg_temp = sanitize_val(aggregated_df["ambient_temp"].mean())
        elif "module_temp" in aggregated_df.columns:
            avg_temp = sanitize_val(aggregated_df["module_temp"].mean())

        date_min = None
        date_max = None
        if "timestamp" in aggregated_df.columns:
            date_min = sanitize_val(aggregated_df["timestamp"].min())
            date_max = sanitize_val(aggregated_df["timestamp"].max())

        sources = list(aggregated_df["source_id"].unique()) if "source_id" in aggregated_df.columns else []

        summary = {
            "total_rows": len(aggregated_df),
            "sources": sources,
            "date_range": {"start": date_min, "end": date_max},
            "total_energy": total_energy,
            "avg_irradiance": avg_irradiance,
            "avg_temp": avg_temp,
            "anomaly_count": anomaly_count,
            "freq": freq,
        }

        return {
            "summary": summary,
            "columns": list(aggregated_df.columns),
            "records": df_to_records(aggregated_df),
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Aggregation error: {str(e)}")
    finally:
        for p in temp_files:
            if os.path.exists(p):
                try:
                    os.unlink(p)
                except Exception:
                    pass


@app.post("/api/enrich-weather")
async def enrich_weather_endpoint(
    payload: Dict[str, Any],
):
    """
    Fetches real-world Open-Meteo weather parameters for specified coordinates
    and merges them with the existing solar aggregation records.
    """
    records = payload.get("records", [])
    latitude = float(payload.get("latitude", 1.3521))
    longitude = float(payload.get("longitude", 103.8198))

    if not records:
        raise HTTPException(status_code=400, detail="No records provided to enrich.")

    df = pd.DataFrame(records)
    if "timestamp" not in df.columns:
        raise HTTPException(status_code=400, detail="Records must contain a 'timestamp' field.")

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    try:
        enriched_df = enrich_with_weather(df, latitude=latitude, longitude=longitude)
        
        weather_cols = [
            c for c in enriched_df.columns
            if c not in df.columns and c != "date"
        ]

        return {
            "weather_columns": weather_cols,
            "records": df_to_records(enriched_df),
            "summary": {
                "latitude": latitude,
                "longitude": longitude,
                "enriched_rows": len(enriched_df),
            }
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Weather enrichment failed: {str(e)}")


@app.post("/api/forecast")
async def forecast_endpoint(
    payload: Dict[str, Any],
):
    """
    Fits the Random Forest SolarForecaster on the provided dataset,
    returning evaluation metrics (R², MAE, RMSE), feature importance,
    and weather scenario forecasts.
    """
    records = payload.get("records", [])
    if not records:
        raise HTTPException(status_code=400, detail="No records provided for forecasting.")

    df = pd.DataFrame(records)
    if "timestamp" not in df.columns or "energy" not in df.columns:
        raise HTTPException(status_code=400, detail="Records must contain both 'timestamp' and 'energy'.")

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    try:
        forecaster = SolarForecaster()
        forecaster.fit(df)
        metrics = forecaster.get_metrics()
        scenarios = forecaster.predict_scenarios()

        # Build feature importances list
        feature_importance = []
        if forecaster.model is not None and hasattr(forecaster.model, "feature_importances_"):
            importances = forecaster.model.feature_importances_
            for feat, imp in sorted(zip(forecaster.feature_cols, importances), key=lambda x: x[1], reverse=True):
                feature_importance.append({
                    "feature": feat,
                    "importance": round(float(imp), 4),
                })

        # Format scenario predictions
        formatted_scenarios = []
        for s in scenarios:
            formatted_scenarios.append({
                "name": s.name,
                "predicted_energy": round(float(s.predicted_energy), 2),
                "conditions": {k: sanitize_val(v) for k, v in s.conditions.items()},
            })

        # Test set actual vs predicted points for chart
        test_curve = []
        if forecaster.dates_test is not None and forecaster.y_test is not None and forecaster.y_pred is not None:
            for d, actual, pred in zip(forecaster.dates_test, forecaster.y_test, forecaster.y_pred):
                test_curve.append({
                    "date": pd.to_datetime(d).strftime("%Y-%m-%d"),
                    "actual": round(float(actual), 2),
                    "predicted": round(float(pred), 2),
                })

        return {
            "metrics": {
                "r2": round(float(metrics.r2), 4),
                "mae": round(float(metrics.mae), 2),
                "rmse": round(float(metrics.rmse), 2),
                "mape": round(float(metrics.mape), 2) if metrics.mape else None,
                "n_train": metrics.n_train,
                "n_test": metrics.n_test,
                "features_used": metrics.features_used,
            },
            "feature_importance": feature_importance,
            "scenarios": formatted_scenarios,
            "test_curve": test_curve,
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Forecasting failed: {str(e)}")


@app.post("/api/export")
async def export_endpoint(
    payload: Dict[str, Any],
):
    """
    Exports the provided records into CSV or Excel format for user download.
    """
    records = payload.get("records", [])
    export_format = payload.get("format", "csv").lower()

    if not records:
        raise HTTPException(status_code=400, detail="No records to export.")

    df = pd.DataFrame(records)

    if export_format in ["xlsx", "excel"]:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Solar_Aggregated")
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="solar_aggregated.xlsx"'},
        )
    else:
        csv_str = df.to_csv(index=False)
        return Response(
            content=csv_str,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="solar_aggregated.csv"'},
        )
