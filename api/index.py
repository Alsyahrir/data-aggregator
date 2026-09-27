"""
Solstice — Vercel Serverless API Backend
Built with FastAPI, interfacing with the solstice core engine.
"""

import sys
import os
import io
import json
import tempfile
import traceback
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

# Ensure root directory is in sys.path so solstice can be imported
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from solstice import Solstice, LLMAnalyzer
from solstice.schema import SCHEMA
from solstice.detection import auto_detect_columns
from solstice.processing import (
    load_file,
    standardise_dataframe,
    merge_with_environment,
    align_timestamps,
    aggregate_to_period,
    detect_anomalies,
    auto_clean,
    validate_dataframe,
)
from solstice.weather import enrich_with_weather
from solstice.forecasting import SolarForecaster

app = FastAPI(
    title="Solstice API",
    description="Native Vercel backend for solar data processing, schema mapping, weather enrichment, and forecasting.",
    version="2.0.0",
)

# Enable CORS for Next.js frontend (local dev & production)
# The frontend is served from the same origin as this function, so CORS is
# only needed for local dev. allow_origins=["*"] together with
# allow_credentials=True is rejected by browsers outright, so credentials are
# off and the wildcard is kept for the dev server.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

# Trimmed hyperparameter grid for the serverless forecast path — see the
# note in forecast_endpoint(). The library's richer default still applies
# when SolarForecaster is used directly.
SERVERLESS_PARAM_GRID = {
    "n_estimators": [100],
    "max_depth": [10, 20],
    "min_samples_leaf": [1],
}


def friendly_read_error(exc: Exception) -> str:
    """Turn a pandas parse failure into something a user can act on.

    Raw messages like "'utf-8' codec can't decode byte 0x89 in position 0"
    tell the user nothing about what to do next.
    """
    msg = str(exc)
    if isinstance(exc, UnicodeDecodeError) or "codec can't decode" in msg:
        return (
            "The file isn't valid UTF-8 text. If it's an Excel workbook, "
            "save it as .xlsx; if it's a CSV, re-save it with UTF-8 encoding."
        )
    if "No columns to parse" in msg:
        return "The file is empty."
    if "Error tokenizing data" in msg:
        return (
            "The rows don't have a consistent number of columns — check for "
            "stray commas or a malformed header row."
        )
    return msg


def friendly_llm_error(exc: Exception) -> str:
    """Explain why LLM-assisted detection didn't run."""
    msg = str(exc)
    low = msg.lower()
    if "401" in msg or "invalid_api_key" in low or "authentication" in low:
        return "LLM detection skipped: the API key was rejected."
    if "429" in msg or "rate limit" in low:
        return "LLM detection skipped: rate limit reached. Try again shortly."
    if "pip install" in low or isinstance(exc, ImportError):
        return "LLM detection skipped: the provider package isn't installed."
    if "all models failed" in low:
        return f"LLM detection skipped: no model responded. ({msg[:120]})"
    return f"LLM detection skipped: {msg[:160]}"


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


# Vercel caps a serverless function's response body at 4.5MB and rejects
# anything larger with FUNCTION_RESPONSE_PAYLOAD_TOO_LARGE — which is a
# platform error page, not our JSON, so the client can't show a useful
# message. Budget below the cap and leave room for the rest of the payload.
MAX_RESPONSE_RECORD_BYTES = 3_500_000

# Never serialise more than this many rows just to measure them.
_SIZE_SAMPLE_ROWS = 200


def records_within_budget(
    df: pd.DataFrame,
    budget_bytes: int = MAX_RESPONSE_RECORD_BYTES,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Serialise as many rows as fit in the response budget.

    Returns (records, truncated). Row size is estimated from a sample rather
    than by serialising everything twice, so a 100k-row frame doesn't cost a
    full render just to find out it's too big.
    """
    if df.empty:
        return [], False

    sample_n = min(len(df), _SIZE_SAMPLE_ROWS)
    sample = df_to_records(df.head(sample_n))
    sample_bytes = len(json.dumps(sample, default=str).encode("utf-8"))
    bytes_per_row = max(sample_bytes / sample_n, 1)

    max_rows = int(budget_bytes // bytes_per_row)
    if max_rows >= len(df):
        return df_to_records(df), False

    return df_to_records(df, max_rows=max(max_rows, 1)), True


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "service": "Solstice API",
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
    llm_api_key: Optional[str] = Form(None),
):
    """
    Accepts uploaded files, parses headers and sample rows, and performs
    automated column-to-schema detection with optional Groq LLM assistance.
    """
    # The library is no longer Groq-only, so the field is now llm_api_key;
    # groq_api_key stays accepted so an older cached frontend keeps working.
    llm_key = llm_api_key or groq_api_key

    results = []
    skipped = []

    for uf in files:
        contents = await uf.read()
        filename = uf.filename or "uploaded_file.csv"
        ext = os.path.splitext(filename)[1].lower()

        if ext not in SUPPORTED_EXTENSIONS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"'{filename}' is not a supported file type. "
                    f"Upload one of: {', '.join(sorted(SUPPORTED_EXTENSIONS))}."
                ),
            )

        try:
            if ext in [".xlsx", ".xls"]:
                df = pd.read_excel(io.BytesIO(contents))
            else:
                df = pd.read_csv(io.BytesIO(contents))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Could not read '{filename}': {friendly_read_error(e)}",
            )

        if df.empty:
            # Previously this silently vanished from the results and the UI
            # just showed nothing, with no hint as to which file was dropped.
            skipped.append({
                "filename": filename,
                "reason": "File has column headers but no data rows.",
            })
            continue

        sample_df = df.head(5)
        mapping, file_type = auto_detect_columns(sample_df)

        mapped_targets = set(mapping.values())
        has_timestamp = "timestamp" in mapped_targets
        has_data = bool(mapped_targets & {"energy", "ambient_temp", "irradiance", "wind_speed", "module_temp", "humidity"})
        sufficient = has_timestamp and has_data and file_type != "unknown"

        detection_method = "keyword"
        confidence = "high" if sufficient else "medium"

        # If confidence is low and an LLM key is provided, attempt LLM analysis
        llm_error = None
        if not sufficient and llm_key and llm_key.strip():
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                    tmp.write(contents)
                    tmp_path = tmp.name

                analyzer = LLMAnalyzer(api_key=llm_key.strip(), verbose=False)
                analyzer.add_file(tmp_path)
                llm_res = analyzer.analyze()
                if llm_res.files:
                    fa = llm_res.files[0]
                    mapping = fa.column_mapping
                    file_type = fa.file_type
                    detection_method = "llm"
                    confidence = fa.confidence
            except Exception as e:
                # Keyword detection still stands, but say why the LLM pass
                # didn't help — a silent no-op here looks like a broken key.
                llm_error = friendly_llm_error(e)
            finally:
                # Must be in finally: on failure the old code never reached
                # the unlink, leaking into the function's /tmp across warm
                # invocations.
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

        results.append({
            "filename": filename,
            "row_count": len(df),
            "columns": list(df.columns),
            "file_type": file_type,
            "detection_method": detection_method,
            "confidence": confidence,
            "mapping": mapping,
            "llm_error": llm_error,
            "sample_rows": df_to_records(sample_df),
        })

    return {"files": results, "skipped": skipped}


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
        agg = Solstice(verbose=False)

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

        records, truncated = records_within_budget(aggregated_df)

        summary = {
            "total_rows": len(aggregated_df),
            "returned_rows": len(records),
            "truncated": truncated,
            "sources": sources,
            "date_range": {"start": date_min, "end": date_max},
            "total_energy": total_energy,
            "avg_irradiance": avg_irradiance,
            "avg_temp": avg_temp,
            "anomaly_count": anomaly_count,
            "freq": freq,
        }

        # Totals above are computed over the full frame, so the headline
        # figures stay correct even when the row list is trimmed to fit.
        return {
            "summary": summary,
            "columns": list(aggregated_df.columns),
            "records": records,
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

        records, truncated = records_within_budget(enriched_df)

        return {
            "weather_columns": weather_cols,
            "records": records,
            "summary": {
                "latitude": latitude,
                "longitude": longitude,
                "enriched_rows": len(enriched_df),
                "returned_rows": len(records),
                "truncated": truncated,
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
        # A serverless invocation is wall-clock bound, so the library's
        # default 8-combination grid (x3 CV folds) is too slow here — it ran
        # ~11s on 730 daily rows, past Vercel's default 10s function limit.
        # This grid keeps the same shape with a fraction of the fits.
        forecaster.fit(df, param_grid=SERVERLESS_PARAM_GRID)
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
            df.to_excel(writer, index=False, sheet_name="Solstice_Aggregated")
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="solstice_aggregated.xlsx"'},
        )
    else:
        csv_str = df.to_csv(index=False)
        return Response(
            content=csv_str,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="solstice_aggregated.csv"'},
        )
