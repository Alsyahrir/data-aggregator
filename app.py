"""
Solstice — Web Interface
Run: streamlit run app.py
"""

import os
import io
import tempfile
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from solstice import Solstice, LLMAnalyzer
from solstice.detection import auto_detect_columns
from solstice.processing import (
    load_file, validate_dataframe, detect_anomalies, auto_clean,
)
from solstice.schema import SCHEMA, print_schema


# ── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Solstice",
    page_icon="☀️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Premium CSS ──────────────────────────────────────────────────────────────

st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&family=Inter:wght@300;400;500;600&display=swap" rel="stylesheet">

<style>
    /* ── GLOBAL ─────────────────────────────────────── */

    * { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important; }
    h1, h2, h3, .hero-title, .step-card h3, .section-header .text h2 {
        font-family: 'DM Sans', 'Inter', sans-serif !important;
    }

    .main .block-container {
        padding-top: 2.5rem;
        padding-bottom: 5rem;
        max-width: 1140px;
    }

    /* Hide Streamlit chrome */
    #MainMenu, footer, header { visibility: hidden; }
    .stDeployButton { display: none; }

    /* ── ANIMATED HERO HEADER ──────────────────────── */

    .hero-container {
        position: relative;
        padding: 3rem 0 2rem 0;
        margin-bottom: 2rem;
        overflow: hidden;
    }

    .hero-glow {
        position: absolute;
        top: -50px;
        left: -50px;
        width: 240px;
        height: 240px;
        background: radial-gradient(circle, rgba(232, 160, 81, 0.12) 0%, rgba(232, 160, 81, 0.04) 40%, transparent 70%);
        border-radius: 50%;
        animation: breathe 8s ease-in-out infinite;
        pointer-events: none;
    }

    .hero-glow-2 {
        position: absolute;
        top: -20px;
        right: -30px;
        width: 180px;
        height: 180px;
        background: radial-gradient(circle, rgba(211, 120, 100, 0.08) 0%, transparent 70%);
        border-radius: 50%;
        animation: breathe 10s ease-in-out infinite 2s;
        pointer-events: none;
    }

    @keyframes breathe {
        0%, 100% { transform: translate(0, 0) scale(1); opacity: 0.7; }
        50% { transform: translate(10px, 8px) scale(1.06); opacity: 1; }
    }

    @keyframes gentle-drift {
        0%, 100% { background-position: 0% center; }
        50% { background-position: 100% center; }
    }

    .hero-title {
        font-size: 2.6rem;
        font-weight: 700;
        letter-spacing: -0.03em;
        line-height: 1.15;
        background: linear-gradient(135deg, #e8a051 0%, #d4785c 40%, #e8a051 70%, #f0c878 100%);
        background-size: 200% auto;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        animation: gentle-drift 8s ease-in-out infinite;
        margin-bottom: 0.6rem;
    }

    .hero-subtitle {
        font-size: 1.05rem;
        font-weight: 400;
        color: rgba(240, 237, 232, 0.55);
        letter-spacing: 0.005em;
        line-height: 1.7;
        max-width: 560px;
    }

    .hero-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 16px;
        border-radius: 24px;
        font-size: 0.73rem;
        font-weight: 600;
        letter-spacing: 0.02em;
        background: rgba(232, 160, 81, 0.08);
        border: 1px solid rgba(232, 160, 81, 0.18);
        color: #e8a051;
        margin-bottom: 1rem;
    }

    /* ── GLASS CARDS ───────────────────────────────── */

    .glass-card {
        background: rgba(255, 255, 255, 0.025);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid rgba(240, 237, 232, 0.05);
        border-radius: 22px;
        padding: 1.6rem;
        margin-bottom: 1rem;
        transition: all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
    }

    .glass-card:hover {
        border-color: rgba(232, 160, 81, 0.15);
        box-shadow: 0 8px 40px rgba(232, 160, 81, 0.06);
        transform: scale(1.015);
    }

    /* ── METRIC CARDS ──────────────────────────────── */

    .metric-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 1.1rem;
        margin: 1.4rem 0;
    }

    .metric-card {
        background: linear-gradient(155deg, rgba(255,255,255,0.035) 0%, rgba(255,255,255,0.008) 100%);
        border: 1px solid rgba(240, 237, 232, 0.05);
        border-radius: 22px;
        padding: 1.3rem 1.4rem;
        text-align: left;
        position: relative;
        overflow: hidden;
        transition: all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
    }

    .metric-card:hover {
        border-color: rgba(232, 160, 81, 0.2);
        transform: scale(1.015);
        box-shadow: 0 6px 28px rgba(232, 160, 81, 0.06);
    }

    .metric-card::after {
        content: '';
        position: absolute;
        top: -10px;
        right: -10px;
        width: 90px;
        height: 90px;
        border-radius: 50%;
        opacity: 0.04;
    }

    .metric-card.amber::after { background: #e8a051; }
    .metric-card.sky::after { background: #6ba5d7; }
    .metric-card.sage::after { background: #7db87f; }
    .metric-card.plum::after { background: #b07cc3; }

    .metric-icon {
        font-size: 1.3rem;
        margin-bottom: 0.6rem;
        display: block;
    }

    .metric-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: #f0ede8;
        letter-spacing: -0.02em;
        line-height: 1.2;
        font-family: 'DM Sans', 'Inter', sans-serif !important;
    }

    .metric-value.amber { color: #e8a051; }
    .metric-value.sky { color: #6ba5d7; }
    .metric-value.sage { color: #7db87f; }
    .metric-value.plum { color: #b07cc3; }

    .metric-label {
        font-size: 0.8rem;
        font-weight: 400;
        color: rgba(240, 237, 232, 0.4);
        letter-spacing: 0.01em;
        margin-top: 0.35rem;
    }

    /* ── SECTION HEADERS ───────────────────────────── */

    .section-header {
        display: flex;
        align-items: center;
        gap: 0.8rem;
        margin: 2.5rem 0 1.2rem 0;
    }

    .section-header .icon {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 42px;
        height: 42px;
        border-radius: 14px;
        font-size: 1.2rem;
    }

    .section-header .icon.amber { background: rgba(232, 160, 81, 0.1); }
    .section-header .icon.sky { background: rgba(107, 165, 215, 0.1); }
    .section-header .icon.sage { background: rgba(125, 184, 127, 0.1); }
    .section-header .icon.plum { background: rgba(176, 124, 195, 0.1); }
    .section-header .icon.rose { background: rgba(211, 120, 130, 0.1); }

    .section-header .text h2 {
        font-size: 1.35rem;
        font-weight: 600;
        color: #f0ede8;
        margin: 0;
        padding: 0;
        letter-spacing: -0.01em;
    }

    .section-header .text p {
        font-size: 0.84rem;
        color: rgba(240, 237, 232, 0.4);
        margin: 3px 0 0 0;
        padding: 0;
        line-height: 1.4;
    }

    .section-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(232, 160, 81, 0.06) 15%, rgba(240, 237, 232, 0.04) 50%, rgba(232, 160, 81, 0.06) 85%, transparent);
        margin: 2.5rem 0;
        border: none;
    }

    /* ── STEP CARDS (EMPTY STATE) ──────────────────── */

    .steps-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 1.3rem;
        margin: 2rem 0;
    }

    .step-card {
        background: linear-gradient(155deg, rgba(255,255,255,0.035) 0%, rgba(255,255,255,0.008) 100%);
        border: 1px solid rgba(240, 237, 232, 0.05);
        border-radius: 24px;
        padding: 2.2rem 1.6rem;
        text-align: center;
        position: relative;
        overflow: hidden;
        transition: all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
    }

    .step-card:hover {
        transform: scale(1.02);
        border-color: rgba(232, 160, 81, 0.15);
        box-shadow: 0 12px 48px rgba(232, 160, 81, 0.06);
    }

    .step-number {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 50px;
        height: 50px;
        border-radius: 50%;
        font-size: 1.2rem;
        font-weight: 600;
        margin-bottom: 1.1rem;
        font-family: 'DM Sans', sans-serif !important;
    }

    .step-number.s1 { background: rgba(232, 160, 81, 0.1); color: #e8a051; }
    .step-number.s2 { background: rgba(107, 165, 215, 0.1); color: #6ba5d7; }
    .step-number.s3 { background: rgba(125, 184, 127, 0.1); color: #7db87f; }

    .step-card h3 {
        font-size: 1.08rem;
        font-weight: 600;
        color: #f0ede8;
        margin: 0 0 0.5rem 0;
    }

    .step-card p {
        font-size: 0.88rem;
        color: rgba(240, 237, 232, 0.45);
        line-height: 1.6;
        margin: 0;
    }

    .step-connector {
        display: none;
    }

    /* ── FILE DETECTION CARDS ──────────────────────── */

    .file-header {
        display: flex;
        align-items: center;
        gap: 0.8rem;
        margin-bottom: 0.8rem;
    }

    .file-icon {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 42px;
        height: 42px;
        border-radius: 14px;
        font-size: 1.1rem;
        flex-shrink: 0;
    }

    .file-icon.inverter { background: rgba(125, 184, 127, 0.1); }
    .file-icon.environment { background: rgba(107, 165, 215, 0.1); }
    .file-icon.irradiance { background: rgba(232, 160, 81, 0.1); }
    .file-icon.unknown { background: rgba(160, 158, 153, 0.1); }

    .file-name {
        font-size: 1rem;
        font-weight: 600;
        color: #f0ede8;
    }

    .file-meta {
        font-size: 0.78rem;
        color: rgba(240, 237, 232, 0.35);
    }

    .type-pill {
        display: inline-flex;
        align-items: center;
        padding: 4px 14px;
        border-radius: 24px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.02em;
    }

    .type-pill.inverter { background: rgba(125, 184, 127, 0.1); color: #7db87f; border: 1px solid rgba(125, 184, 127, 0.2); }
    .type-pill.environment { background: rgba(107, 165, 215, 0.1); color: #6ba5d7; border: 1px solid rgba(107, 165, 215, 0.2); }
    .type-pill.irradiance { background: rgba(232, 160, 81, 0.1); color: #e8a051; border: 1px solid rgba(232, 160, 81, 0.2); }
    .type-pill.unknown { background: rgba(160, 158, 153, 0.1); color: #a09e99; border: 1px solid rgba(160, 158, 153, 0.2); }

    .detection-badge {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 3px 12px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 500;
    }

    .detection-badge.keyword {
        background: rgba(125, 184, 127, 0.08);
        color: #7db87f;
    }

    .detection-badge.llm {
        background: rgba(232, 160, 81, 0.08);
        color: #e8a051;
    }

    /* ── BUTTONS ────────────────────────────────────── */

    .stButton > button {
        border-radius: 24px;
        font-weight: 600;
        letter-spacing: 0.005em;
        transition: all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
        border: 1px solid rgba(240, 237, 232, 0.08);
    }

    .stButton > button:hover {
        transform: scale(1.02);
        box-shadow: 0 6px 24px rgba(232, 160, 81, 0.12);
    }

    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #e8a051, #d4785c) !important;
        border: none !important;
        color: white !important;
    }

    .stDownloadButton > button {
        width: 100%;
        border-radius: 20px;
        font-weight: 500;
        transition: all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
    }

    .stDownloadButton > button:hover {
        transform: scale(1.015);
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
    }

    /* ── TABS ───────────────────────────────────────── */

    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: rgba(255,255,255,0.02);
        border-radius: 20px;
        padding: 5px;
        border: 1px solid rgba(240, 237, 232, 0.04);
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 16px;
        padding: 8px 18px;
        font-size: 0.84rem;
        font-weight: 500;
        color: rgba(240, 237, 232, 0.45);
        transition: all 0.3s ease;
    }

    .stTabs [aria-selected="true"] {
        background: rgba(232, 160, 81, 0.1) !important;
        color: #e8a051 !important;
    }

    /* ── SIDEBAR ────────────────────────────────────── */

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #12151c 0%, #151a24 100%);
        border-right: 1px solid rgba(240, 237, 232, 0.03);
    }

    section[data-testid="stSidebar"] .block-container {
        padding-top: 1.5rem;
    }

    .sidebar-logo {
        display: flex;
        align-items: center;
        gap: 0.7rem;
        padding: 0.6rem 0 1.4rem 0;
        border-bottom: 1px solid rgba(240, 237, 232, 0.04);
        margin-bottom: 1.3rem;
    }

    .sidebar-logo .icon {
        font-size: 1.5rem;
    }

    .sidebar-logo .text {
        font-size: 0.95rem;
        font-weight: 600;
        color: #f0ede8;
        letter-spacing: -0.01em;
        font-family: 'DM Sans', 'Inter', sans-serif !important;
    }

    .sidebar-section-title {
        font-size: 0.68rem;
        font-weight: 600;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: rgba(240, 237, 232, 0.25);
        margin: 1.4rem 0 0.6rem 0;
    }

    /* ── EXPANDERS ──────────────────────────────────── */

    .streamlit-expanderHeader {
        font-weight: 600;
        font-size: 0.95rem;
    }

    details[data-testid="stExpander"] {
        background: rgba(255,255,255,0.015);
        border: 1px solid rgba(240, 237, 232, 0.04);
        border-radius: 18px;
    }

    /* ── FILE UPLOADER ─────────────────────────────── */

    [data-testid="stFileUploader"] > section {
        border: 2px dashed rgba(232, 160, 81, 0.15);
        border-radius: 22px;
        background: rgba(232, 160, 81, 0.015);
        transition: all 0.4s ease;
    }

    [data-testid="stFileUploader"] > section:hover {
        border-color: rgba(232, 160, 81, 0.3);
        background: rgba(232, 160, 81, 0.03);
        box-shadow: 0 4px 24px rgba(232, 160, 81, 0.04);
    }

    /* ── DATAFRAMES ─────────────────────────────────── */

    [data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(240, 237, 232, 0.04);
    }

    /* ── ALERTS / INFO ─────────────────────────────── */

    .anomaly-alert {
        display: flex;
        align-items: center;
        gap: 0.9rem;
        padding: 1.1rem 1.3rem;
        border-radius: 18px;
        background: rgba(232, 160, 81, 0.04);
        border: 1px solid rgba(232, 160, 81, 0.1);
        margin: 1rem 0;
    }

    .anomaly-alert .count {
        font-size: 1.4rem;
        font-weight: 700;
        color: #e8a051;
        font-family: 'DM Sans', sans-serif !important;
    }

    .anomaly-alert .text {
        font-size: 0.88rem;
        color: rgba(240, 237, 232, 0.6);
    }

    /* ── FOOTER ─────────────────────────────────────── */

    .app-footer {
        text-align: center;
        padding: 3rem 0 1.5rem 0;
        color: rgba(240, 237, 232, 0.2);
        font-size: 0.8rem;
        line-height: 1.6;
    }

    .app-footer a {
        color: rgba(232, 160, 81, 0.4);
        text-decoration: none;
    }

    /* ── RESPONSIVE ─────────────────────────────────── */

    @media (max-width: 768px) {
        .metric-grid { grid-template-columns: repeat(2, 1fr); }
        .steps-grid { grid-template-columns: 1fr; }
        .hero-title { font-size: 2rem; }
    }
</style>
""", unsafe_allow_html=True)


# ── Session State Init ───────────────────────────────────────────────────────

if "uploaded_files_data" not in st.session_state:
    st.session_state.uploaded_files_data = {}
if "analysis_done" not in st.session_state:
    st.session_state.analysis_done = False
if "aggregated_df" not in st.session_state:
    st.session_state.aggregated_df = None
if "merged_df" not in st.session_state:
    st.session_state.merged_df = None
if "file_analyses" not in st.session_state:
    st.session_state.file_analyses = []
if "mappings_edited" not in st.session_state:
    st.session_state.mappings_edited = {}
if "weather_enriched_df" not in st.session_state:
    st.session_state.weather_enriched_df = None
if "forecaster" not in st.session_state:
    st.session_state.forecaster = None


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div class="sidebar-logo">
        <span class="icon">☀️</span>
        <span class="text">Solar Aggregator</span>
    </div>
    """, unsafe_allow_html=True)
    
    st.caption("Hey there! Configure your settings below.")
    
    st.markdown('<div class="sidebar-section-title">Aggregation</div>', unsafe_allow_html=True)

    
    freq_options = {
        "Hourly": "1h",
        "Daily": "1D",
        "Weekly": "1W",
        "Monthly": "1M",
    }
    freq_label = st.selectbox(
        "Aggregation Period",
        list(freq_options.keys()),
        index=1,
        help="How to group the data. Daily is most common for solar analysis."
    )
    freq = freq_options[freq_label]
    
    st.markdown('<div class="sidebar-section-title">Anomaly Detection</div>', unsafe_allow_html=True)
    
    anomaly_enabled = st.toggle("Enable anomaly detection", value=True)
    if anomaly_enabled:
        anomaly_method = st.selectbox(
            "Detection method",
            ["iqr", "zscore"],
            format_func=lambda x: "IQR (Interquartile Range)" if x == "iqr" else "Z-Score",
            index=0,
            help="IQR is robust to skewed data. Z-score works well for normal distributions."
        )
        anomaly_strategy = st.selectbox(
            "Handling strategy",
            ["flag", "drop", "clip"],
            format_func=lambda x: {"flag": "🚩 Flag only", "drop": "🗑️ Remove rows", "clip": "📐 Clip to bounds"}[x],
            index=0,
            help="Flag: mark anomalies. Drop: remove them. Clip: cap to bounds."
        )
    
    st.markdown('<div class="sidebar-section-title">Smart Detection</div>', unsafe_allow_html=True)
    
    st.caption("Use an LLM when column names are non-standard.")
    api_key = st.text_input(
        "Groq API Key",
        type="password",
        placeholder="gsk_...",
        help="Free key from https://console.groq.com"
    )
    
    st.markdown('<div class="sidebar-section-title">Schema Reference</div>', unsafe_allow_html=True)
    
    with st.expander("View Standard Fields", expanded=False):
        schema_data = []
        for name, field in SCHEMA.items():
            schema_data.append({
                "Field": name,
                "Req": "✅" if field.required else "",
                "Agg": field.aggregation.value.upper(),
                "Unit": field.unit or "—",
            })
        st.dataframe(pd.DataFrame(schema_data), hide_index=True, use_container_width=True, height=300)


# ── Hero Header ──────────────────────────────────────────────────────────────

st.markdown("""
<div class="hero-container">
    <div class="hero-glow"></div>
    <div class="hero-glow-2"></div>
    <div class="hero-badge">☀️ Solar Data Platform</div>
    <div class="hero-title">Let's make sense<br>of your solar data</div>
    <div class="hero-subtitle">
        Drop in your solar panel and weather files — we'll clean, merge,
        and analyze everything for you. No fuss, just insights.
    </div>
</div>
""", unsafe_allow_html=True)


# ── File Upload ──────────────────────────────────────────────────────────────

st.markdown("""
<div class="section-header">
    <div class="icon amber">📁</div>
    <div class="text">
        <h2>Your files</h2>
        <p>Drop your CSV or Excel files here — we'll figure out what's what</p>
    </div>
</div>
""", unsafe_allow_html=True)

uploaded_files = st.file_uploader(
    "Drag and drop CSV or Excel files",
    type=["csv", "xlsx", "xls"],
    accept_multiple_files=True,
    label_visibility="collapsed",
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def save_uploaded_to_temp(uploaded_file) -> str:
    """Save an uploaded file to a temp path and return the path."""
    suffix = os.path.splitext(uploaded_file.name)[1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getbuffer())
    tmp.close()
    return tmp.name


def detect_file(filepath, filename, df_sample, api_key=None):
    """Run detection on a single file, return analysis info."""
    mapping, file_type = auto_detect_columns(df_sample)
    
    mapped_targets = set(mapping.values())
    has_timestamp = "timestamp" in mapped_targets
    has_data = bool(mapped_targets & {"energy", "ambient_temp", "irradiance", "wind_speed", "module_temp", "humidity"})
    keyword_sufficient = has_timestamp and has_data and file_type != "unknown"
    
    detection_method = "keyword"
    confidence = "high" if keyword_sufficient else "low"
    
    if not keyword_sufficient and api_key:
        try:
            analyzer = LLMAnalyzer(api_key=api_key, verbose=False)
            analyzer.add_file(filepath)
            result = analyzer.analyze()
            if result.files:
                fa = result.files[0]
                return {
                    "filename": filename, "filepath": filepath,
                    "file_type": fa.file_type, "mapping": fa.column_mapping,
                    "detection_method": fa.detection_method, "confidence": fa.confidence,
                    "columns": list(df_sample.columns), "row_count": len(df_sample),
                }
        except Exception as e:
            st.warning(f"LLM detection failed for {filename}: {e}")
    
    return {
        "filename": filename, "filepath": filepath,
        "file_type": file_type, "mapping": mapping,
        "detection_method": detection_method, "confidence": confidence,
        "columns": list(df_sample.columns), "row_count": len(df_sample),
    }


def get_file_icon(file_type):
    """Return emoji icon for file type."""
    return {"inverter": "⚡", "environment": "🌡️", "irradiance": "☀️"}.get(file_type, "📄")


def make_plotly_layout(title="", xaxis_title="", yaxis_title=""):
    """Return a consistent plotly layout dict."""
    return dict(
        title=dict(text=title, font=dict(size=16, family="DM Sans, Inter")),
        xaxis_title=xaxis_title, yaxis_title=yaxis_title,
        template='plotly_dark',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#d4d0c8', family='Inter'),
        hovermode='x unified',
        margin=dict(t=50, b=40, l=50, r=20),
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            font=dict(size=11),
        ),
    )


# ── Process Uploaded Files ───────────────────────────────────────────────────

if uploaded_files:
    # Save and detect
    new_files = {}
    for uf in uploaded_files:
        if uf.name not in st.session_state.uploaded_files_data:
            tmp_path = save_uploaded_to_temp(uf)
            df_sample = load_file(tmp_path)
            analysis = detect_file(tmp_path, uf.name, df_sample, api_key)
            analysis["row_count"] = len(df_sample)
            analysis["df_sample"] = df_sample
            new_files[uf.name] = analysis
    
    st.session_state.uploaded_files_data.update(new_files)
    
    current_names = {uf.name for uf in uploaded_files}
    for name in list(st.session_state.uploaded_files_data.keys()):
        if name not in current_names:
            del st.session_state.uploaded_files_data[name]
    
    # ── Detection Results ────────────────────────────────────────────────
    
    st.markdown("""
    <div class="section-header">
        <div class="icon sky">🔍</div>
        <div class="text">
            <h2>What we found</h2>
            <p>Here's how we mapped your columns — feel free to adjust anything</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    files_data = st.session_state.uploaded_files_data
    
    for fname, fdata in files_data.items():
        ftype = fdata['file_type']
        ftype_class = ftype if ftype in ['inverter', 'environment', 'irradiance'] else 'unknown'
        icon = get_file_icon(ftype)
        
        with st.expander(f"{icon} **{fname}** — {fdata['row_count']:,} rows", expanded=True):
            
            # File header with badges
            st.markdown(f"""
            <div style="display: flex; align-items: center; gap: 0.6rem; margin-bottom: 1rem;">
                <span class="type-pill {ftype_class}">{ftype}</span>
                <span class="detection-badge {fdata['detection_method']}">{fdata['detection_method'].upper()}</span>
                <span style="font-size: 0.78rem; color: rgba(255,255,255,0.35);">
                    Confidence: {fdata['confidence']}
                </span>
            </div>
            """, unsafe_allow_html=True)
            
            col_mapping, col_preview = st.columns([1, 1])
            
            with col_mapping:
                st.markdown("**Column Mappings**")
                schema_targets = ["(skip)"] + list(SCHEMA.keys())
                
                if fname not in st.session_state.mappings_edited:
                    st.session_state.mappings_edited[fname] = dict(fdata['mapping'])
                
                current_mapping = st.session_state.mappings_edited[fname]
                new_mapping = {}
                
                for col in fdata['columns']:
                    current_target = current_mapping.get(col, "(skip)")
                    idx = schema_targets.index(current_target) if current_target in schema_targets else 0
                    selected = st.selectbox(
                        f"`{col}`", schema_targets, index=idx,
                        key=f"map_{fname}_{col}"
                    )
                    if selected != "(skip)":
                        new_mapping[col] = selected
                
                st.session_state.mappings_edited[fname] = new_mapping
            
            with col_preview:
                st.markdown("**Data Preview**")
                st.dataframe(fdata['df_sample'].head(5), use_container_width=True, height=220)
    
    # ── Aggregate Button ─────────────────────────────────────────────────
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    col_l, col_btn, col_r = st.columns([1, 2, 1])
    with col_btn:
        run_clicked = st.button(
            "🚀  Aggregate Data",
            use_container_width=True,
            type="primary",
        )
    
    if run_clicked:
        progress = st.progress(0, text="Loading files...")
        try:
            agg = Solstice(verbose=False)
            
            total_files = len(files_data)
            for i, (fname, fdata) in enumerate(files_data.items()):
                mapping = st.session_state.mappings_edited.get(fname, fdata['mapping'])
                source_id = fname.replace(".csv", "").replace(".xlsx", "").replace("_data", "").upper()
                agg.add_file(filepath=fdata['filepath'], source_id=source_id, mapping=mapping)
                progress.progress(
                    int((i + 1) / total_files * 50),
                    text=f"Processing {fname}..."
                )
            
            progress.progress(60, text="Merging & aligning...")
            aggregated_df = agg.aggregate(freq=freq)
            
            progress.progress(80, text="Running anomaly detection...")
            if anomaly_enabled:
                aggregated_df = auto_clean(aggregated_df, strategy=anomaly_strategy, method=anomaly_method)
            
            merged_df = agg.get_dataframe("merged")
            progress.progress(100, text="✅ Complete!")
            
            st.session_state.aggregated_df = aggregated_df
            st.session_state.merged_df = merged_df
            st.session_state.analysis_done = True
            st.session_state.weather_enriched_df = None
            st.session_state.forecaster = None
            
            st.success(f"Aggregated **{len(aggregated_df):,}** rows across **{aggregated_df['source_id'].nunique() if 'source_id' in aggregated_df.columns else 0}** sources.")
            
        except Exception as e:
            st.error(f"Aggregation failed: {str(e)}")
            st.session_state.analysis_done = False


# ── Results Dashboard ────────────────────────────────────────────────────────

if st.session_state.analysis_done and st.session_state.aggregated_df is not None:
    df = st.session_state.aggregated_df
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="section-header">
        <div class="icon sage">📊</div>
        <div class="text">
            <h2>Here's what your data looks like</h2>
            <p>Interactive charts and insights from your solar data</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # ── Date Range Filter ────────────────────────────────────────────────
    
    if 'timestamp' in df.columns:
        fc1, fc2 = st.columns(2)
        with fc1:
            date_start = st.date_input(
                "From", value=df['timestamp'].min().date(),
                min_value=df['timestamp'].min().date(),
                max_value=df['timestamp'].max().date(),
                key="date_start",
            )
        with fc2:
            date_end = st.date_input(
                "To", value=df['timestamp'].max().date(),
                min_value=df['timestamp'].min().date(),
                max_value=df['timestamp'].max().date(),
                key="date_end",
            )
        mask = (df['timestamp'].dt.date >= date_start) & (df['timestamp'].dt.date <= date_end)
        df_filtered = df[mask].copy()
    else:
        df_filtered = df.copy()
    
    # ── Summary Metrics ──────────────────────────────────────────────────
    
    total_energy = df_filtered['energy'].sum() if 'energy' in df_filtered.columns else 0
    date_range = (df_filtered['timestamp'].max() - df_filtered['timestamp'].min()).days if 'timestamp' in df_filtered.columns else 0
    n_sources = df_filtered['source_id'].nunique() if 'source_id' in df_filtered.columns else 0
    n_rows = len(df_filtered)
    
    st.markdown(f"""
    <div class="metric-grid">
        <div class="metric-card amber">
            <span class="metric-icon">⚡</span>
            <div class="metric-value amber">{total_energy:,.0f}</div>
            <div class="metric-label">Energy harvested (kWh)</div>
        </div>
        <div class="metric-card sky">
            <span class="metric-icon">📅</span>
            <div class="metric-value sky">{date_range}</div>
            <div class="metric-label">Days of data</div>
        </div>
        <div class="metric-card sage">
            <span class="metric-icon">🔌</span>
            <div class="metric-value sage">{n_sources}</div>
            <div class="metric-label">Data sources</div>
        </div>
        <div class="metric-card plum">
            <span class="metric-icon">📋</span>
            <div class="metric-value plum">{n_rows:,}</div>
            <div class="metric-label">Records</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Anomaly alert
    if 'is_anomaly' in df_filtered.columns:
        n_anomalies = int(df_filtered['is_anomaly'].sum())
        if n_anomalies > 0:
            pct = n_anomalies / len(df_filtered) * 100
            st.markdown(f"""
            <div class="anomaly-alert">
                <span class="count">{n_anomalies}</span>
                <span class="text">anomalous rows detected ({pct:.1f}% of data) — review in the Anomalies tab below</span>
            </div>
            """, unsafe_allow_html=True)
    
    # ── Chart Tabs ───────────────────────────────────────────────────────
    
    tab_labels = ["📈 Energy", "📊 Monthly", "🎯 Sources", "🔍 Quality"]
    
    numeric_cols = [c for c in df_filtered.columns if pd.api.types.is_numeric_dtype(df_filtered[c]) and c not in ['observation_count'] and not c.startswith('_anomaly_')]
    if len(numeric_cols) >= 3:
        tab_labels.append("🔗 Correlations")
    
    if 'is_anomaly' in df_filtered.columns:
        tab_labels.append("⚠️ Anomalies")
    
    tab_labels.append("📋 Statistics")
    
    tabs = st.tabs(tab_labels)
    tab_idx = 0
    
    # -- Energy Over Time
    with tabs[tab_idx]:
        tab_idx += 1
        if 'energy' in df_filtered.columns and 'timestamp' in df_filtered.columns:
            plot_df = df_filtered[df_filtered['energy'] > 0]
            fig = px.area(
                plot_df, x='timestamp', y='energy',
                color='source_id',
                title='Energy Production Over Time',
                labels={'energy': 'Energy (kWh)', 'timestamp': '', 'source_id': 'Source'},
                color_discrete_sequence=['#e8a051', '#6ba5d7', '#7db87f', '#b07cc3', '#d4785c'],
            )
            fig.update_traces(line=dict(width=2), fillcolor=None)
            fig.update_layout(**make_plotly_layout('Energy Production Over Time', '', 'Energy (kWh)'))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No energy or timestamp data available.")
    
    # -- Monthly Summary
    with tabs[tab_idx]:
        tab_idx += 1
        if 'energy' in df_filtered.columns and 'timestamp' in df_filtered.columns:
            df_m = df_filtered.copy()
            df_m['month'] = df_m['timestamp'].dt.to_period('M').astype(str)
            monthly = df_m.groupby(['month', 'source_id'])['energy'].sum().reset_index()
            
            fig = px.bar(
                monthly, x='month', y='energy', color='source_id',
                barmode='group',
                labels={'energy': 'Energy (kWh)', 'month': '', 'source_id': 'Source'},
                color_discrete_sequence=['#e8a051', '#6ba5d7', '#7db87f', '#b07cc3'],
            )
            fig.update_traces(marker_line_width=0, marker_cornerradius=6)
            fig.update_layout(**make_plotly_layout('Monthly Energy Summary', '', 'Energy (kWh)'))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No energy or timestamp data available.")
    
    # -- Source Comparison
    with tabs[tab_idx]:
        tab_idx += 1
        if 'energy' in df_filtered.columns and 'source_id' in df_filtered.columns:
            totals = df_filtered.groupby('source_id')['energy'].sum().reset_index()
            totals = totals.sort_values('energy', ascending=True)
            
            fig = px.bar(
                totals, x='energy', y='source_id', orientation='h',
                labels={'energy': 'Total Energy (kWh)', 'source_id': ''},
                color='energy',
                color_continuous_scale=['#6ba5d7', '#e8a051', '#7db87f'],
                text='energy',
            )
            fig.update_traces(
                texttemplate='%{text:,.0f} kWh', textposition='outside',
                marker_line_width=0, marker_cornerradius=6,
            )
            fig.update_layout(**make_plotly_layout('Total Energy by Source', 'Energy (kWh)', ''))
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No energy or source data available.")
    
    # -- Data Quality
    with tabs[tab_idx]:
        tab_idx += 1
        null_pct = (df_filtered.isnull().sum() / len(df_filtered) * 100).sort_values(ascending=True)
        null_pct = null_pct[[c for c in null_pct.index if not c.startswith('_anomaly_')]]
        null_df = pd.DataFrame({'Column': null_pct.index, 'Missing %': null_pct.values})
        
        fig = px.bar(
            null_df, x='Missing %', y='Column', orientation='h',
            color='Missing %',
            color_continuous_scale=['#7db87f', '#e8a051', '#d4785c'],
            range_color=[0, 100],
        )
        fig.update_traces(marker_line_width=0, marker_cornerradius=4)
        fig.update_layout(**make_plotly_layout('Data Completeness', 'Missing %', ''))
        fig.update_layout(xaxis=dict(range=[0, max(100, null_pct.max() + 5)]))
        st.plotly_chart(fig, use_container_width=True)
    
    # -- Correlations
    if len(numeric_cols) >= 3:
        with tabs[tab_idx]:
            tab_idx += 1
            corr_cols = [c for c in numeric_cols if c != 'is_anomaly']
            corr_matrix = df_filtered[corr_cols].corr()
            
            fig = px.imshow(
                corr_matrix, text_auto=".2f",
                color_continuous_scale=["#d4785c", "#1a1e28", "#7db87f"],
                aspect="auto",
            )
            fig.update_layout(**make_plotly_layout('Feature Correlation Matrix', '', ''))
            st.plotly_chart(fig, use_container_width=True)
            
            if 'energy' in corr_matrix.columns:
                energy_corr = corr_matrix['energy'].drop('energy').sort_values(key=abs, ascending=False)
                st.markdown("**Top correlations with Energy:**")
                for col, val in energy_corr.head(5).items():
                    bar_width = abs(val) * 100
                    color = "#7db87f" if val > 0 else "#d4785c"
                    st.markdown(
                        f"<div style='display:flex;align-items:center;gap:0.6rem;margin:4px 0;'>"
                        f"<span style='width:120px;font-size:0.85rem;color:rgba(255,255,255,0.7);'>{col}</span>"
                        f"<div style='flex:1;height:8px;background:rgba(255,255,255,0.06);border-radius:4px;overflow:hidden;'>"
                        f"<div style='width:{bar_width}%;height:100%;background:{color};border-radius:4px;transition:width 0.5s ease;'></div>"
                        f"</div>"
                        f"<span style='width:50px;text-align:right;font-size:0.82rem;font-weight:600;color:{color};'>{val:+.3f}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
    
    # -- Anomalies
    if 'is_anomaly' in df_filtered.columns:
        with tabs[tab_idx]:
            tab_idx += 1
            anomaly_df = df_filtered[df_filtered['is_anomaly'] == True]
            
            if len(anomaly_df) == 0:
                st.markdown("""
                <div style="text-align:center;padding:3rem 0;">
                    <div style="font-size:3rem;margin-bottom:1rem;">✨</div>
                    <div style="font-size:1.1rem;font-weight:600;color:#7db87f;">Looking good — no anomalies found</div>
                    <div style="font-size:0.85rem;color:rgba(240,237,232,0.4);margin-top:0.4rem;">Your data is nice and clean!</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                # Which columns have anomalies
                anomaly_cols = [c.replace('_anomaly_', '') for c in df_filtered.columns if c.startswith('_anomaly_')]
                anomaly_counts = {}
                for col in anomaly_cols:
                    count = int(df_filtered[f'_anomaly_{col}'].sum())
                    if count > 0:
                        anomaly_counts[col] = count
                
                if anomaly_counts:
                    cols_per_row = min(4, len(anomaly_counts))
                    acols = st.columns(cols_per_row)
                    for i, (col, count) in enumerate(sorted(anomaly_counts.items(), key=lambda x: -x[1])):
                        with acols[i % cols_per_row]:
                            st.metric(f"🚩 {col}", f"{count}")
                
                # Scatter
                if 'energy' in df_filtered.columns and 'timestamp' in df_filtered.columns:
                    fig = go.Figure()
                    normal = df_filtered[df_filtered['is_anomaly'] == False]
                    fig.add_trace(go.Scatter(
                        x=normal['timestamp'], y=normal['energy'],
                        mode='markers', name='Normal',
                        marker=dict(color='#6ba5d7', size=5, opacity=0.5),
                    ))
                    fig.add_trace(go.Scatter(
                        x=anomaly_df['timestamp'], y=anomaly_df['energy'],
                        mode='markers', name='Anomaly',
                        marker=dict(color='#d4785c', size=9, symbol='x', opacity=0.9,
                                    line=dict(width=1, color='#d4785c')),
                    ))
                    fig.update_layout(**make_plotly_layout('Anomaly Detection Results', '', 'Energy (kWh)'))
                    st.plotly_chart(fig, use_container_width=True)
                
                with st.expander(f"View {len(anomaly_df)} anomalous rows"):
                    display_cols = [c for c in anomaly_df.columns if not c.startswith('_anomaly_')]
                    st.dataframe(anomaly_df[display_cols], use_container_width=True, height=300)
    
    # -- Statistics
    with tabs[tab_idx]:
        tab_idx += 1
        stat_cols = [c for c in df_filtered.columns
                     if pd.api.types.is_numeric_dtype(df_filtered[c])
                     and not c.startswith('_anomaly_') and c != 'is_anomaly']
        if stat_cols:
            stats = df_filtered[stat_cols].describe().T
            stats.columns = ["Count", "Mean", "Std", "Min", "25%", "50%", "75%", "Max"]
            st.dataframe(stats.style.format("{:.2f}"), use_container_width=True)
        else:
            st.info("No numeric columns to show statistics for.")
    
    # ── Data Preview ─────────────────────────────────────────────────────
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="section-header">
        <div class="icon plum">📋</div>
        <div class="text">
            <h2>Take a closer look</h2>
            <p>Browse the aggregated results or the raw merged data</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    preview_option = st.radio("View:", ["Aggregated", "Raw Merged"], horizontal=True, label_visibility="collapsed")
    
    if preview_option == "Aggregated":
        display_cols = [c for c in df_filtered.columns if not c.startswith('_anomaly_')]
        st.dataframe(df_filtered[display_cols], use_container_width=True, height=400)
    else:
        if st.session_state.merged_df is not None:
            st.dataframe(st.session_state.merged_df, use_container_width=True, height=400)
        else:
            st.info("Raw merged data not available.")
    
    # ── Validation ───────────────────────────────────────────────────────
    
    is_valid, issues = validate_dataframe(df_filtered)
    if issues:
        errors = [i for i in issues if i['severity'] == 'error']
        warnings = [i for i in issues if i['severity'] == 'warning']
        
        if errors:
            for err in errors:
                st.error(f"❌ {err['message']}")
        if warnings:
            with st.expander(f"⚠️ {len(warnings)} validation warning(s)"):
                for w in warnings:
                    st.caption(f"• {w['message']}")
    
    # ── Downloads ────────────────────────────────────────────────────────
    
    st.markdown("""
    <div class="section-header">
        <div class="icon rose">📥</div>
        <div class="text">
            <h2>Take it with you</h2>
            <p>Download your results as CSV or Excel</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    dl1, dl2, dl3 = st.columns(3)
    
    with dl1:
        export_cols = [c for c in df_filtered.columns if not c.startswith('_anomaly_')]
        csv_agg = df_filtered[export_cols].to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇️  Aggregated CSV",
            csv_agg,
            file_name=f"solstice_aggregated_{freq_label.lower()}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    
    with dl2:
        if st.session_state.merged_df is not None:
            csv_raw = st.session_state.merged_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️  Raw Standardised CSV",
                csv_raw,
                file_name="solar_standardized_raw.csv",
                mime="text/csv",
                use_container_width=True,
            )
    
    with dl3:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_filtered[export_cols].to_excel(writer, sheet_name='Aggregated', index=False)
            if st.session_state.merged_df is not None:
                st.session_state.merged_df.to_excel(writer, sheet_name='Raw Standardised', index=False)
        st.download_button(
            "⬇️  Excel Workbook",
            buffer.getvalue(),
            file_name=f"solar_data_{freq_label.lower()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    
    # ── Weather Enrichment ───────────────────────────────────────────────
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="section-header">
        <div class="icon sky">🌤️</div>
        <div class="text">
            <h2>Add weather context</h2>
            <p>Pull in historical weather data from Open-Meteo — completely free</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    w1, w2, w3 = st.columns([2, 2, 1])
    with w1:
        lat = st.number_input("Latitude", value=1.3521, format="%.4f", help="e.g. 1.3521 for Singapore")
    with w2:
        lon = st.number_input("Longitude", value=103.8198, format="%.4f", help="e.g. 103.8198 for Singapore")
    with w3:
        st.markdown("<br>", unsafe_allow_html=True)
        enrich_clicked = st.button("🌤️ Fetch Weather", type="primary", use_container_width=True)
    
    if enrich_clicked:
        with st.spinner("Fetching weather data from Open-Meteo..."):
            try:
                from solstice.weather import enrich_with_weather
                enriched = enrich_with_weather(df, latitude=lat, longitude=lon)
                st.session_state.weather_enriched_df = enriched
                st.session_state.forecaster = None
                new_cols = [c for c in enriched.columns if c not in df.columns]
                st.success(f"Enriched with **{len(new_cols)}** weather features!")
            except Exception as e:
                st.error(f"Weather enrichment failed: {e}")
    
    if st.session_state.weather_enriched_df is not None:
        enriched_df = st.session_state.weather_enriched_df
        weather_cols = [c for c in enriched_df.columns if c not in df.columns]
        if weather_cols:
            with st.expander("📊 Weather data preview", expanded=False):
                st.dataframe(
                    enriched_df[['timestamp', 'source_id'] + weather_cols].head(20),
                    use_container_width=True, height=280,
                )
            csv_enr = enriched_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️  Download Enriched CSV",
                csv_enr, file_name="solar_weather_enriched.csv",
                mime="text/csv", use_container_width=True,
            )
    
    # ── Forecasting ──────────────────────────────────────────────────────
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="section-header">
        <div class="icon plum">🔮</div>
        <div class="text">
            <h2>Predict future output</h2>
            <p>Train a model to forecast daily energy production under different weather scenarios</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    forecast_df = st.session_state.weather_enriched_df or df
    has_features = any(
        c in forecast_df.columns
        for c in ['irradiance', 'ambient_temp', 'humidity', 'wind_speed',
                   'cloud_cover_mean', 'sunshine_hours', 'precipitation_sum']
    )
    
    if not has_features:
        st.markdown("""
        <div class="glass-card" style="text-align:center;padding:2rem;">
            <div style="font-size:2rem;margin-bottom:0.5rem;">💡</div>
            <div style="font-size:0.95rem;color:rgba(240,237,232,0.55);">
                Add weather data above first — it makes the forecasts much more accurate!
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    if 'energy' in forecast_df.columns and len(forecast_df) >= 30:
        fc_col1, fc_col2, fc_col3 = st.columns([1, 2, 1])
        with fc_col2:
            forecast_clicked = st.button("🧠  Train Forecast Model", type="primary", use_container_width=True)
        
        if forecast_clicked:
            progress_fc = st.progress(0, text="Preparing data...")
            try:
                from solstice.forecasting import SolarForecaster
                forecaster = SolarForecaster()
                progress_fc.progress(30, text="Training Random Forest...")
                forecaster.fit(forecast_df)
                progress_fc.progress(100, text="✅ Model trained!")
                st.session_state.forecaster = forecaster
            except ImportError:
                st.error("scikit-learn is required: `pip install scikit-learn`")
            except Exception as e:
                st.error(f"Forecasting error: {e}")
        
        if st.session_state.forecaster is not None:
            forecaster = st.session_state.forecaster
            metrics = forecaster.get_metrics()
            
            # Model metrics
            st.markdown(f"""
            <div class="metric-grid">
                <div class="metric-card amber">
                    <span class="metric-icon">📉</span>
                    <div class="metric-value amber">{metrics.mae:,.1f}</div>
                    <div class="metric-label">Avg. error (kWh)</div>
                </div>
                <div class="metric-card sky">
                    <span class="metric-icon">📊</span>
                    <div class="metric-value sky">{metrics.rmse:,.1f}</div>
                    <div class="metric-label">Root mean sq. error</div>
                </div>
                <div class="metric-card sage">
                    <span class="metric-icon">🎯</span>
                    <div class="metric-value sage">{metrics.r2:.4f}</div>
                    <div class="metric-label">R² accuracy</div>
                </div>
                <div class="metric-card plum">
                    <span class="metric-icon">📐</span>
                    <div class="metric-value plum">{metrics.mape:.1f}%</div>
                    <div class="metric-label">Percentage error</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.caption(f"Trained on {metrics.n_train} samples · Tested on {metrics.n_test} samples · Best params: {metrics.best_params}")
            
            fc_tab1, fc_tab2, fc_tab3 = st.tabs([
                "📈 Actual vs Predicted",
                "🏆 Feature Importance",
                "🌦️ Scenarios",
            ])
            
            with fc_tab1:
                avp = forecaster.get_actual_vs_predicted()
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=avp['timestamp'], y=avp['actual'],
                    mode='lines+markers', name='Actual',
                    line=dict(color='#6ba5d7', width=2),
                    marker=dict(size=5),
                ))
                fig.add_trace(go.Scatter(
                    x=avp['timestamp'], y=avp['predicted'],
                    mode='lines+markers', name='Predicted',
                    line=dict(color='#e8a051', width=2, dash='dot'),
                    marker=dict(size=5),
                ))
                fig.update_layout(**make_plotly_layout(
                    f'Actual vs Predicted  •  R² = {metrics.r2:.3f}', '', 'Energy (kWh)'
                ))
                st.plotly_chart(fig, use_container_width=True)
            
            with fc_tab2:
                importance = forecaster.get_feature_importance()
                fig = px.bar(
                    importance, x='importance', y='feature', orientation='h',
                    color='importance',
                    color_continuous_scale=['#6ba5d7', '#7db87f', '#e8a051'],
                )
                fig.update_traces(marker_line_width=0, marker_cornerradius=4)
                fig.update_layout(**make_plotly_layout('Feature Importance', 'Importance', ''))
                fig.update_layout(
                    yaxis=dict(categoryorder='total ascending'),
                    coloraxis_showscale=False,
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with fc_tab3:
                scenarios = forecaster.predict_scenarios()
                if scenarios:
                    scenario_df = pd.DataFrame([
                        {"Scenario": s.name, "Predicted Energy (kWh)": s.predicted_energy}
                        for s in scenarios
                    ])
                    
                    fig = px.bar(
                        scenario_df,
                        x='Predicted Energy (kWh)', y='Scenario',
                        orientation='h', text='Predicted Energy (kWh)',
                        color='Predicted Energy (kWh)',
                        color_continuous_scale=['#d4785c', '#e8a051', '#7db87f'],
                    )
                    fig.update_traces(
                        texttemplate='%{text:,.0f} kWh', textposition='outside',
                        marker_line_width=0, marker_cornerradius=6,
                    )
                    fig.update_layout(**make_plotly_layout('Energy Predictions by Weather Scenario', 'Predicted Energy (kWh)', ''))
                    fig.update_layout(
                        yaxis=dict(categoryorder='total ascending'),
                        coloraxis_showscale=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)
    elif 'energy' in forecast_df.columns:
        st.info(f"Need at least 30 data points for forecasting (currently {len(forecast_df)}).")
    
    st.markdown("""
    <div class="app-footer">
        Made with ☀️ for the Solstice project<br>
        Powered by Streamlit & Plotly
    </div>
    """, unsafe_allow_html=True)


# ── Empty State (No files uploaded) ──────────────────────────────────────────

if not uploaded_files:
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="steps-grid">
        <div class="step-card">
            <div class="step-number s1">1</div>
            <h3>Drop your files</h3>
            <p>Toss in your CSV or Excel files — inverter logs, weather readings, irradiance measurements, whatever you've got.</p>
        </div>
        <div class="step-card">
            <div class="step-number s2">2</div>
            <h3>Check the mapping</h3>
            <p>We'll auto-detect your columns. If something looks off, just tweak it — or let the AI handle tricky names.</p>
        </div>
        <div class="step-card">
            <div class="step-number s3">3</div>
            <h3>Get your insights</h3>
            <p>One click gives you clean aggregated data, anomaly flags, weather context, and energy forecasts.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Feature highlights
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    f1, f2, f3, f4 = st.columns(4)
    
    with f1:
        st.markdown("""
        <div class="glass-card" style="text-align:center;min-height:160px;">
            <div style="font-size:1.8rem;margin-bottom:0.5rem;">🔎</div>
            <div style="font-size:0.9rem;font-weight:600;color:#f0ede8;margin-bottom:0.3rem;">Spot the outliers</div>
            <div style="font-size:0.78rem;color:rgba(240,237,232,0.4);">IQR & Z-score methods to flag, drop, or clip unusual readings</div>
        </div>
        """, unsafe_allow_html=True)
    
    with f2:
        st.markdown("""
        <div class="glass-card" style="text-align:center;min-height:160px;">
            <div style="font-size:1.8rem;margin-bottom:0.5rem;">🌤️</div>
            <div style="font-size:0.9rem;font-weight:600;color:#f0ede8;margin-bottom:0.3rem;">Weather context</div>
            <div style="font-size:0.78rem;color:rgba(240,237,232,0.4);">Pull in free historical weather data from Open-Meteo</div>
        </div>
        """, unsafe_allow_html=True)
    
    with f3:
        st.markdown("""
        <div class="glass-card" style="text-align:center;min-height:160px;">
            <div style="font-size:1.8rem;margin-bottom:0.5rem;">🔮</div>
            <div style="font-size:0.9rem;font-weight:600;color:#f0ede8;margin-bottom:0.3rem;">Forecast energy</div>
            <div style="font-size:0.78rem;color:rgba(240,237,232,0.4);">Train a model and predict output under different weather scenarios</div>
        </div>
        """, unsafe_allow_html=True)
    
    with f4:
        st.markdown("""
        <div class="glass-card" style="text-align:center;min-height:160px;">
            <div style="font-size:1.8rem;margin-bottom:0.5rem;">🧠</div>
            <div style="font-size:0.9rem;font-weight:600;color:#f0ede8;margin-bottom:0.3rem;">Smart column detection</div>
            <div style="font-size:0.78rem;color:rgba(240,237,232,0.4);">AI figures out tricky column names when keywords don't cut it</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="app-footer">
        Made with ☀️ for the Solstice project<br>
        Powered by Streamlit & Plotly
    </div>
    """, unsafe_allow_html=True)
