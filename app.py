"""
Solar Data Aggregator — Web Interface
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

from solar_aggregator import SolarAggregator, LLMAnalyzer
from solar_aggregator.detection import auto_detect_columns
from solar_aggregator.processing import load_file, validate_dataframe
from solar_aggregator.schema import SCHEMA, print_schema


# ── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Solar Data Aggregator",
    page_icon="☀️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Custom CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* Main header */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #f39c12, #e74c3c, #f39c12);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        color: #888;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    
    /* File cards */
    .file-card {
        background: linear-gradient(135deg, #1a1f2e, #232a3e);
        border: 1px solid #2d3548;
        border-radius: 12px;
        padding: 1.2rem;
        margin-bottom: 0.8rem;
        transition: border-color 0.3s;
    }
    .file-card:hover {
        border-color: #f39c12;
    }
    .file-card .type-badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .type-inverter { background: #27ae6033; color: #2ecc71; }
    .type-environment { background: #3498db33; color: #3498db; }
    .type-irradiance { background: #f39c1233; color: #f39c12; }
    .type-unknown { background: #95a5a633; color: #95a5a6; }
    
    /* Metric cards */
    .metric-row {
        display: flex;
        gap: 1rem;
        margin: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e, #232a3e);
        border: 1px solid #2d3548;
        border-radius: 12px;
        padding: 1rem 1.5rem;
        flex: 1;
        text-align: center;
    }
    .metric-card .value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #f39c12;
    }
    .metric-card .label {
        font-size: 0.85rem;
        color: #888;
        margin-top: 0.2rem;
    }
    
    /* Detection badge */
    .detection-keyword { color: #2ecc71; }
    .detection-llm { color: #f39c12; }
    
    /* Smooth transitions */
    .stButton > button {
        transition: all 0.3s ease;
        border-radius: 8px;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(243, 156, 18, 0.3);
    }
    
    /* Download button styling */
    .stDownloadButton > button {
        width: 100%;
        border-radius: 8px;
    }
    
    /* Hide Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
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


# ── Header ───────────────────────────────────────────────────────────────────

st.markdown('<div class="main-header">☀️ Solar Data Aggregator</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Upload your solar panel and weather data files — get clean, aggregated results in seconds.</div>', unsafe_allow_html=True)


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Settings")
    
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
    
    st.markdown("---")
    
    st.markdown("### 🤖 Smart Detection (Optional)")
    st.caption("If column names are unusual, an LLM can figure them out automatically.")
    api_key = st.text_input(
        "Groq API Key",
        type="password",
        placeholder="gsk_...",
        help="Free key from https://console.groq.com — only needed if keyword detection fails."
    )
    
    st.markdown("---")
    
    st.markdown("### 📖 Schema Reference")
    with st.expander("View Standard Fields"):
        schema_data = []
        for name, field in SCHEMA.items():
            schema_data.append({
                "Field": name,
                "Required": "✅" if field.required else "❌",
                "Aggregation": field.aggregation.value.upper(),
                "Unit": field.unit or "—",
                "Description": field.description,
            })
        st.dataframe(pd.DataFrame(schema_data), hide_index=True, use_container_width=True)


# ── File Upload ──────────────────────────────────────────────────────────────

st.markdown("## 📁 Upload Your Files")

uploaded_files = st.file_uploader(
    "Drag and drop CSV or Excel files",
    type=["csv", "xlsx", "xls"],
    accept_multiple_files=True,
    help="Upload inverter data, weather/environment data, and irradiance data."
)


# ── Process Uploaded Files ───────────────────────────────────────────────────

def save_uploaded_to_temp(uploaded_file) -> str:
    """Save an uploaded file to a temp path and return the path."""
    suffix = os.path.splitext(uploaded_file.name)[1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getbuffer())
    tmp.close()
    return tmp.name


def detect_file(filepath: str, filename: str, df_sample: pd.DataFrame, api_key: str = None):
    """Run detection on a single file, return analysis info."""
    mapping, file_type = auto_detect_columns(df_sample)
    
    mapped_targets = set(mapping.values())
    has_timestamp = "timestamp" in mapped_targets
    has_data = bool(mapped_targets & {"energy", "ambient_temp", "irradiance", "wind_speed", "module_temp", "humidity"})
    keyword_sufficient = has_timestamp and has_data and file_type != "unknown"
    
    detection_method = "keyword"
    confidence = "high" if keyword_sufficient else "low"
    
    # If keyword detection isn't sufficient and we have an API key, try LLM
    if not keyword_sufficient and api_key:
        try:
            analyzer = LLMAnalyzer(api_key=api_key, verbose=False)
            analyzer.add_file(filepath)
            result = analyzer.analyze()
            if result.files:
                fa = result.files[0]
                return {
                    "filename": filename,
                    "filepath": filepath,
                    "file_type": fa.file_type,
                    "mapping": fa.column_mapping,
                    "detection_method": fa.detection_method,
                    "confidence": fa.confidence,
                    "columns": list(df_sample.columns),
                    "row_count": len(df_sample),
                }
        except Exception:
            pass  # Fall through to keyword result
    
    return {
        "filename": filename,
        "filepath": filepath,
        "file_type": file_type,
        "mapping": mapping,
        "detection_method": detection_method,
        "confidence": confidence,
        "columns": list(df_sample.columns),
        "row_count": len(df_sample),
    }


if uploaded_files:
    # Save files and run detection
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
    
    # Remove files that were un-uploaded
    current_names = {uf.name for uf in uploaded_files}
    for name in list(st.session_state.uploaded_files_data.keys()):
        if name not in current_names:
            del st.session_state.uploaded_files_data[name]
    
    # ── File Detection Results ───────────────────────────────────────────
    
    st.markdown("## 🔍 Detected Columns")
    st.caption("Review the auto-detected mappings below. Edit if needed.")
    
    files_data = st.session_state.uploaded_files_data
    
    for fname, fdata in files_data.items():
        type_class = f"type-{fdata['file_type']}" if fdata['file_type'] in ['inverter', 'environment', 'irradiance'] else 'type-unknown'
        
        with st.expander(f"📄 **{fname}** — {fdata['file_type'].upper()} ({fdata['row_count']:,} rows)", expanded=True):
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                method_color = "detection-keyword" if fdata['detection_method'] == 'keyword' else "detection-llm"
                st.markdown(f"**Detection:** <span class='{method_color}'>{fdata['detection_method'].upper()}</span> | **Confidence:** {fdata['confidence']}", unsafe_allow_html=True)
                
                st.markdown("**Column Mappings:**")
                # Show editable mappings
                schema_targets = ["(skip)"] + list(SCHEMA.keys())
                
                if fname not in st.session_state.mappings_edited:
                    st.session_state.mappings_edited[fname] = dict(fdata['mapping'])
                
                current_mapping = st.session_state.mappings_edited[fname]
                new_mapping = {}
                
                for col in fdata['columns']:
                    current_target = current_mapping.get(col, "(skip)")
                    idx = schema_targets.index(current_target) if current_target in schema_targets else 0
                    selected = st.selectbox(
                        f"`{col}`",
                        schema_targets,
                        index=idx,
                        key=f"map_{fname}_{col}"
                    )
                    if selected != "(skip)":
                        new_mapping[col] = selected
                
                st.session_state.mappings_edited[fname] = new_mapping
            
            with col2:
                st.markdown("**Data Preview:**")
                st.dataframe(fdata['df_sample'].head(5), use_container_width=True, height=200)
    
    
    # ── Aggregate Button ─────────────────────────────────────────────────
    
    st.markdown("---")
    
    col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
    with col_btn2:
        run_clicked = st.button(
            "🚀 Aggregate Data",
            use_container_width=True,
            type="primary",
        )
    
    if run_clicked:
        with st.spinner("Processing your data..."):
            try:
                agg = SolarAggregator(verbose=False)
                
                for fname, fdata in files_data.items():
                    mapping = st.session_state.mappings_edited.get(fname, fdata['mapping'])
                    source_id = fname.replace(".csv", "").replace(".xlsx", "").replace("_data", "").upper()
                    agg.add_file(
                        filepath=fdata['filepath'],
                        source_id=source_id,
                        mapping=mapping,
                    )
                
                aggregated_df = agg.aggregate(freq=freq)
                merged_df = agg.get_dataframe("merged")
                
                st.session_state.aggregated_df = aggregated_df
                st.session_state.merged_df = merged_df
                st.session_state.analysis_done = True
                
                st.success(f"✅ Aggregated {len(aggregated_df):,} rows successfully!")
                
            except Exception as e:
                st.error(f"❌ Error during aggregation: {str(e)}")
                st.session_state.analysis_done = False


# ── Results Dashboard ────────────────────────────────────────────────────────

if st.session_state.analysis_done and st.session_state.aggregated_df is not None:
    df = st.session_state.aggregated_df
    
    st.markdown("---")
    st.markdown("## 📊 Results Dashboard")
    
    # ── Summary Metrics ──────────────────────────────────────────────────
    
    metric_cols = st.columns(4)
    
    with metric_cols[0]:
        total_energy = df['energy'].sum() if 'energy' in df.columns else 0
        st.metric("⚡ Total Energy", f"{total_energy:,.0f} kWh")
    
    with metric_cols[1]:
        date_range_days = (df['timestamp'].max() - df['timestamp'].min()).days if 'timestamp' in df.columns else 0
        st.metric("📅 Date Range", f"{date_range_days} days")
    
    with metric_cols[2]:
        n_sources = df['source_id'].nunique() if 'source_id' in df.columns else 0
        st.metric("🔌 Sources", f"{n_sources}")
    
    with metric_cols[3]:
        st.metric("📋 Total Rows", f"{len(df):,}")
    
    # ── Charts ───────────────────────────────────────────────────────────
    
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Energy Over Time", "📊 Monthly Summary", "🎯 Panel Comparison", "🔍 Data Quality"])
    
    with tab1:
        if 'energy' in df.columns and 'timestamp' in df.columns:
            fig = px.line(
                df[df['energy'] > 0],
                x='timestamp',
                y='energy',
                color='source_id',
                title='Energy Production Over Time',
                labels={'energy': 'Energy (kWh)', 'timestamp': 'Date', 'source_id': 'Source'},
                template='plotly_dark',
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#fafafa'),
                hovermode='x unified',
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No energy or timestamp data available for this chart.")
    
    with tab2:
        if 'energy' in df.columns and 'timestamp' in df.columns:
            df_monthly = df.copy()
            df_monthly['month'] = df_monthly['timestamp'].dt.to_period('M').astype(str)
            monthly_agg = df_monthly.groupby(['month', 'source_id'])['energy'].sum().reset_index()
            
            fig = px.bar(
                monthly_agg,
                x='month',
                y='energy',
                color='source_id',
                barmode='group',
                title='Monthly Energy Production',
                labels={'energy': 'Energy (kWh)', 'month': 'Month', 'source_id': 'Source'},
                template='plotly_dark',
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#fafafa'),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No energy or timestamp data available for this chart.")
    
    with tab3:
        if 'energy' in df.columns and 'source_id' in df.columns:
            totals = df.groupby('source_id')['energy'].sum().reset_index()
            totals = totals.sort_values('energy', ascending=True)
            
            fig = px.bar(
                totals,
                x='energy',
                y='source_id',
                orientation='h',
                title='Total Energy by Source',
                labels={'energy': 'Total Energy (kWh)', 'source_id': 'Source'},
                template='plotly_dark',
                text='energy',
            )
            fig.update_traces(texttemplate='%{text:,.0f} kWh', textposition='outside')
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#fafafa'),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No energy or source data available for this chart.")
    
    with tab4:
        # Missing data analysis
        null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=True)
        null_df = pd.DataFrame({'Column': null_pct.index, 'Missing %': null_pct.values})
        
        fig = px.bar(
            null_df,
            x='Missing %',
            y='Column',
            orientation='h',
            title='Missing Data by Column',
            template='plotly_dark',
            color='Missing %',
            color_continuous_scale=['#2ecc71', '#f39c12', '#e74c3c'],
            range_color=[0, 100],
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#fafafa'),
            xaxis=dict(range=[0, 100]),
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # ── Data Preview ─────────────────────────────────────────────────────
    
    st.markdown("### 📋 Data Preview")
    preview_option = st.radio(
        "View:",
        ["Aggregated Data", "Raw Merged Data"],
        horizontal=True,
    )
    
    if preview_option == "Aggregated Data":
        st.dataframe(df, use_container_width=True, height=400)
    else:
        if st.session_state.merged_df is not None:
            st.dataframe(st.session_state.merged_df, use_container_width=True, height=400)
        else:
            st.info("Raw merged data not available.")
    
    # ── Validation ───────────────────────────────────────────────────────
    
    is_valid, errors = validate_dataframe(df)
    if not is_valid:
        st.warning("⚠️ Data Validation Issues:")
        for err in errors:
            st.markdown(f"- {err}")
    
    # ── Downloads ────────────────────────────────────────────────────────
    
    st.markdown("### 📥 Download Results")
    
    dl_col1, dl_col2 = st.columns(2)
    
    with dl_col1:
        csv_agg = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇️ Download Aggregated CSV",
            csv_agg,
            file_name=f"solar_aggregated_{freq_label.lower()}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    
    with dl_col2:
        if st.session_state.merged_df is not None:
            csv_raw = st.session_state.merged_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️ Download Raw Standardised CSV",
                csv_raw,
                file_name="solar_standardized_raw.csv",
                mime="text/csv",
                use_container_width=True,
            )


# ── Empty State ──────────────────────────────────────────────────────────────

if not uploaded_files:
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 1️⃣ Upload
        Drop your CSV or Excel files above.  
        Supports inverter data, weather data, and irradiance data.
        """)
    
    with col2:
        st.markdown("""
        ### 2️⃣ Review
        Check the auto-detected column mappings.  
        Edit them if the detection got something wrong.
        """)
    
    with col3:
        st.markdown("""
        ### 3️⃣ Download
        Get clean, aggregated data as CSV.  
        View interactive charts right in your browser.
        """)
    
    st.markdown("---")
    st.caption("Built with ❤️ for the Solar Data Aggregator FYP | Powered by Streamlit")
