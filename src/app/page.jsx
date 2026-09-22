'use client';

import React, { useState, useEffect, useMemo } from 'react';
import {
  Upload,
  Sun,
  Zap,
  CloudSun,
  Activity,
  Download,
  AlertTriangle,
  CheckCircle2,
  Settings,
  ChevronRight,
  TrendingUp,
  BarChart3,
  Sliders,
  Sparkles,
  RefreshCw,
  FileSpreadsheet,
} from 'lucide-react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Line, Bar } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const SCHEMA_FIELDS = [
  { name: 'timestamp', req: true, unit: 'ISO / Date', desc: 'Date and time of record' },
  { name: 'energy', req: false, unit: 'kWh / MWh', desc: 'Solar generation yield' },
  { name: 'irradiance', req: false, unit: 'W/m²', desc: 'Solar radiation intensity' },
  { name: 'ambient_temp', req: false, unit: '°C', desc: 'Surrounding air temperature' },
  { name: 'module_temp', req: false, unit: '°C', desc: 'Photovoltaic panel temperature' },
  { name: 'wind_speed', req: false, unit: 'm/s', desc: 'Wind velocity' },
  { name: 'humidity', req: false, unit: '%', desc: 'Relative humidity' },
];

export default function SolarAggregatorPage() {
  const [activeStep, setActiveStep] = useState(1);
  const [apiOnline, setApiOnline] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingMsg, setLoadingMsg] = useState('');
  const [errorMsg, setErrorMsg] = useState('');

  // Step 1: Upload & Detect State
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [detectionResults, setDetectionResults] = useState([]);
  const [mappings, setMappings] = useState({});
  const [groqKey, setGroqKey] = useState('');
  const [showSchema, setShowSchema] = useState(false);

  // Step 2: Settings
  const [freq, setFreq] = useState('1D');
  const [anomalyEnabled, setAnomalyEnabled] = useState(true);
  const [anomalyMethod, setAnomalyMethod] = useState('iqr');
  const [anomalyStrategy, setAnomalyStrategy] = useState('flag');

  // Step 3: Processed Results
  const [processedData, setProcessedData] = useState(null);

  // Step 4: Weather & Forecast
  const [lat, setLat] = useState('1.3521');
  const [lon, setLon] = useState('103.8198');
  const [weatherEnriched, setWeatherEnriched] = useState(false);
  const [forecastData, setForecastData] = useState(null);

  // Check API health on mount
  useEffect(() => {
    fetch('/api/health')
      .then((res) => res.json())
      .then((data) => {
        if (data.status === 'ok') setApiOnline(true);
      })
      .catch(() => setApiOnline(false));
  }, []);

  // Handle Drag & Drop Upload
  const handleDrop = async (e) => {
    e.preventDefault();
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      const files = Array.from(e.dataTransfer.files);
      await processFilesDetection(files);
    }
  };

  const handleFileInput = async (e) => {
    if (e.target.files && e.target.files.length > 0) {
      const files = Array.from(e.target.files);
      await processFilesDetection(files);
    }
  };

  // Call /api/detect
  const processFilesDetection = async (files) => {
    setLoading(true);
    setLoadingMsg('Analyzing headers and detecting column schemas...');
    setErrorMsg('');

    try {
      const formData = new FormData();
      files.forEach((f) => formData.append('files', f));
      if (groqKey.trim()) {
        formData.append('groq_api_key', groqKey.trim());
      }

      const res = await fetch('/api/detect', {
        method: 'POST',
        body: formData,
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Failed to detect column schema');
      }

      const data = await res.json();
      setUploadedFiles(files);
      setDetectionResults(data.files || []);

      // Initialize mappings
      const initialMappings = {};
      (data.files || []).forEach((f) => {
        initialMappings[f.filename] = { ...f.mapping };
      });
      setMappings(initialMappings);
      setActiveStep(1);
    } catch (err) {
      setErrorMsg(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Handle Mapping change
  const handleMappingChange = (filename, col, targetField) => {
    setMappings((prev) => {
      const updated = { ...(prev[filename] || {}) };
      if (targetField === '(skip)') {
        delete updated[col];
      } else {
        updated[col] = targetField;
      }
      return { ...prev, [filename]: updated };
    });
  };

  // Run Aggregation Pipeline
  const runAggregation = async () => {
    if (!uploadedFiles.length) {
      setErrorMsg('Please upload at least one solar file first.');
      return;
    }

    setLoading(true);
    setLoadingMsg('Merging streams, aligning timestamps & evaluating anomalies...');
    setErrorMsg('');

    try {
      const formData = new FormData();
      uploadedFiles.forEach((f) => formData.append('files', f));
      formData.append('mappings_json', JSON.stringify(mappings));
      formData.append('freq', freq);
      formData.append('anomaly_enabled', String(anomalyEnabled));
      formData.append('anomaly_method', anomalyMethod);
      formData.append('anomaly_strategy', anomalyStrategy);

      const res = await fetch('/api/process', {
        method: 'POST',
        body: formData,
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Aggregation pipeline failed');
      }

      const result = await res.json();
      setProcessedData(result);
      setWeatherEnriched(false);
      setForecastData(null);
      setActiveStep(3);
    } catch (err) {
      setErrorMsg(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Run Weather Enrichment
  const runWeatherEnrichment = async () => {
    if (!processedData || !processedData.records) return;

    setLoading(true);
    setLoadingMsg(`Contacting Open-Meteo for coordinates (${lat}, ${lon})...`);
    setErrorMsg('');

    try {
      const res = await fetch('/api/enrich-weather', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          records: processedData.records,
          latitude: parseFloat(lat),
          longitude: parseFloat(lon),
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Weather enrichment failed');
      }

      const result = await res.json();
      setProcessedData((prev) => ({
        ...prev,
        records: result.records,
        weatherColumns: result.weather_columns,
      }));
      setWeatherEnriched(true);
    } catch (err) {
      setErrorMsg(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Run ML Forecast
  const runForecast = async () => {
    if (!processedData || !processedData.records) return;

    setLoading(true);
    setLoadingMsg('Training Random Forest model & synthesizing weather scenarios...');
    setErrorMsg('');

    try {
      const res = await fetch('/api/forecast', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          records: processedData.records,
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Forecasting failed');
      }

      const result = await res.json();
      setForecastData(result);
      setActiveStep(4);
    } catch (err) {
      setErrorMsg(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Export Data
  const handleExport = async (format) => {
    if (!processedData || !processedData.records) return;

    try {
      const res = await fetch('/api/export', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          records: processedData.records,
          format: format,
        }),
      });

      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `solar_aggregated.${format === 'xlsx' ? 'xlsx' : 'csv'}`;
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (err) {
      setErrorMsg('Export failed: ' + err.message);
    }
  };

  // Build Time-Series Chart Data
  const timeSeriesChartData = useMemo(() => {
    if (!processedData || !processedData.records || processedData.records.length === 0) {
      return null;
    }

    const records = processedData.records;
    const labels = records.map((r) => {
      const d = new Date(r.timestamp);
      return isNaN(d.getTime()) ? r.timestamp : d.toLocaleDateString();
    });

    const energyValues = records.map((r) => r.energy ?? null);
    const irradianceValues = records.map((r) => r.irradiance ?? null);

    return {
      labels,
      datasets: [
        {
          label: 'Energy (kWh)',
          data: energyValues,
          borderColor: '#e8a051',
          backgroundColor: 'rgba(232, 160, 81, 0.15)',
          fill: true,
          tension: 0.25,
          pointRadius: records.length > 60 ? 0 : 3,
          pointHoverRadius: 6,
          pointBackgroundColor: '#f5c27f',
          yAxisID: 'y',
        },
        ...(records[0]?.irradiance !== undefined
          ? [
              {
                label: 'Solar Irradiance (W/m²)',
                data: irradianceValues,
                borderColor: '#5ea3db',
                backgroundColor: 'transparent',
                borderDash: [5, 5],
                tension: 0.2,
                pointRadius: 0,
                yAxisID: 'y1',
              },
            ]
          : []),
      ],
    };
  }, [processedData]);

  // Forecast Comparison Chart
  const forecastChartData = useMemo(() => {
    if (!forecastData || !forecastData.test_curve || forecastData.test_curve.length === 0) {
      return null;
    }

    const curve = forecastData.test_curve;
    return {
      labels: curve.map((c) => c.date),
      datasets: [
        {
          label: 'Actual Energy (kWh)',
          data: curve.map((c) => c.actual),
          borderColor: '#a876cc',
          backgroundColor: 'rgba(168, 118, 204, 0.12)',
          fill: true,
          tension: 0.2,
          pointRadius: 3,
        },
        {
          label: 'Predicted Model (kWh)',
          data: curve.map((c) => c.predicted),
          borderColor: '#52b788',
          backgroundColor: 'transparent',
          borderDash: [4, 4],
          tension: 0.2,
          pointRadius: 3,
        },
      ],
    };
  }, [forecastData]);

  return (
    <div className="app-container">
      {/* ── Top Navbar ────────────────────────────────────────── */}
      <nav className="navbar">
        <div className="nav-brand">
          <div className="brand-icon-wrap">☀️</div>
          <div>
            <span className="brand-title">Solar Data Aggregator</span>
            <span className="brand-tag">Vercel Native</span>
          </div>
        </div>
        <div className="nav-status">
          <div className="status-dot" style={{ background: apiOnline ? '#52b788' : '#e06d75' }} />
          <span>{apiOnline ? 'FastAPI Serverless Online' : 'Connecting to API...'}</span>
        </div>
      </nav>

      {/* ── Hero Banner ───────────────────────────────────────── */}
      <header className="hero">
        <div className="hero-glow-1" />
        <div className="hero-glow-2" />
        <div className="hero-pill">
          <Sparkles size={14} /> Intelligent Solar Data Pipeline
        </div>
        <h1 className="hero-title">Harmonize &amp; Forecast Solar Energy</h1>
        <p className="hero-desc">
          Drop in raw multi-inverter and environmental files. We automatically map columns,
          align timestamps, filter anomalies, enrich with Open-Meteo weather, and forecast yields
          with Random Forest ML.
        </p>
      </header>

      {/* ── Stepper Navigation ────────────────────────────────── */}
      <div className="steps-nav">
        <button
          className={`step-btn ${activeStep === 1 ? 'active' : ''}`}
          onClick={() => setActiveStep(1)}
        >
          <div className="step-num">1</div>
          <div className="step-text">
            <span className="step-label">Input</span>
            <span className="step-name">Upload &amp; Detect</span>
          </div>
        </button>

        <button
          className={`step-btn ${activeStep === 2 ? 'active' : ''}`}
          onClick={() => setActiveStep(2)}
        >
          <div className="step-num">2</div>
          <div className="step-text">
            <span className="step-label">Pipeline</span>
            <span className="step-name">Aggregation Rules</span>
          </div>
        </button>

        <button
          className={`step-btn ${activeStep === 3 ? 'active' : ''}`}
          onClick={() => setActiveStep(3)}
          disabled={!processedData}
        >
          <div className="step-num">3</div>
          <div className="step-text">
            <span className="step-label">Analytics</span>
            <span className="step-name">Interactive Studio</span>
          </div>
        </button>

        <button
          className={`step-btn ${activeStep === 4 ? 'active' : ''}`}
          onClick={() => setActiveStep(4)}
          disabled={!processedData}
        >
          <div className="step-num">4</div>
          <div className="step-text">
            <span className="step-label">Intelligence</span>
            <span className="step-name">Weather &amp; ML</span>
          </div>
        </button>
      </div>

      {/* ── Notifications / Loading States ────────────────────── */}
      {loading && (
        <div
          className="card"
          style={{
            textAlign: 'center',
            borderColor: 'var(--amber-primary)',
            background: 'rgba(232, 160, 81, 0.08)',
          }}
        >
          <RefreshCw size={28} className="animate-spin" style={{ margin: '0 auto 12px auto', color: '#e8a051' }} />
          <h3 style={{ fontSize: '1.15rem', color: '#f5c27f', marginBottom: '4px' }}>Processing</h3>
          <p style={{ fontSize: '0.9rem', color: 'var(--text-secondary)' }}>{loadingMsg}</p>
        </div>
      )}

      {errorMsg && (
        <div
          className="card"
          style={{
            borderColor: 'var(--coral)',
            background: 'rgba(224, 109, 117, 0.1)',
            display: 'flex',
            alignItems: 'center',
            gap: '12px',
          }}
        >
          <AlertTriangle color="#e06d75" size={24} />
          <div style={{ flex: 1 }}>
            <h4 style={{ color: '#e06d75', fontSize: '0.95rem' }}>Error Encountered</h4>
            <p style={{ color: 'var(--text-main)', fontSize: '0.88rem' }}>{errorMsg}</p>
          </div>
          <button
            className="btn btn-secondary"
            style={{ padding: '4px 12px', fontSize: '0.78rem' }}
            onClick={() => setErrorMsg('')}
          >
            Dismiss
          </button>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════ */}
      {/* STEP 1: UPLOAD & DETECT                                    */}
      {/* ═════════════════════════════════════════════════════════ */}
      {activeStep === 1 && (
        <section>
          <div
            className="dropzone"
            onDragOver={(e) => e.preventDefault()}
            onDrop={handleDrop}
          >
            <input
              type="file"
              multiple
              accept=".csv,.xlsx,.xls"
              className="dropzone-input"
              onChange={handleFileInput}
            />
            <div className="dropzone-icon">📁</div>
            <h3 className="dropzone-title">Drop your CSV or Excel files here</h3>
            <p className="dropzone-sub">
              Drag &amp; drop solar inverters, environmental or weather data files, or click to browse
            </p>
          </div>

          {/* Optional Smart Detection Groq Key */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: '1rem',
              marginTop: '1.25rem',
              padding: '1rem 1.4rem',
              background: 'rgba(18, 22, 32, 0.5)',
              borderRadius: 'var(--radius-md)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
              <Sparkles size={18} color="#e8a051" />
              <div>
                <div style={{ fontSize: '0.88rem', fontWeight: 600 }}>LLM Schema Detective (Groq API)</div>
                <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>
                  Optional: AI assisted mapping for non-standard or foreign column names
                </div>
              </div>
            </div>
            <input
              type="password"
              placeholder="gsk_..."
              value={groqKey}
              onChange={(e) => setGroqKey(e.target.value)}
              style={{
                background: 'rgba(12, 14, 20, 0.8)',
                border: '1px solid var(--border-subtle)',
                color: 'var(--text-main)',
                padding: '6px 12px',
                borderRadius: '8px',
                fontSize: '0.85rem',
                width: '240px',
                outline: 'none',
              }}
            />
          </div>

          {/* Detection Results */}
          {detectionResults.length > 0 && (
            <div className="file-list">
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <h3 className="font-title" style={{ fontSize: '1.25rem', color: 'var(--text-main)' }}>
                  Detected Schemas ({detectionResults.length} files)
                </h3>
                <button
                  className="btn btn-secondary"
                  style={{ fontSize: '0.8rem', padding: '6px 12px' }}
                  onClick={() => setShowSchema(!showSchema)}
                >
                  {showSchema ? 'Hide Schema Guide' : 'View Target Schema'}
                </button>
              </div>

              {showSchema && (
                <div className="card" style={{ padding: '1.25rem' }}>
                  <h4 style={{ marginBottom: '0.75rem', fontSize: '0.95rem', color: 'var(--amber-light)' }}>
                    Standard Solar Schema Reference
                  </h4>
                  <div className="table-wrap">
                    <table className="custom-table">
                      <thead>
                        <tr>
                          <th>Target Field</th>
                          <th>Required</th>
                          <th>Units</th>
                          <th>Description</th>
                        </tr>
                      </thead>
                      <tbody>
                        {SCHEMA_FIELDS.map((sf) => (
                          <tr key={sf.name}>
                            <td style={{ fontFamily: 'monospace', color: '#e8a051' }}>{sf.name}</td>
                            <td>{sf.req ? '✅ Yes' : 'Optional'}</td>
                            <td>{sf.unit}</td>
                            <td>{sf.desc}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {detectionResults.map((f) => (
                <div key={f.filename} className="file-card">
                  <div className="file-header">
                    <div className="file-info">
                      <span className="file-name">{f.filename}</span>
                      <span className={`badge ${f.file_type}`}>{f.file_type}</span>
                      <span className="badge method">{f.detection_method.toUpperCase()}</span>
                      <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                        {f.row_count.toLocaleString()} rows • Confidence: {f.confidence}
                      </span>
                    </div>
                  </div>

                  <div className="mapping-grid">
                    {f.columns.map((col) => {
                      const currentMapped = mappings[f.filename]?.[col] || '(skip)';
                      return (
                        <div key={col} className="map-item">
                          <span className="col-source-name" title={col}>
                            {col}
                          </span>
                          <select
                            className="select-schema"
                            value={currentMapped}
                            onChange={(e) => handleMappingChange(f.filename, col, e.target.value)}
                          >
                            <option value="(skip)">(skip)</option>
                            {SCHEMA_FIELDS.map((sf) => (
                              <option key={sf.name} value={sf.name}>
                                {sf.name}
                              </option>
                            ))}
                          </select>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ))}

              <div style={{ textAlign: 'right', marginTop: '1rem' }}>
                <button
                  className="btn btn-primary"
                  onClick={() => setActiveStep(2)}
                >
                  Configure Aggregation <ChevronRight size={18} />
                </button>
              </div>
            </div>
          )}
        </section>
      )}

      {/* ═════════════════════════════════════════════════════════ */}
      {/* STEP 2: PIPELINE & AGGREGATION RULES                       */}
      {/* ═════════════════════════════════════════════════════════ */}
      {activeStep === 2 && (
        <section className="card">
          <h2 className="font-title" style={{ fontSize: '1.4rem', marginBottom: '1.5rem' }}>
            Configure Aggregation &amp; Quality Filters
          </h2>

          <div className="controls-bar" style={{ marginTop: 0 }}>
            {/* Aggregation Frequency */}
            <div className="control-group">
              <span className="control-label">Interval:</span>
              <div className="btn-pill-group">
                {[
                  { label: 'Hourly', val: '1h' },
                  { label: 'Daily', val: '1D' },
                  { label: 'Weekly', val: '1W' },
                  { label: 'Monthly', val: '1M' },
                ].map((item) => (
                  <button
                    key={item.val}
                    className={`btn-pill ${freq === item.val ? 'active' : ''}`}
                    onClick={() => setFreq(item.val)}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Anomaly Detection Toggle */}
            <div className="control-group">
              <span className="control-label">Anomaly Detection:</span>
              <div className="btn-pill-group">
                <button
                  className={`btn-pill ${anomalyEnabled ? 'active' : ''}`}
                  onClick={() => setAnomalyEnabled(true)}
                >
                  Enabled
                </button>
                <button
                  className={`btn-pill ${!anomalyEnabled ? 'active' : ''}`}
                  onClick={() => setAnomalyEnabled(false)}
                >
                  Disabled
                </button>
              </div>
            </div>

            {/* Method & Strategy */}
            {anomalyEnabled && (
              <>
                <div className="control-group">
                  <span className="control-label">Method:</span>
                  <select
                    className="select-schema"
                    value={anomalyMethod}
                    onChange={(e) => setAnomalyMethod(e.target.value)}
                  >
                    <option value="iqr">IQR (Interquartile Range)</option>
                    <option value="zscore">Z-Score</option>
                  </select>
                </div>

                <div className="control-group">
                  <span className="control-label">Handling Strategy:</span>
                  <select
                    className="select-schema"
                    value={anomalyStrategy}
                    onChange={(e) => setAnomalyStrategy(e.target.value)}
                  >
                    <option value="flag">🚩 Flag only</option>
                    <option value="drop">🗑️ Remove rows</option>
                    <option value="clip">📐 Clip to bounds</option>
                  </select>
                </div>
              </>
            )}
          </div>

          <div style={{ marginTop: '2.5rem', display: 'flex', justifyContent: 'space-between' }}>
            <button className="btn btn-secondary" onClick={() => setActiveStep(1)}>
              Back to Files
            </button>
            <button className="btn btn-primary" onClick={runAggregation}>
              <Zap size={18} /> Run Aggregation Pipeline
            </button>
          </div>
        </section>
      )}

      {/* ═════════════════════════════════════════════════════════ */}
      {/* STEP 3: ANALYTICS & INTERACTIVE CHARTS                     */}
      {/* ═════════════════════════════════════════════════════════ */}
      {activeStep === 3 && processedData && (
        <section>
          {/* KPI Cards */}
          <div className="metrics-grid">
            <div className="metric-card amber">
              <div className="metric-icon">⚡</div>
              <div className="metric-value">
                {processedData.summary.total_energy
                  ? `${processedData.summary.total_energy.toLocaleString()} kWh`
                  : 'N/A'}
              </div>
              <div className="metric-label">Total Generation</div>
            </div>

            <div className="metric-card sky">
              <div className="metric-icon">☀️</div>
              <div className="metric-value">
                {processedData.summary.avg_irradiance
                  ? `${processedData.summary.avg_irradiance} W/m²`
                  : 'N/A'}
              </div>
              <div className="metric-label">Mean Irradiance</div>
            </div>

            <div className="metric-card emerald">
              <div className="metric-icon">📊</div>
              <div className="metric-value">{processedData.summary.total_rows.toLocaleString()}</div>
              <div className="metric-label">Processed Intervals</div>
            </div>

            <div className="metric-card coral">
              <div className="metric-icon">🚩</div>
              <div className="metric-value">{processedData.summary.anomaly_count}</div>
              <div className="metric-label">Anomalies Detected</div>
            </div>
          </div>

          {/* Time Series Chart */}
          {timeSeriesChartData && (
            <div className="chart-card">
              <div className="chart-header">
                <div>
                  <h3 className="chart-title">Solar Generation Timeline</h3>
                  <p className="chart-sub">
                    Aggregated production ({freq}) over time with irradiance alignment
                  </p>
                </div>
                <div style={{ display: 'flex', gap: '8px' }}>
                  <button
                    className="btn btn-secondary"
                    style={{ padding: '6px 12px', fontSize: '0.8rem' }}
                    onClick={() => handleExport('csv')}
                  >
                    <Download size={14} /> CSV
                  </button>
                  <button
                    className="btn btn-secondary"
                    style={{ padding: '6px 12px', fontSize: '0.8rem' }}
                    onClick={() => handleExport('xlsx')}
                  >
                    <FileSpreadsheet size={14} /> Excel
                  </button>
                </div>
              </div>

              <div style={{ height: '360px' }}>
                <Line
                  data={timeSeriesChartData}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                      x: {
                        grid: { color: 'rgba(255,255,255,0.05)' },
                        ticks: { color: '#9ca3af', maxTicksLimit: 14 },
                      },
                      y: {
                        grid: { color: 'rgba(255,255,255,0.05)' },
                        ticks: { color: '#9ca3af' },
                        title: { display: true, text: 'kWh', color: '#e8a051' },
                      },
                      y1: {
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        ticks: { color: '#5ea3db' },
                        title: { display: true, text: 'W/m²', color: '#5ea3db' },
                      },
                    },
                    plugins: {
                      legend: { labels: { color: '#f5f3ef' } },
                      tooltip: {
                        backgroundColor: '#161b26',
                        titleColor: '#e8a051',
                        bodyColor: '#f5f3ef',
                        borderColor: 'rgba(232,160,81,0.3)',
                        borderWidth: 1,
                      },
                    },
                  }}
                />
              </div>
            </div>
          )}

          {/* Enriched & Forecast Callout */}
          <div
            className="card"
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: '1rem',
              borderColor: 'var(--amber-border)',
              background: 'linear-gradient(135deg, rgba(232,160,81,0.1) 0%, rgba(22,27,38,0.7) 100%)',
            }}
          >
            <div>
              <h3 style={{ fontSize: '1.2rem', marginBottom: '4px' }}>
                Ready to enrich with Open-Meteo &amp; Forecast?
              </h3>
              <p style={{ color: 'var(--text-secondary)', fontSize: '0.88rem' }}>
                Fetch live satellite weather data and train a Random Forest ML model for scenario analysis.
              </p>
            </div>
            <button className="btn btn-primary" onClick={() => setActiveStep(4)}>
              Proceed to ML &amp; Weather <ChevronRight size={18} />
            </button>
          </div>
        </section>
      )}

      {/* ═════════════════════════════════════════════════════════ */}
      {/* STEP 4: WEATHER ENRICHMENT & FORECASTING                   */}
      {/* ═════════════════════════════════════════════════════════ */}
      {activeStep === 4 && processedData && (
        <section>
          {/* Weather Settings */}
          <div className="card">
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.25rem' }}>
              <CloudSun size={24} color="#5ea3db" />
              <h3 className="font-title" style={{ fontSize: '1.25rem' }}>
                Open-Meteo Weather Enrichment
              </h3>
            </div>
            <p style={{ fontSize: '0.88rem', color: 'var(--text-secondary)', marginBottom: '1.25rem' }}>
              Fetches historical temperature, clearness index, sunshine duration, and solar radiation
              for your array's geographical location.
            </p>

            <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <span style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>Latitude:</span>
                <input
                  type="text"
                  value={lat}
                  onChange={(e) => setLat(e.target.value)}
                  style={{
                    background: 'var(--bg-surface)',
                    border: '1px solid var(--border-subtle)',
                    color: 'var(--text-main)',
                    padding: '6px 12px',
                    borderRadius: '8px',
                    width: '110px',
                  }}
                />
              </div>

              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <span style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>Longitude:</span>
                <input
                  type="text"
                  value={lon}
                  onChange={(e) => setLon(e.target.value)}
                  style={{
                    background: 'var(--bg-surface)',
                    border: '1px solid var(--border-subtle)',
                    color: 'var(--text-main)',
                    padding: '6px 12px',
                    borderRadius: '8px',
                    width: '110px',
                  }}
                />
              </div>

              <button className="btn btn-secondary" onClick={runWeatherEnrichment}>
                <CloudSun size={16} /> Fetch Weather Data
              </button>

              {weatherEnriched && (
                <span style={{ fontSize: '0.82rem', color: '#52b788', display: 'flex', alignItems: 'center', gap: '4px' }}>
                  <CheckCircle2 size={16} /> Weather Features Synced!
                </span>
              )}
            </div>
          </div>

          {/* Forecasting Trigger */}
          <div style={{ textAlign: 'center', margin: '2rem 0' }}>
            <button className="btn btn-primary" style={{ padding: '0.9rem 2.5rem' }} onClick={runForecast}>
              <TrendingUp size={20} /> Train Random Forest Forecaster
            </button>
          </div>

          {/* Forecast Results */}
          {forecastData && (
            <div>
              {/* Metrics Header */}
              <div className="metrics-grid">
                <div className="metric-card emerald">
                  <div className="metric-icon">🎯</div>
                  <div className="metric-value">{forecastData.metrics.r2}</div>
                  <div className="metric-label">Model Fit (R² Score)</div>
                </div>

                <div className="metric-card amber">
                  <div className="metric-icon">📉</div>
                  <div className="metric-value">{forecastData.metrics.mae} kWh</div>
                  <div className="metric-label">Mean Absolute Error</div>
                </div>

                <div className="metric-card sky">
                  <div className="metric-icon">📐</div>
                  <div className="metric-value">{forecastData.metrics.rmse} kWh</div>
                  <div className="metric-label">RMSE Deviation</div>
                </div>

                <div className="metric-card coral">
                  <div className="metric-icon">🧪</div>
                  <div className="metric-value">{forecastData.metrics.n_test} days</div>
                  <div className="metric-label">Holdout Test Samples</div>
                </div>
              </div>

              {/* Forecast Actual vs Predicted Curve */}
              {forecastChartData && (
                <div className="chart-card">
                  <div className="chart-header">
                    <div>
                      <h3 className="chart-title">Test Validation: Actual vs. Forecast</h3>
                      <p className="chart-sub">
                        Comparison of true generation against Random Forest predictions on holdout data
                      </p>
                    </div>
                  </div>
                  <div style={{ height: '340px' }}>
                    <Line
                      data={forecastChartData}
                      options={{
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                          x: {
                            grid: { color: 'rgba(255,255,255,0.05)' },
                            ticks: { color: '#9ca3af' },
                          },
                          y: {
                            grid: { color: 'rgba(255,255,255,0.05)' },
                            ticks: { color: '#9ca3af' },
                            title: { display: true, text: 'Energy (kWh)', color: '#f5c27f' },
                          },
                        },
                        plugins: {
                          legend: { labels: { color: '#f5f3ef' } },
                        },
                      }}
                    />
                  </div>
                </div>
              )}

              {/* Weather Scenario Predictions */}
              <div className="card">
                <h3 className="font-title" style={{ fontSize: '1.25rem', marginBottom: '0.5rem' }}>
                  Weather Scenario Projections
                </h3>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  Simulated generation under hypothetical meteorological conditions
                </p>

                <div className="scenarios-grid">
                  {forecastData.scenarios.map((sc) => (
                    <div key={sc.name} className="scenario-card">
                      <div className="scenario-name">
                        <Sun size={18} color="#e8a051" /> {sc.name}
                      </div>
                      <div className="scenario-val">
                        {sc.predicted_energy.toLocaleString()}
                        <span className="scenario-unit">kWh/day</span>
                      </div>
                      <div className="scenario-meta">
                        {Object.entries(sc.conditions).map(([k, v]) => (
                          <div key={k} style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <span>{k.replace(/_/g, ' ')}:</span>
                            <span style={{ color: '#fff' }}>{v}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </section>
      )}
    </div>
  );
}
