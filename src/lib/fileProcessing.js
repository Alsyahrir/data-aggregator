'use client';

/**
 * Client-side CSV/Excel handling that keeps large uploads under Vercel's
 * ~4.5MB serverless request-body limit, which the API itself cannot raise.
 *
 * Two independent tricks:
 *
 *  1. sampleFileForDetection — /api/detect only ever needs a handful of
 *     sample rows to guess column mappings, so instead of uploading a whole
 *     multi-MB file just to sniff its header, we read the first N rows in
 *     the browser and upload only those. Applies regardless of file size
 *     (cheap even for small files) and completely removes /api/detect from
 *     the size-limit problem.
 *
 *  2. prepareFileForProcessing — for the real aggregation upload, files
 *     under the threshold are sent unchanged (byte-for-byte, same as
 *     before). Files over the threshold are parsed fully in the browser,
 *     grouped by the user's confirmed column mapping and chosen frequency,
 *     and summed/averaged per solstice/schema.py's own aggregation rules
 *     (energy=SUM, everything else=MEAN) — the exact same rule the server
 *     would apply. The trade-off: multi-source timestamp alignment at fine
 *     (15min) resolution is lost for these files, since they're already
 *     collapsed to the target frequency before the server ever sees them.
 */

import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import {
  DETECT_SAMPLE_THRESHOLD_BYTES,
  DETECT_SAMPLE_ROWS,
  PROCESS_PREAGG_THRESHOLD_BYTES,
} from './fileProcessingConstants.js';

// Mirrors solstice/schema.py's get_aggregation_rules().
const SUM_FIELDS = new Set(['energy']);
const MEAN_FIELDS = new Set([
  'ambient_temp', 'irradiance', 'wind_speed', 'module_temp',
  'humidity', 'power', 'voltage', 'current',
]);

function isExcel(filename) {
  return /\.xlsx?$/i.test(filename || '');
}

/**
 * Parse a date/time string into components, day-first when ambiguous
 * (matching solstice/processing.py's pd.to_datetime(..., dayfirst=True)).
 * Returns null if unparseable.
 */
function parseTimestampComponents(raw) {
  if (raw === null || raw === undefined) return null;
  const str = String(raw).trim();
  if (!str) return null;

  // ISO-like: YYYY-MM-DD[ T]HH:mm[:ss] — unambiguous, checked first.
  let m = str.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if (m) {
    return {
      y: +m[1], mo: +m[2], d: +m[3],
      h: m[4] ? +m[4] : 0, mi: m[5] ? +m[5] : 0, s: m[6] ? +m[6] : 0,
    };
  }

  // Day-first: D/M/YYYY or D-M-YYYY [HH:mm[:ss]]
  m = str.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if (m) {
    return {
      y: +m[3], mo: +m[2], d: +m[1],
      h: m[4] ? +m[4] : 0, mi: m[5] ? +m[5] : 0, s: m[6] ? +m[6] : 0,
    };
  }

  // Last resort: let the browser's own parser take a shot.
  const fallback = new Date(str);
  if (!Number.isNaN(fallback.getTime())) {
    return {
      y: fallback.getFullYear(), mo: fallback.getMonth() + 1, d: fallback.getDate(),
      h: fallback.getHours(), mi: fallback.getMinutes(), s: fallback.getSeconds(),
    };
  }
  return null;
}

/** Floor timestamp components to the start of their bucket for `freq`. */
function floorComponents(c, freq) {
  switch (freq) {
    case '1h':
      return { y: c.y, mo: c.mo, d: c.d, h: c.h, mi: 0, s: 0 };
    case '1W': {
      // Monday-anchored week.
      const dt = new Date(c.y, c.mo - 1, c.d);
      const dow = (dt.getDay() + 6) % 7; // 0 = Monday
      dt.setDate(dt.getDate() - dow);
      return { y: dt.getFullYear(), mo: dt.getMonth() + 1, d: dt.getDate(), h: 0, mi: 0, s: 0 };
    }
    case '1M':
      return { y: c.y, mo: c.mo, d: 1, h: 0, mi: 0, s: 0 };
    case '1D':
    default:
      return { y: c.y, mo: c.mo, d: c.d, h: 0, mi: 0, s: 0 };
  }
}

function componentsToISOString(c) {
  const pad = (n, len = 2) => String(n).padStart(len, '0');
  return `${pad(c.y, 4)}-${pad(c.mo)}-${pad(c.d)}T${pad(c.h)}:${pad(c.mi)}:${pad(c.s)}`;
}

/**
 * Parse a CSV or Excel File into { fields, rows }. `maxRows`, when given,
 * limits how many data rows are read — both parsers stop early rather than
 * reading the whole file, so sampling a huge file stays fast.
 */
function parseFile(file, maxRows = null) {
  if (isExcel(file.name)) {
    return file.arrayBuffer().then((buf) => {
      const readOpts = maxRows ? { sheetRows: maxRows + 1 } : {};
      const wb = XLSX.read(buf, { type: 'array', cellDates: false, ...readOpts });
      const sheet = wb.Sheets[wb.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json(sheet, { defval: null, raw: true });
      const fields = rows.length ? Object.keys(rows[0]) : [];
      return { fields, rows: maxRows ? rows.slice(0, maxRows) : rows };
    });
  }

  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      worker: !maxRows, // full parses (potentially huge) run off the main thread
      preview: maxRows || 0,
      complete: (res) => resolve({ fields: res.meta.fields || [], rows: res.data }),
      error: reject,
    });
  });
}

/**
 * Returns a small File (header + first N rows) safe to upload for
 * /api/detect regardless of how large the source file is. Files already
 * under the threshold pass through unchanged.
 */
export async function sampleFileForDetection(file) {
  if (file.size <= DETECT_SAMPLE_THRESHOLD_BYTES) return file;

  const { fields, rows } = await parseFile(file, DETECT_SAMPLE_ROWS);
  const csv = Papa.unparse({ fields, data: rows });
  return new File([csv], file.name, { type: 'text/csv' });
}

/**
 * For files over the threshold: parse fully, group by the confirmed
 * mapping's source_id/timestamp columns floored to `freq`, and sum/average
 * per schema rules. Returns a compact CSV File plus an identity mapping
 * (columns are already renamed to schema target names). Files under the
 * threshold pass through with their original mapping, unchanged.
 */
export async function prepareFileForProcessing(file, mapping, freq) {
  if (file.size <= PROCESS_PREAGG_THRESHOLD_BYTES) {
    return { file, mapping, preAggregated: false };
  }

  const { rows } = await parseFile(file);

  const timestampSrcCol = Object.keys(mapping).find((k) => mapping[k] === 'timestamp');
  if (!timestampSrcCol) {
    // No timestamp mapping to bucket by — fall back to the raw file and let
    // the server produce its usual, clearer validation error.
    return { file, mapping, preAggregated: false };
  }
  const sourceIdSrcCol = Object.keys(mapping).find((k) => mapping[k] === 'source_id');
  const targetFields = [...new Set(
    Object.values(mapping).filter((t) => t !== 'timestamp' && t !== 'source_id')
  )].filter((t) => SUM_FIELDS.has(t) || MEAN_FIELDS.has(t));

  const buckets = new Map();

  for (const row of rows) {
    const comps = parseTimestampComponents(row[timestampSrcCol]);
    if (!comps) continue;
    const floored = floorComponents(comps, freq);
    const sourceId = sourceIdSrcCol ? String(row[sourceIdSrcCol] ?? '') : null;
    const key = sourceIdSrcCol ? `${sourceId}::${componentsToISOString(floored)}` : componentsToISOString(floored);

    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = { timestamp: componentsToISOString(floored), sourceId, sums: {}, counts: {} };
      buckets.set(key, bucket);
    }

    for (const [srcCol, target] of Object.entries(mapping)) {
      if (!targetFields.includes(target)) continue;
      const raw = row[srcCol];
      const num = typeof raw === 'number' ? raw : parseFloat(raw);
      if (Number.isNaN(num)) continue;
      bucket.sums[target] = (bucket.sums[target] || 0) + num;
      bucket.counts[target] = (bucket.counts[target] || 0) + 1;
    }
  }

  const outRows = [...buckets.values()]
    .map((bucket) => {
      const row = { timestamp: bucket.timestamp };
      if (sourceIdSrcCol) row.source_id = bucket.sourceId;
      for (const field of targetFields) {
        if (!(field in bucket.sums)) continue;
        row[field] = SUM_FIELDS.has(field) ? bucket.sums[field] : bucket.sums[field] / bucket.counts[field];
      }
      return row;
    })
    .sort((a, b) => (a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0));

  const fields = sourceIdSrcCol ? ['timestamp', 'source_id', ...targetFields] : ['timestamp', ...targetFields];
  const csv = Papa.unparse({ fields, data: outRows });
  const compactFile = new File([csv], file.name.replace(/\.(xlsx?|csv)$/i, '.csv'), { type: 'text/csv' });

  const identityMapping = {};
  fields.forEach((f) => { identityMapping[f] = f; });

  return { file: compactFile, mapping: identityMapping, preAggregated: true, originalRows: rows.length, aggregatedRows: outRows.length };
}
