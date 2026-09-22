// Split out from fileProcessing.js so the UI can reference these thresholds
// (e.g. to show a "will be pre-aggregated" badge) without pulling in the
// xlsx/papaparse dependencies that module needs.

// Below this, /api/detect gets the original file untouched.
export const DETECT_SAMPLE_THRESHOLD_BYTES = 1 * 1024 * 1024; // 1MB
export const DETECT_SAMPLE_ROWS = 50;

// Below this, /api/process gets the original file untouched. Vercel's cap is
// 4.5MB total for the whole multipart body (all files + fields combined),
// so we leave meaningful headroom per file.
export const PROCESS_PREAGG_THRESHOLD_BYTES = 3.5 * 1024 * 1024; // 3.5MB

export const FILE_PROCESSING_THRESHOLDS = {
  detectSampleBytes: DETECT_SAMPLE_THRESHOLD_BYTES,
  processPreAggBytes: PROCESS_PREAGG_THRESHOLD_BYTES,
};
