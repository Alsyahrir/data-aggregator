import os
import json
import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

from .llm_providers import LLMProvider, GroqProvider

DATA_FIELDS = {"energy", "ambient_temp", "irradiance", "wind_speed", "module_temp", "humidity"}


@dataclass
class FileAnalysis:
    filename: str
    filepath: str
    file_type: str
    source_id: str
    column_mapping: Dict[str, str]
    ignored_columns: List[str]
    confidence: str
    notes: str
    row_count: int
    detection_method: str = "llm"  # "keyword" or "llm"


@dataclass
class AnalysisResult:
    summary: str
    files: List[FileAnalysis]
    merge_strategy: str
    warnings: List[str]
    raw_response: str


class LLMAnalyzer:
    """
    Analyzes solar data files using a two-tier detection strategy:
      Tier 1 — Keyword pattern matching (fast, no API call)
      Tier 2 — LLM semantic analysis (fallback for ambiguous files)

    Tier 2 works with any LLM backend — pass a `provider=` (see
    solstice.llm_providers: GroqProvider, OpenAIProvider, AnthropicProvider,
    OpenAICompatibleProvider for anything else, or CustomProvider to wrap
    your own function). `api_key=` remains as a shorthand for the default
    Groq provider.

    Usage:
        from solstice.llm_providers import OpenAIProvider

        analyzer = LLMAnalyzer(provider=OpenAIProvider(api_key="sk-..."))
        # or, for the previous Groq-only shorthand:
        analyzer = LLMAnalyzer(api_key="your-groq-key")

        analyzer.add_file("data.xlsx")
        analyzer.analyze()
        agg = analyzer.create_aggregator()
        df = agg.aggregate(freq="1D")
    """

    def __init__(
        self,
        provider: Optional[LLMProvider] = None,
        api_key: Optional[str] = None,
        verbose: bool = False,
    ):
        self.verbose = verbose
        self.files_info: List[Dict] = []
        self.filepaths: List[str] = []
        self._df_samples: List[pd.DataFrame] = []
        self.analysis_result: Optional[AnalysisResult] = None

        if provider is not None:
            self.provider: Optional[LLMProvider] = provider
            self.api_key = api_key
        else:
            # Backward-compatible shorthand: api_key (or GROQ_API_KEY env
            # var) builds a default GroqProvider. Pass `provider=` directly
            # for any other backend — see solstice.llm_providers.
            self.api_key = api_key or os.environ.get('GROQ_API_KEY')
            self.provider = None
            if self.api_key:
                try:
                    self.provider = GroqProvider(api_key=self.api_key)
                except ImportError as e:
                    self._log(str(e))

    def _log(self, msg: str):
        if self.verbose:
            print(msg)

    def add_file(self, filepath: str) -> 'LLMAnalyzer':
        """Add a file for analysis."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        filename = os.path.basename(filepath)

        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, nrows=10)
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                row_count = sum(1 for _ in f) - 1
        else:
            df = pd.read_excel(filepath, nrows=10)
            row_count = len(pd.read_excel(filepath))

        columns_info = []
        for col in df.columns:
            samples = df[col].dropna().head(2).tolist()
            columns_info.append({
                "name": col,
                "dtype": str(df[col].dtype),
                "samples": [str(v)[:50] for v in samples]
            })

        self.files_info.append({
            "filename": filename,
            "row_count": row_count,
            "columns": columns_info
        })
        self.filepaths.append(filepath)
        self._df_samples.append(df)

        self._log(f"Scanned: {filename} ({row_count:,} rows, {len(df.columns)} columns)")
        return self

    # ------------------------------------------------------------------
    # Tier 1: keyword detection
    # ------------------------------------------------------------------

    def _try_keyword_detection(self, df: pd.DataFrame):
        """Run keyword-based column detection. Returns (mapping, file_type)."""
        from .detection import auto_detect_columns
        return auto_detect_columns(df)

    def _is_keyword_sufficient(self, mapping: Dict[str, str], file_type: str) -> bool:
        """
        Keyword result is sufficient if:
          - timestamp is mapped
          - at least one data field is mapped
          - file type was identified (not "unknown")
        If file_type is unknown, the column names are too non-standard for
        keyword matching and the file should fall through to LLM.
        """
        mapped_targets = set(mapping.values())
        return (
            "timestamp" in mapped_targets
            and bool(mapped_targets & DATA_FIELDS)
            and file_type != "unknown"
        )

    def _keyword_to_file_analysis(self, file_info: Dict, filepath: str,
                                   mapping: Dict[str, str], file_type: str) -> FileAnalysis:
        """Build a FileAnalysis from keyword detection results."""
        filename = file_info["filename"]
        # Strip source_id from keyword mapping — it is always derived from filename
        clean_mapping = {k: v for k, v in mapping.items() if v != "source_id"}
        all_columns = [c["name"] for c in file_info["columns"]]
        ignored = [c for c in all_columns if c not in clean_mapping]
        source_id = (
            filename.replace(".csv", "").replace(".xlsx", "")
                    .replace("_data", "").upper()
        )
        return FileAnalysis(
            filename=filename,
            filepath=filepath,
            file_type=file_type if file_type != "unknown" else "inverter",
            source_id=source_id,
            column_mapping=clean_mapping,
            ignored_columns=ignored,
            confidence="high",
            notes="Resolved by keyword pattern matching",
            row_count=file_info["row_count"],
            detection_method="keyword",
        )

    # ------------------------------------------------------------------
    # Tier 2: LLM detection
    # ------------------------------------------------------------------

    def _build_prompt(self, files_info: List[Dict]) -> str:
        return f"""You are a solar energy data expert.

Map these {len(files_info)} files to this schema:

SCHEMA:
- timestamp: Date/time (use "Date" column, NOT "Timestamp" if both exist)
- energy: Energy in kWh (look for "Value", "kwh", "energy", "Net Energy")
- ambient_temp, irradiance, wind_speed, module_temp, humidity (optional)

CRITICAL RULES:
1. Map each schema field to ONLY ONE source column
2. If both "Date" and "Timestamp" columns exist, ONLY use "Date" for timestamp
3. Map columns like "Value (Graph Scale...)" or "Net Energy" to energy
4. Ignore: "Timestamp" (time only), "Parameter", "Meter reading", "Device Name"

FILES:
{json.dumps(files_info, indent=2)}

RESPOND WITH ONLY THIS JSON:
{{
    "analysis_summary": "Brief overview",
    "files": [
        {{
            "filename": "file.xlsx",
            "file_type": "inverter",
            "source_id": "PANEL01",
            "column_mapping": {{
                "Date": "timestamp",
                "Value (Graph Scale : 1.000000 )": "energy"
            }},
            "ignored_columns": ["Timestamp", "Parameter", "Device Name", "Meter reading"],
            "confidence": "high",
            "notes": "Energy values in kWh"
        }}
    ],
    "merge_strategy": "Concatenate by source_id",
    "warnings": []
}}

REMEMBER: Only ONE column per schema field!"""

    def _call_llm(self, files_info: List[Dict]) -> str:
        """Send files_info to the configured LLM provider, return raw response text.

        Per-model retry/fallback (e.g. Groq's multiple free-tier models) is
        the provider's responsibility, not this method's — see
        solstice.llm_providers.OpenAICompatibleProvider.
        """
        prompt = self._build_prompt(files_info)
        system_prompt = (
            "You are a solar data expert. Respond only with valid JSON. "
            "Never map multiple columns to the same field."
        )
        return self.provider.complete(system_prompt, prompt)

    def _parse_llm_response(self, response_text: str, filepaths: List[str],
                             files_info: List[Dict]) -> List[FileAnalysis]:
        """Parse LLM JSON response into a list of FileAnalysis objects."""
        text = response_text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        start, end = text.find("{"), text.rfind("}") + 1
        if start == -1:
            raise ValueError(f"No JSON in LLM response: {response_text[:200]}")

        data = json.loads(text[start:end])
        results = []
        for i, f in enumerate(data.get("files", [])):
            filepath = filepaths[i] if i < len(filepaths) else ""
            info = files_info[i] if i < len(files_info) else {}
            results.append(FileAnalysis(
                filename=f.get("filename", ""),
                filepath=filepath,
                file_type=f.get("file_type", "unknown"),
                source_id=f.get("source_id", f"SOURCE_{i}"),
                column_mapping=f.get("column_mapping", {}),
                ignored_columns=f.get("ignored_columns", []),
                confidence=f.get("confidence", "medium"),
                notes=f.get("notes", ""),
                row_count=info.get("row_count", 0),
                detection_method="llm",
            ))
        return results

    def _fix_duplicate_mappings(self, file_analyses: List[FileAnalysis]):
        """Remove duplicate column→target mappings, keeping the best source."""
        for fa in file_analyses:
            mapping, ignored = fa.column_mapping, fa.ignored_columns
            target_sources: Dict[str, List[str]] = {}
            for src, tgt in mapping.items():
                target_sources.setdefault(tgt, []).append(src)

            for target, sources in target_sources.items():
                if len(sources) <= 1:
                    continue
                if target == "timestamp":
                    best = next((s for s in sources if s.lower() == "date"), sources[0])
                elif target == "energy":
                    best = next((s for s in sources if "value" in s.lower() or "energy" in s.lower()), sources[0])
                else:
                    best = sources[0]
                for s in sources:
                    if s != best:
                        del mapping[s]
                        if s not in ignored:
                            ignored.append(s)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def analyze(self) -> AnalysisResult:
        """
        Run two-tier analysis:
          1. Keyword pattern matching for each file
          2. LLM semantic analysis for files that keyword matching couldn't resolve
        """
        if not self.files_info:
            raise ValueError("No files added. Call add_file() first.")

        resolved: Dict[int, FileAnalysis] = {}
        llm_indices: List[int] = []

        # Tier 1: keyword detection
        for i, (file_info, filepath, df) in enumerate(
                zip(self.files_info, self.filepaths, self._df_samples)):
            mapping, file_type = self._try_keyword_detection(df)
            if self._is_keyword_sufficient(mapping, file_type):
                resolved[i] = self._keyword_to_file_analysis(file_info, filepath, mapping, file_type)
                self._log(f"[Tier 1] {file_info['filename']}: resolved by keyword matching")
            else:
                llm_indices.append(i)
                self._log(f"[Tier 2] {file_info['filename']}: ambiguous, sending to LLM")

        # Tier 2: LLM for ambiguous files
        if llm_indices:
            if self.provider is None:
                raise ValueError(
                    "No LLM provider configured. Pass provider=<LLMProvider instance> "
                    "(see solstice.llm_providers: GroqProvider, OpenAIProvider, "
                    "AnthropicProvider, OpenAICompatibleProvider, CustomProvider) "
                    "or api_key=... for the default Groq provider."
                )

            llm_files_info = [self.files_info[i] for i in llm_indices]
            llm_filepaths = [self.filepaths[i] for i in llm_indices]

            response_text = self._call_llm(llm_files_info)
            llm_results = self._parse_llm_response(response_text, llm_filepaths, llm_files_info)

            for idx, fa in zip(llm_indices, llm_results):
                resolved[idx] = fa

        # Assemble in original file order
        all_files = [resolved[i] for i in range(len(self.files_info))]
        self._fix_duplicate_mappings(all_files)

        keyword_count = sum(1 for fa in all_files if fa.detection_method == "keyword")
        llm_count = len(all_files) - keyword_count
        summary_parts = []
        if keyword_count:
            summary_parts.append(f"{keyword_count} file(s) via keyword matching")
        if llm_count:
            summary_parts.append(f"{llm_count} file(s) via LLM")
        summary = "Mapped " + ", ".join(summary_parts)

        self.analysis_result = AnalysisResult(
            summary=summary,
            files=all_files,
            merge_strategy="Concatenate by source_id",
            warnings=[],
            raw_response="",
        )
        return self.analysis_result

    def get_analysis_summary(self) -> str:
        """Return a formatted summary of the analysis results."""
        if not self.analysis_result:
            return "No analysis yet. Call analyze() first."

        r = self.analysis_result
        lines = [
            "", "=" * 70, "LLM ANALYSIS RESULTS", "=" * 70,
            "", f"Summary: {r.summary}", "", "FILES:", "-" * 50,
        ]

        for f in r.files:
            lines.append(f"\n{f.filename}")
            lines.append(f"   Detection: {f.detection_method.upper()}")
            lines.append(f"   Type: {f.file_type}")
            lines.append(f"   Source ID: {f.source_id}")
            lines.append(f"   Rows: {f.row_count:,}")
            lines.append(f"   Mappings:")
            for orig, schema in f.column_mapping.items():
                lines.append(f"      '{orig}' -> {schema}")
            if f.ignored_columns:
                lines.append(f"   Ignored: {', '.join(f.ignored_columns)}")

        lines.extend(["", "-" * 50, f"Merge Strategy: {r.merge_strategy}"])
        if r.warnings:
            lines.append("\nWARNINGS:")
            for w in r.warnings:
                lines.append(f"   - {w}")
        lines.append("=" * 70)

        return "\n".join(lines)

    def create_aggregator(self) -> 'Solstice':
        """Create a Solstice aggregator from the analysis results."""
        if not self.analysis_result:
            raise ValueError("No analysis. Call analyze() first.")

        from .aggregator import Solstice
        agg = Solstice(verbose=self.verbose)

        for f in self.analysis_result.files:
            agg.add_file(filepath=f.filepath, source_id=f.source_id, mapping=f.column_mapping)

        return agg


def analyze_and_aggregate(files: List[str], api_key: Optional[str] = None,
                           provider: Optional[LLMProvider] = None, freq: str = "1D",
                           output: Optional[str] = None) -> pd.DataFrame:
    """One-liner: analyze and aggregate.

    Pass either `api_key` (Groq shorthand) or `provider` (any LLMProvider —
    see solstice.llm_providers).
    """
    analyzer = LLMAnalyzer(provider=provider, api_key=api_key)
    for f in files:
        analyzer.add_file(f)
    analyzer.analyze()
    print(analyzer.get_analysis_summary())

    agg = analyzer.create_aggregator()
    df = agg.aggregate(freq=freq)
    if output:
        agg.save(output)
    print(agg.get_summary())
    return df


def get_prompt_for_manual_llm(files: List[str]) -> str:
    """Get the prompt that would be sent to the LLM, for manual copy-paste."""
    analyzer = LLMAnalyzer(api_key=None, verbose=False)
    for f in files:
        analyzer.add_file(f)
    return analyzer._build_prompt(analyzer.files_info)
