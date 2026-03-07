"""
Part 5: LLM Integration (Groq)

This module provides LLM-assisted analysis of solar data files.
It uses the Groq API (free) to help identify column mappings when
auto-detection isn't enough.

The LLM only handles schema detection. All actual data processing
is done by the library's processing functions.

Setup:
1. Get API key from https://console.groq.com
2. Install: pip install groq
3. Use LLMAnalyzer with your API key
"""

import os
import json
import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Check if groq is installed
GROQ_AVAILABLE = False
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    Groq = None


@dataclass
class FileAnalysis:
    """Analysis result for a single file."""
    filename: str
    filepath: str
    file_type: str
    source_id: str
    column_mapping: Dict[str, str]
    ignored_columns: List[str]
    confidence: str
    notes: str
    row_count: int


@dataclass
class AnalysisResult:
    """Complete analysis result from LLM."""
    summary: str
    files: List[FileAnalysis]
    merge_strategy: str
    warnings: List[str]
    raw_response: str


class LLMAnalyzer:
    """
    Analyzes solar data files using the Groq LLM.
    
    This class scans files, sends metadata to the LLM, and parses
    the response to create column mappings.
    
    Basic usage:
        analyzer = LLMAnalyzer(api_key="your-groq-key")
        analyzer.add_file("inverter1.csv")
        analyzer.add_file("weather.csv")
        result = analyzer.analyze()
        print(analyzer.get_analysis_summary())
        agg = analyzer.create_aggregator()
        df = agg.aggregate(freq="1D")
    """
    
    def __init__(self, api_key: Optional[str] = None, verbose: bool = True):
        """
        Initialize the analyzer.
        
        Get your API key from https://console.groq.com
        You can also set the GROQ_API_KEY environment variable.
        """
        self.verbose = verbose
        self.files_info: List[Dict] = []
        self.filepaths: List[str] = []
        self.analysis_result: Optional[AnalysisResult] = None
        
        self.api_key = api_key or os.environ.get('GROQ_API_KEY')
        
        if not GROQ_AVAILABLE:
            self._log("Groq not installed. Run: pip install groq")
        elif self.api_key:
            self.client = Groq(api_key=self.api_key)
            self._log("Groq API configured")
    
    def _log(self, msg: str):
        if self.verbose:
            print(msg)
    
    def add_file(self, filepath: str) -> 'LLMAnalyzer':
        """
        Add a file for analysis.
        
        Returns self for method chaining.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        filename = os.path.basename(filepath)
        self._log(f"\nScanning: {filename}")
        
        # Load sample data
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, nrows=10)
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                row_count = sum(1 for _ in f) - 1
        else:
            df = pd.read_excel(filepath, nrows=10)
            row_count = len(pd.read_excel(filepath))
        
        # Collect column info
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
        
        self._log(f"  Columns: {len(df.columns)}")
        self._log(f"  Rows: {row_count:,}")
        
        return self
    
    def _build_prompt(self) -> str:
        """Build the analysis prompt for the LLM."""
        prompt = f"""You are a solar energy data engineering expert.

I have {len(self.files_info)} data files that need to be mapped to a standard schema.
Analyze each file and tell me how the columns map to the schema.

STANDARD SCHEMA:

REQUIRED:
- timestamp: Date/time of measurement (look for: measured_on, datetime, time, date)
- energy: Energy in kWh (look for: kwh, energy, output, generation, value)
  Important: Must contain energy values, not voltage or power_factor

OPTIONAL:
- ambient_temp: Air temperature in C (must have "ambient", not inverter temp)
- irradiance: Solar radiation GHI in W/m2 (look for: ghi, irradiance, not dni)
- wind_speed: Wind speed (look for: wind, speed)
- module_temp: Panel temperature (look for: module, panel, cell temp)
- humidity: Relative humidity (look for: humidity, rh)

CRITICAL RULES:
1. Each schema field can only be mapped ONCE per file
2. Do NOT map multiple source columns to the same schema field
3. If there are separate Date and Time columns, only use the Date column for timestamp
4. If a column contains energy values (like "Value" or "Net Energy"), map it to energy

FILES TO ANALYZE:

{json.dumps(self.files_info, indent=2)}

For each file, determine:
1. File type: inverter, environment, irradiance, or mixed
2. Column mapping: which columns map to which schema fields (ONE-TO-ONE only!)
3. Source ID: a short identifier for this data source
4. Ignored columns: columns that don't map to the schema

RESPOND WITH ONLY THIS JSON:

{{
    "analysis_summary": "Brief overview of what you found",
    "files": [
        {{
            "filename": "example.csv",
            "file_type": "inverter|environment|irradiance",
            "source_id": "SHORT_ID",
            "column_mapping": {{
                "original_column_name": "schema_field_name"
            }},
            "ignored_columns": ["col1", "col2"],
            "confidence": "high|medium|low",
            "notes": "Any observations about this file"
        }}
    ],
    "merge_strategy": "How these files should be combined",
    "warnings": ["Any potential issues"]
}}

REMEMBER: Never map two columns to the same schema field! Each mapping must be unique.
"""
        return prompt
    
    def analyze(self) -> AnalysisResult:
        """
        Analyze all files using the Groq LLM.
        
        Returns an AnalysisResult with file mappings.
        """
        if not self.files_info:
            raise ValueError("No files added. Use add_file() first.")
        
        if not GROQ_AVAILABLE:
            raise ImportError("Groq not installed. Run: pip install groq")
        
        if not self.api_key:
            raise ValueError("No API key. Get one from https://console.groq.com")
        
        self._log("\n" + "=" * 60)
        self._log("ANALYZING WITH GROQ LLM")
        self._log("=" * 60)
        
        prompt = self._build_prompt()
        self._log(f"\nSending {len(self.files_info)} files to Groq...")
        
        try:
            # Models to try
            model_names = [
                'llama-3.3-70b-versatile',
                'llama-3.1-8b-instant',
                'mixtral-8x7b-32768',
                'gemma2-9b-it',
            ]
            
            response_text = None
            last_error = None
            
            for model_name in model_names:
                try:
                    self._log(f"Trying model: {model_name}...")
                    
                    response = self.client.chat.completions.create(
                        model=model_name,
                        messages=[
                            {
                                "role": "system",
                                "content": "You are a solar energy data engineering expert. Respond only with valid JSON. Never map multiple columns to the same schema field."
                            },
                            {
                                "role": "user", 
                                "content": prompt
                            }
                        ],
                        temperature=0.1,
                        max_tokens=2000,
                    )
                    
                    response_text = response.choices[0].message.content
                    self._log(f"Success with {model_name}")
                    break
                    
                except Exception as e:
                    last_error = e
                    self._log(f"  {model_name}: {str(e)[:50]}")
                    continue
            
            if response_text is None:
                raise RuntimeError(f"No working model found. Last error: {last_error}")
            
            self._log("Response received")
            
        except Exception as e:
            raise RuntimeError(f"Groq API error: {e}")
        
        # Parse response
        self.analysis_result = self._parse_response(response_text)
        
        # Validate and fix duplicate mappings
        self._fix_duplicate_mappings()
        
        return self.analysis_result
    
    def _fix_duplicate_mappings(self):
        """Fix any duplicate mappings in the analysis result."""
        if not self.analysis_result:
            return
        
        for file_analysis in self.analysis_result.files:
            mapping = file_analysis.column_mapping
            
            # Find duplicates (multiple columns mapped to same field)
            target_counts = {}
            for source, target in mapping.items():
                target_counts[target] = target_counts.get(target, 0) + 1
            
            duplicates = {t for t, c in target_counts.items() if c > 1}
            
            if duplicates:
                self._log(f"\nFixing duplicate mappings in {file_analysis.filename}:")
                
                for dup_target in duplicates:
                    # Find all sources mapping to this target
                    sources = [s for s, t in mapping.items() if t == dup_target]
                    self._log(f"  {dup_target}: {sources}")
                    
                    # Keep only the best one (prefer "Date" for timestamp, actual value columns for energy)
                    if dup_target == "timestamp":
                        # Prefer "Date" over "Timestamp" or "Time"
                        best = None
                        for s in sources:
                            if "date" in s.lower() and "time" not in s.lower():
                                best = s
                                break
                        if not best:
                            best = sources[0]
                    else:
                        best = sources[0]
                    
                    # Remove all except the best
                    for s in sources:
                        if s != best:
                            del mapping[s]
                            file_analysis.ignored_columns.append(s)
                            self._log(f"    Removed: {s} -> {dup_target}")
                    
                    self._log(f"    Kept: {best} -> {dup_target}")
    
    def _parse_response(self, response_text: str) -> AnalysisResult:
        """Parse the LLM's JSON response."""
        text = response_text.strip()
        
        # Remove markdown code blocks
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        # Find JSON
        start = text.find("{")
        end = text.rfind("}") + 1
        
        if start == -1:
            raise ValueError(f"No JSON in response: {response_text[:500]}")
        
        json_str = text[start:end]
        
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}\n{json_str[:500]}")
        
        # Build result
        files = []
        for i, f in enumerate(data.get("files", [])):
            filepath = self.filepaths[i] if i < len(self.filepaths) else ""
            files.append(FileAnalysis(
                filename=f.get("filename", ""),
                filepath=filepath,
                file_type=f.get("file_type", "unknown"),
                source_id=f.get("source_id", f"SOURCE_{i}"),
                column_mapping=f.get("column_mapping", {}),
                ignored_columns=f.get("ignored_columns", []),
                confidence=f.get("confidence", "medium"),
                notes=f.get("notes", ""),
                row_count=self.files_info[i]["row_count"] if i < len(self.files_info) else 0
            ))
        
        return AnalysisResult(
            summary=data.get("analysis_summary", ""),
            files=files,
            merge_strategy=data.get("merge_strategy", ""),
            warnings=data.get("warnings", []),
            raw_response=response_text
        )
    
    def get_analysis_summary(self) -> str:
        """Get a formatted summary of the analysis."""
        if not self.analysis_result:
            return "No analysis performed yet. Call analyze() first."
        
        r = self.analysis_result
        
        lines = [
            "",
            "=" * 70,
            "LLM ANALYSIS RESULTS",
            "=" * 70,
            "",
            f"Summary: {r.summary}",
            "",
            "FILES:",
            "-" * 50
        ]
        
        for f in r.files:
            lines.append(f"\n{f.filename}")
            lines.append(f"   Type: {f.file_type}")
            lines.append(f"   Source ID: {f.source_id}")
            lines.append(f"   Rows: {f.row_count:,}")
            lines.append(f"   Confidence: {f.confidence}")
            lines.append(f"   Mappings:")
            for orig, schema in f.column_mapping.items():
                lines.append(f"      '{orig}' -> {schema}")
            if f.ignored_columns:
                lines.append(f"   Ignored: {', '.join(f.ignored_columns)}")
            if f.notes:
                lines.append(f"   Notes: {f.notes}")
        
        lines.append("")
        lines.append("-" * 50)
        lines.append(f"Merge Strategy: {r.merge_strategy}")
        
        if r.warnings:
            lines.append("")
            lines.append("WARNINGS:")
            for w in r.warnings:
                lines.append(f"   - {w}")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)
    
    def create_aggregator(self) -> 'SolarAggregator':
        """
        Create a SolarAggregator configured with the LLM's mappings.
        
        Returns a ready-to-use aggregator.
        """
        if not self.analysis_result:
            raise ValueError("No analysis. Call analyze() first.")
        
        from .aggregator import SolarAggregator
        
        self._log("\n" + "=" * 60)
        self._log("CREATING AGGREGATOR FROM LLM ANALYSIS")
        self._log("=" * 60)
        
        agg = SolarAggregator(verbose=self.verbose)
        
        for f in self.analysis_result.files:
            self._log(f"\nAdding {f.filename} as {f.file_type}...")
            agg.add_file(
                filepath=f.filepath,
                source_id=f.source_id,
                mapping=f.column_mapping
            )
        
        return agg


def analyze_and_aggregate(
    files: List[str],
    api_key: str,
    freq: str = "1D",
    output: Optional[str] = None
) -> pd.DataFrame:
    """
    One-liner to analyze files with LLM and aggregate.
    
    Example:
        df = analyze_and_aggregate(
            files=["inv1.csv", "weather.csv"],
            api_key="your-key",
            freq="1D",
            output="daily.csv"
        )
    """
    analyzer = LLMAnalyzer(api_key=api_key)
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
    """
    Get a prompt to manually copy-paste to ChatGPT or Claude.
    
    Use this if you don't have an API key.
    """
    analyzer = LLMAnalyzer(api_key=None, verbose=False)
    for f in files:
        analyzer.add_file(f)
    return analyzer._build_prompt()