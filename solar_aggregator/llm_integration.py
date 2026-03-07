"""
Part 5: LLM Integration (Groq)

Setup:
1. Get API key from https://console.groq.com
2. pip install groq
3. Use LLMAnalyzer with your API key
"""

import os
import json
import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

GROQ_AVAILABLE = False
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    Groq = None


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


@dataclass
class AnalysisResult:
    summary: str
    files: List[FileAnalysis]
    merge_strategy: str
    warnings: List[str]
    raw_response: str


class LLMAnalyzer:
    """
    Analyzes solar data files using Groq LLM.
    
    Usage:
        analyzer = LLMAnalyzer(api_key="your-key")
        analyzer.add_file("data.xlsx")
        analyzer.analyze()
        agg = analyzer.create_aggregator()
        df = agg.aggregate(freq="1D")
    """
    
    def __init__(self, api_key: Optional[str] = None, verbose: bool = True):
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
        """Add file for analysis."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        filename = os.path.basename(filepath)
        self._log(f"\nScanning: {filename}")
        
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
        
        self._log(f"  Columns: {len(df.columns)}")
        self._log(f"  Rows: {row_count:,}")
        
        return self
    
    def _build_prompt(self) -> str:
        return f"""You are a solar energy data expert.

Map these {len(self.files_info)} files to this schema:

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
{json.dumps(self.files_info, indent=2)}

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
    
    def analyze(self) -> AnalysisResult:
        """Analyze files with Groq LLM."""
        if not self.files_info:
            raise ValueError("No files. Use add_file() first.")
        if not GROQ_AVAILABLE:
            raise ImportError("Run: pip install groq")
        if not self.api_key:
            raise ValueError("No API key. Get from https://console.groq.com")
        
        self._log("\n" + "=" * 60)
        self._log("ANALYZING WITH GROQ LLM")
        self._log("=" * 60)
        
        prompt = self._build_prompt()
        self._log(f"\nSending {len(self.files_info)} files to Groq...")
        
        models = ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768']
        response_text = None
        
        for model in models:
            try:
                self._log(f"Trying model: {model}...")
                response = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a solar data expert. Respond only with valid JSON. Never map multiple columns to the same field."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=2000,
                )
                response_text = response.choices[0].message.content
                self._log(f"Success with {model}")
                break
            except Exception as e:
                self._log(f"  {model}: {str(e)[:50]}")
        
        if response_text is None:
            raise RuntimeError("All models failed")
        
        self._log("Response received")
        self.analysis_result = self._parse_response(response_text)
        self._fix_duplicate_mappings()
        
        return self.analysis_result
    
    def _fix_duplicate_mappings(self):
        """Fix duplicate column mappings."""
        if not self.analysis_result:
            return
        
        for file_analysis in self.analysis_result.files:
            mapping = file_analysis.column_mapping
            ignored = file_analysis.ignored_columns
            
            # Find duplicates
            target_sources = {}
            for source, target in mapping.items():
                if target not in target_sources:
                    target_sources[target] = []
                target_sources[target].append(source)
            
            # Fix each duplicate
            for target, sources in target_sources.items():
                if len(sources) > 1:
                    self._log(f"\nFixing duplicate '{target}': {sources}")
                    
                    # Choose best source
                    if target == "timestamp":
                        best = next((s for s in sources if s.lower() == "date"), sources[0])
                    elif target == "energy":
                        best = next((s for s in sources if "value" in s.lower() or "energy" in s.lower()), sources[0])
                    else:
                        best = sources[0]
                    
                    # Remove others
                    for s in sources:
                        if s != best:
                            del mapping[s]
                            if s not in ignored:
                                ignored.append(s)
                            self._log(f"  Removed: '{s}'")
                    self._log(f"  Kept: '{best}'")
    
    def _parse_response(self, response_text: str) -> AnalysisResult:
        """Parse LLM JSON response."""
        text = response_text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1:
            raise ValueError(f"No JSON found: {response_text[:200]}")
        
        data = json.loads(text[start:end])
        
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
        """Get formatted summary."""
        if not self.analysis_result:
            return "No analysis yet. Call analyze() first."
        
        r = self.analysis_result
        lines = ["", "=" * 70, "LLM ANALYSIS RESULTS", "=" * 70, "", f"Summary: {r.summary}", "", "FILES:", "-" * 50]
        
        for f in r.files:
            lines.append(f"\n{f.filename}")
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
    
    def create_aggregator(self) -> 'SolarAggregator':
        """Create aggregator from LLM analysis."""
        if not self.analysis_result:
            raise ValueError("No analysis. Call analyze() first.")
        
        from .aggregator import SolarAggregator
        
        self._log("\n" + "=" * 60)
        self._log("CREATING AGGREGATOR FROM LLM ANALYSIS")
        self._log("=" * 60)
        
        agg = SolarAggregator(verbose=self.verbose)
        
        for f in self.analysis_result.files:
            self._log(f"\nAdding {f.filename} as {f.file_type}...")
            agg.add_file(filepath=f.filepath, source_id=f.source_id, mapping=f.column_mapping)
        
        return agg


def analyze_and_aggregate(files: List[str], api_key: str, freq: str = "1D", output: Optional[str] = None) -> pd.DataFrame:
    """One-liner: analyze and aggregate."""
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
    """Get prompt to copy-paste to ChatGPT/Claude."""
    analyzer = LLMAnalyzer(api_key=None, verbose=False)
    for f in files:
        analyzer.add_file(f)
    return analyzer._build_prompt()
