"""Tests for solstice.llm_integration module.

Real Groq calls are never made: tests either exercise pure logic (parsing,
duplicate resolution, keyword-sufficiency), exercise the full analyze() flow
through files whose standard column names resolve via Tier-1 keyword
matching alone, or mock analyzer.client directly for the Tier-2 LLM path.
"""

import json

import pandas as pd
import pytest
from unittest.mock import MagicMock

from solstice.llm_integration import FileAnalysis, LLMAnalyzer, get_prompt_for_manual_llm

STANDARD_INVERTER_DF = pd.DataFrame({
    "timestamp": pd.date_range("2024-01-01", periods=10, freq="D").astype(str),
    "energy": range(10),
})

# Column names with no English keyword overlap — forces Tier-1 to fail
# and fall through to LLM detection.
AMBIGUOUS_DF = pd.DataFrame({
    "Zeitpunkt": pd.date_range("2024-01-01", periods=10, freq="D").astype(str),
    "Wert": range(10),
})


def _write_csv(tmp_path, filename, df):
    path = str(tmp_path / filename)
    df.to_csv(path, index=False)
    return path


class TestIsKeywordSufficient:
    def test_sufficient_when_timestamp_and_data_field_mapped(self):
        analyzer = LLMAnalyzer(api_key=None)
        mapping = {"timestamp": "timestamp", "energy": "energy"}
        assert analyzer._is_keyword_sufficient(mapping, "inverter") is True

    def test_insufficient_when_file_type_unknown(self):
        analyzer = LLMAnalyzer(api_key=None)
        mapping = {"timestamp": "timestamp", "energy": "energy"}
        assert analyzer._is_keyword_sufficient(mapping, "unknown") is False

    def test_insufficient_without_timestamp(self):
        analyzer = LLMAnalyzer(api_key=None)
        mapping = {"energy": "energy"}
        assert analyzer._is_keyword_sufficient(mapping, "inverter") is False

    def test_insufficient_without_any_data_field(self):
        analyzer = LLMAnalyzer(api_key=None)
        mapping = {"timestamp": "timestamp"}
        assert analyzer._is_keyword_sufficient(mapping, "inverter") is False


class TestParseLLMResponse:
    def test_parses_plain_json(self):
        analyzer = LLMAnalyzer(api_key=None)
        text = json.dumps({"files": [{"filename": "a.csv"}]})
        result = analyzer._parse_llm_response(text, ["a.csv"], [{"row_count": 10}])
        assert len(result) == 1
        assert result[0].filename == "a.csv"
        assert result[0].detection_method == "llm"
        assert result[0].row_count == 10

    def test_parses_json_in_markdown_fence(self):
        analyzer = LLMAnalyzer(api_key=None)
        payload = json.dumps({"files": [{"filename": "b.csv", "confidence": "high"}]})
        text = f"Here you go:\n```json\n{payload}\n```"
        result = analyzer._parse_llm_response(text, ["b.csv"], [{"row_count": 5}])
        assert result[0].filename == "b.csv"
        assert result[0].confidence == "high"

    def test_raises_when_no_json_found(self):
        analyzer = LLMAnalyzer(api_key=None)
        with pytest.raises(ValueError, match="No JSON"):
            analyzer._parse_llm_response("not json at all", [], [])


class TestFixDuplicateMappings:
    def test_prefers_date_column_for_timestamp_field(self):
        analyzer = LLMAnalyzer(api_key=None)
        fa = FileAnalysis(
            filename="a.csv", filepath="a.csv", file_type="inverter", source_id="A",
            column_mapping={"Date": "timestamp", "Timestamp": "timestamp"},
            ignored_columns=[], confidence="high", notes="", row_count=10,
        )
        analyzer._fix_duplicate_mappings([fa])
        assert fa.column_mapping == {"Date": "timestamp"}
        assert "Timestamp" in fa.ignored_columns

    def test_prefers_value_or_energy_named_column_for_energy_field(self):
        analyzer = LLMAnalyzer(api_key=None)
        fa = FileAnalysis(
            filename="a.csv", filepath="a.csv", file_type="inverter", source_id="A",
            column_mapping={"Meter Reading": "energy", "Value (kWh)": "energy"},
            ignored_columns=[], confidence="high", notes="", row_count=10,
        )
        analyzer._fix_duplicate_mappings([fa])
        assert fa.column_mapping == {"Value (kWh)": "energy"}

    def test_leaves_single_mapping_untouched(self):
        analyzer = LLMAnalyzer(api_key=None)
        fa = FileAnalysis(
            filename="a.csv", filepath="a.csv", file_type="inverter", source_id="A",
            column_mapping={"Date": "timestamp", "kWh": "energy"},
            ignored_columns=[], confidence="high", notes="", row_count=10,
        )
        analyzer._fix_duplicate_mappings([fa])
        assert fa.column_mapping == {"Date": "timestamp", "kWh": "energy"}


class TestAnalyzeKeywordOnly:
    """Exercises the full analyze() flow WITHOUT touching Groq — standard
    column names resolve entirely via Tier-1 keyword matching."""

    def test_resolves_standard_file_without_api_key(self, tmp_path):
        path = _write_csv(tmp_path, "inv01.csv", STANDARD_INVERTER_DF)
        analyzer = LLMAnalyzer(api_key=None)
        analyzer.add_file(path)
        result = analyzer.analyze()

        assert len(result.files) == 1
        assert result.files[0].detection_method == "keyword"
        assert result.files[0].column_mapping.get("energy") == "energy"

    def test_create_aggregator_from_keyword_analysis(self, tmp_path):
        path = _write_csv(tmp_path, "inv01.csv", STANDARD_INVERTER_DF)
        analyzer = LLMAnalyzer(api_key=None)
        analyzer.add_file(path)
        analyzer.analyze()

        agg = analyzer.create_aggregator()
        df = agg.aggregate(freq="1D")
        assert "energy" in df.columns
        assert len(df) > 0

    def test_raises_when_ambiguous_file_has_no_api_key(self, tmp_path):
        path = _write_csv(tmp_path, "weird.csv", AMBIGUOUS_DF)
        analyzer = LLMAnalyzer(api_key=None)
        analyzer.add_file(path)
        with pytest.raises(ValueError, match="No API key"):
            analyzer.analyze()

    def test_get_analysis_summary_before_analyze(self):
        analyzer = LLMAnalyzer(api_key=None)
        assert "No analysis yet" in analyzer.get_analysis_summary()

    def test_create_aggregator_before_analyze_raises(self):
        analyzer = LLMAnalyzer(api_key=None)
        with pytest.raises(ValueError, match="No analysis"):
            analyzer.create_aggregator()


class TestLLMPath:
    """Mocks analyzer.client directly — no real Groq API traffic."""

    def test_falls_back_across_models_then_raises(self, tmp_path):
        path = _write_csv(tmp_path, "weird.csv", AMBIGUOUS_DF)
        analyzer = LLMAnalyzer(api_key="fake-key")
        analyzer.add_file(path)

        analyzer.client = MagicMock()
        analyzer.client.chat.completions.create.side_effect = Exception("API down")

        with pytest.raises(RuntimeError, match="All Groq models failed"):
            analyzer.analyze()
        assert analyzer.client.chat.completions.create.call_count == 3  # 3 models tried

    def test_uses_llm_mapping_for_ambiguous_file(self, tmp_path):
        path = _write_csv(tmp_path, "weird.csv", AMBIGUOUS_DF)
        analyzer = LLMAnalyzer(api_key="fake-key")
        analyzer.add_file(path)

        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = json.dumps({
            "analysis_summary": "test",
            "files": [{
                "filename": "weird.csv",
                "file_type": "inverter",
                "source_id": "WEIRD",
                "column_mapping": {"Zeitpunkt": "timestamp", "Wert": "energy"},
                "ignored_columns": [],
                "confidence": "high",
                "notes": "",
            }],
            "merge_strategy": "Concatenate by source_id",
            "warnings": [],
        })

        analyzer.client = MagicMock()
        analyzer.client.chat.completions.create.return_value = fake_response

        result = analyzer.analyze()
        assert result.files[0].detection_method == "llm"
        assert result.files[0].column_mapping == {"Zeitpunkt": "timestamp", "Wert": "energy"}

    def test_mixed_keyword_and_llm_files_in_one_analysis(self, tmp_path):
        standard_path = _write_csv(tmp_path, "std.csv", STANDARD_INVERTER_DF)
        ambiguous_path = _write_csv(tmp_path, "weird.csv", AMBIGUOUS_DF)

        analyzer = LLMAnalyzer(api_key="fake-key")
        analyzer.add_file(standard_path)
        analyzer.add_file(ambiguous_path)

        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = json.dumps({
            "files": [{
                "filename": "weird.csv",
                "file_type": "inverter",
                "source_id": "WEIRD",
                "column_mapping": {"Zeitpunkt": "timestamp", "Wert": "energy"},
                "ignored_columns": [],
                "confidence": "high",
            }],
        })
        analyzer.client = MagicMock()
        analyzer.client.chat.completions.create.return_value = fake_response

        result = analyzer.analyze()
        methods = {f.filename: f.detection_method for f in result.files}
        assert methods == {"std.csv": "keyword", "weird.csv": "llm"}
        # Order is preserved matching add_file() call order.
        assert [f.filename for f in result.files] == ["std.csv", "weird.csv"]


class TestGetPromptForManualLLM:
    def test_returns_prompt_containing_filename_and_schema(self, tmp_path):
        path = _write_csv(tmp_path, "a.csv", STANDARD_INVERTER_DF)
        prompt = get_prompt_for_manual_llm([path])
        assert "a.csv" in prompt
        assert "SCHEMA" in prompt
