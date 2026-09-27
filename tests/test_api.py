"""Tests for the Vercel serverless backend (api/index.py).

This file covers the deployed HTTP surface, which until now had none — every
bug in it was found by hand with curl. No real network calls are made: the
LLM providers are mocked and the weather fetch is patched.

The platform limits these endpoints defend against (Vercel's 4.5MB response
cap, the function timeout) can't be reproduced locally, so the tests assert
on the behaviour that keeps us under them rather than on the limits.
"""

import io
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.index import (
    MAX_RESPONSE_RECORD_BYTES,
    app,
    build_provider,
    records_within_budget,
    sanitize_val,
)

client = TestClient(app)


def _csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _upload(df: pd.DataFrame, name: str = "inv01.csv"):
    return ("files", (name, io.BytesIO(_csv_bytes(df)), "text/csv"))


STANDARD = pd.DataFrame({
    "timestamp": pd.date_range("2024-01-01", periods=48, freq="h").astype(str),
    "energy": range(48),
})

# No English keywords, so Tier-1 detection fails and the LLM path is reached.
AMBIGUOUS = pd.DataFrame({
    "Zeitpunkt": pd.date_range("2024-01-01", periods=10, freq="D").astype(str),
    "Wert": range(10),
})


class TestHealthAndSchema:
    def test_health_reports_ok(self):
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_schema_lists_required_fields(self):
        r = client.get("/api/schema")
        assert r.status_code == 200
        names = {f["name"] for f in r.json()["fields"]}
        assert {"timestamp", "energy", "irradiance"} <= names

    def test_llm_providers_are_listed(self):
        r = client.get("/api/llm-providers")
        assert r.status_code == 200
        ids = {p["id"] for p in r.json()["providers"]}
        assert ids == {"groq", "openai", "anthropic", "compatible"}

    def test_compatible_provider_declares_its_extra_fields(self):
        r = client.get("/api/llm-providers")
        compat = next(p for p in r.json()["providers"] if p["id"] == "compatible")
        assert compat["needs_base_url"] is True
        assert compat["needs_model"] is True


class TestSanitizeVal:
    def test_bools_stay_bools(self):
        """bool is a subclass of int, so an int check placed first silently
        turned every True/False into 1/0 in responses and exports."""
        assert sanitize_val(True) is True
        assert sanitize_val(False) is False

    def test_numpy_bools_stay_bools(self):
        import numpy as np
        assert sanitize_val(np.bool_(True)) is True

    def test_ints_and_floats(self):
        import numpy as np
        assert sanitize_val(np.int64(7)) == 7
        assert sanitize_val(1.23456789) == pytest.approx(1.2346, abs=1e-4)

    def test_nan_and_inf_become_none(self):
        import numpy as np
        assert sanitize_val(np.nan) is None
        assert sanitize_val(np.inf) is None


class TestRecordsWithinBudget:
    def test_small_frame_is_not_truncated(self):
        records, truncated = records_within_budget(STANDARD)
        assert truncated is False
        assert len(records) == len(STANDARD)

    def test_empty_frame(self):
        records, truncated = records_within_budget(pd.DataFrame())
        assert records == []
        assert truncated is False

    def test_large_frame_is_trimmed_to_fit(self):
        big = pd.DataFrame({
            "timestamp": pd.date_range("2020-01-01", periods=60_000, freq="h").astype(str),
            "source_id": ["SOURCE_WITH_A_LONGISH_NAME"] * 60_000,
            "energy": [1.23456] * 60_000,
        })
        records, truncated = records_within_budget(big)
        assert truncated is True
        assert 0 < len(records) < len(big)
        payload = json.dumps(records).encode("utf-8")
        assert len(payload) <= MAX_RESPONSE_RECORD_BYTES * 1.1

    def test_respects_an_explicit_budget(self):
        records, truncated = records_within_budget(STANDARD, budget_bytes=200)
        assert truncated is True
        assert len(records) >= 1


class TestDetect:
    def test_detects_standard_columns(self):
        r = client.post("/api/detect", files=[_upload(STANDARD)])
        assert r.status_code == 200
        f = r.json()["files"][0]
        assert f["file_type"] == "inverter"
        assert f["detection_method"] == "keyword"
        assert f["mapping"]["energy"] == "energy"

    def test_rejects_unsupported_extension(self):
        r = client.post(
            "/api/detect",
            files=[("files", ("photo.png", io.BytesIO(b"\x89PNG\r\n"), "image/png"))],
        )
        assert r.status_code == 400
        assert "not a supported file type" in r.json()["detail"]

    def test_header_only_file_is_reported_not_dropped(self):
        """It used to vanish from the results with a 200 and no explanation."""
        r = client.post(
            "/api/detect",
            files=[("files", ("empty.csv", io.BytesIO(b"timestamp,energy\n"), "text/csv"))],
        )
        assert r.status_code == 200
        body = r.json()
        assert body["files"] == []
        assert body["skipped"][0]["filename"] == "empty.csv"

    def test_unreadable_file_gets_an_actionable_message(self):
        r = client.post(
            "/api/detect",
            files=[("files", ("data.csv", io.BytesIO(b"\xff\xfe\x00broken"), "text/csv"))],
        )
        assert r.status_code == 400
        assert "utf-8" not in r.json()["detail"].lower() or "UTF-8" in r.json()["detail"]

    def test_llm_failure_is_reported_per_file(self):
        """A rejected key used to hit `except: pass`, so the feature looked
        broken rather than misconfigured."""
        with patch("api.index.build_provider") as mk:
            mk.return_value.complete.side_effect = Exception("401 invalid_api_key")
            r = client.post(
                "/api/detect",
                files=[_upload(AMBIGUOUS, "weird.csv")],
                data={"llm_api_key": "bad", "llm_provider": "groq"},
            )
        assert r.status_code == 200
        assert "key was rejected" in r.json()["files"][0]["llm_error"]

    def test_llm_mapping_is_used_when_it_succeeds(self):
        payload = json.dumps({"files": [{
            "filename": "weird.csv", "file_type": "inverter", "source_id": "WEIRD",
            "column_mapping": {"Zeitpunkt": "timestamp", "Wert": "energy"},
            "ignored_columns": [], "confidence": "high", "notes": "",
        }]})
        with patch("api.index.build_provider") as mk:
            mk.return_value.complete.return_value = payload
            r = client.post(
                "/api/detect",
                files=[_upload(AMBIGUOUS, "weird.csv")],
                data={"llm_api_key": "k", "llm_provider": "groq"},
            )
        f = r.json()["files"][0]
        assert f["detection_method"] == "llm"
        assert f["mapping"] == {"Zeitpunkt": "timestamp", "Wert": "energy"}

    def test_keyword_path_never_calls_the_provider(self):
        with patch("api.index.build_provider") as mk:
            client.post(
                "/api/detect",
                files=[_upload(STANDARD)],
                data={"llm_api_key": "k", "llm_provider": "groq"},
            )
        mk.assert_not_called()


class TestBuildProvider:
    def test_unknown_provider_is_a_400(self):
        with pytest.raises(Exception) as exc:
            build_provider("bogus", "k")
        assert "Unknown LLM provider" in str(exc.value.detail)

    def test_compatible_requires_base_url(self):
        with pytest.raises(Exception) as exc:
            build_provider("compatible", "k", base_url=None, model="m")
        assert "base URL" in str(exc.value.detail)

    def test_compatible_requires_model(self):
        with pytest.raises(Exception) as exc:
            build_provider("compatible", "k", base_url="http://x/v1", model=None)
        assert "model" in str(exc.value.detail)

    def test_builds_groq_by_default(self):
        with patch("openai.OpenAI"):
            from solstice.llm_providers import GroqProvider
            assert isinstance(build_provider("groq", "gsk_x"), GroqProvider)


class TestProcess:
    def _post(self, df=STANDARD, name="inv01.csv", **extra):
        data = {
            "mappings_json": json.dumps({name: {"timestamp": "timestamp", "energy": "energy"}}),
            "freq": "1D",
            "anomaly_enabled": "false",
        }
        data.update(extra)
        return client.post("/api/process", files=[_upload(df, name)], data=data)

    def test_aggregates_and_summarises(self):
        r = self._post()
        assert r.status_code == 200
        body = r.json()
        assert body["summary"]["total_rows"] == 2
        assert body["summary"]["total_energy"] == pytest.approx(sum(range(48)))
        assert body["summary"]["truncated"] is False

    def test_reports_returned_rows(self):
        body = self._post().json()
        assert body["summary"]["returned_rows"] == len(body["records"])

    def test_invalid_mappings_json_is_a_400(self):
        r = client.post(
            "/api/process",
            files=[_upload(STANDARD)],
            data={"mappings_json": "not json", "freq": "1D"},
        )
        assert r.status_code == 400
        assert "Invalid mappings JSON" in r.json()["detail"]

    def test_rejects_unsupported_extension(self):
        r = client.post(
            "/api/process",
            files=[("files", ("x.png", io.BytesIO(b"\x89PNG"), "image/png"))],
            data={"mappings_json": "{}", "freq": "1D"},
        )
        assert r.status_code == 400

    def test_anomaly_flags_serialise_as_booleans(self):
        spiky = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=40, freq="D").astype(str),
            "energy": [1.0] * 39 + [999.0],
        })
        body = self._post(spiky, anomaly_enabled="true").json()
        assert any(isinstance(rec.get("is_anomaly"), bool) for rec in body["records"])

    def test_source_id_comes_from_the_shared_helper(self):
        """Library and API must agree, or the same file gets two identities."""
        from solstice.processing import derive_source_id
        body = self._post(name="PLANT_A.XLSX" if False else "plant_a_data.csv").json()
        assert body["records"][0]["source_id"] == derive_source_id("plant_a_data.csv")


class TestEnrichWeather:
    BASE = {"records": [{"timestamp": "2024-01-01", "energy": 1.0}]}

    @pytest.mark.parametrize(
        "lat,lon,field",
        [
            (999, 0, "latitude"),
            (-91, 0, "latitude"),
            ("abc", 0, "latitude"),
            (0, -500, "longitude"),
            (0, 181, "longitude"),
        ],
    )
    def test_bad_coordinates_are_400_not_500(self, lat, lon, field):
        """These used to surface as a bare 500 with no body, or an opaque
        upstream error from Open-Meteo."""
        r = client.post("/api/enrich-weather", json={**self.BASE, "latitude": lat, "longitude": lon})
        assert r.status_code == 400
        assert field in r.json()["detail"]

    def test_missing_records_is_a_400(self):
        r = client.post("/api/enrich-weather", json={"records": [], "latitude": 1, "longitude": 1})
        assert r.status_code == 400

    def test_records_without_timestamp_is_a_400(self):
        r = client.post(
            "/api/enrich-weather",
            json={"records": [{"energy": 1}], "latitude": 1, "longitude": 1},
        )
        assert r.status_code == 400

    def test_successful_enrichment_returns_weather_columns(self):
        def fake_enrich(df, latitude, longitude, **kw):
            out = df.copy()
            out["temperature_2m_mean"] = 28.0
            out["clearness_index"] = 0.5
            return out

        with patch("api.index.enrich_with_weather", side_effect=fake_enrich):
            r = client.post("/api/enrich-weather", json={**self.BASE, "latitude": 5.3, "longitude": 100.3})
        assert r.status_code == 200
        body = r.json()
        assert set(body["weather_columns"]) == {"temperature_2m_mean", "clearness_index"}
        assert body["summary"]["truncated"] is False


class TestForecast:
    def test_missing_records_is_a_400(self):
        r = client.post("/api/forecast", json={"records": []})
        assert r.status_code == 400

    def test_records_without_energy_is_a_400(self):
        r = client.post("/api/forecast", json={"records": [{"timestamp": "2024-01-01"}]})
        assert r.status_code == 400
        assert "energy" in r.json()["detail"]


class TestExport:
    RECORDS = [
        {"timestamp": "2024-01-01", "source_id": "A", "energy": 1.5},
        {"timestamp": "2024-01-02", "source_id": "A", "energy": 2.5},
    ]

    def test_csv_export(self):
        r = client.post("/api/export", json={"records": self.RECORDS, "format": "csv"})
        assert r.status_code == 200
        assert "attachment" in r.headers["content-disposition"]

    def test_xlsx_export_is_a_real_workbook(self):
        r = client.post("/api/export", json={"records": self.RECORDS, "format": "xlsx"})
        assert r.status_code == 200
        assert r.content[:2] == b"PK"

    def test_empty_export_is_a_400(self):
        r = client.post("/api/export", json={"records": [], "format": "csv"})
        assert r.status_code == 400

    def test_export_full_returns_every_row(self):
        """The whole point of this endpoint: /api/process trims its JSON, so
        exporting what the client holds would hand over a partial file."""
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=240, freq="h").astype(str),
            "energy": [1.0] * 240,
        })
        r = client.post(
            "/api/export-full",
            files=[_upload(df, "inv01.csv")],
            data={
                "mappings_json": json.dumps({"inv01.csv": {"timestamp": "timestamp", "energy": "energy"}}),
                "freq": "1D",
                "anomaly_enabled": "false",
                "export_format": "csv",
            },
        )
        assert r.status_code == 200
        assert r.headers.get("content-encoding") == "gzip"
        # TestClient decompresses transparently, as a browser would.
        rows = r.text.strip().splitlines()
        assert len(rows) - 1 == 10  # 240 hourly rows -> 10 daily
