import pytest


@pytest.fixture(autouse=True)
def _no_real_groq_key(monkeypatch):
    """Prevent any test from accidentally making a real Groq API call by
    picking up a developer's local GROQ_API_KEY env var. Tests that need to
    exercise the LLM path mock the client explicitly instead."""
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
