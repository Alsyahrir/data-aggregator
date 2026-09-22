"""Tests for solstice.llm_providers module.

The underlying `openai`/`anthropic` SDK clients are mocked throughout — no
real API traffic anywhere in this file.
"""

from unittest.mock import MagicMock, patch

import pytest

from solstice.llm_providers import (
    AnthropicProvider,
    CustomProvider,
    GroqProvider,
    OpenAICompatibleProvider,
    OpenAIProvider,
)


def _fake_openai_response(text: str) -> MagicMock:
    resp = MagicMock()
    resp.choices = [MagicMock()]
    resp.choices[0].message.content = text
    return resp


class TestOpenAICompatibleProvider:
    def test_requires_nonempty_models_list(self):
        with pytest.raises(ValueError, match="models"):
            OpenAICompatibleProvider(api_key="k", models=[])

    def test_friendly_error_when_openai_package_missing(self, monkeypatch):
        import sys
        monkeypatch.setitem(sys.modules, "openai", None)
        with pytest.raises(ImportError, match="pip install openai"):
            OpenAICompatibleProvider(api_key="k", models=["m"])

    def test_returns_first_successful_model_response(self):
        with patch("openai.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = _fake_openai_response("hello")

            provider = OpenAICompatibleProvider(api_key="k", models=["model-a"])
            result = provider.complete("system", "user")

            assert result == "hello"
            mock_client.chat.completions.create.assert_called_once()
            _, kwargs = mock_client.chat.completions.create.call_args
            assert kwargs["model"] == "model-a"
            assert kwargs["messages"] == [
                {"role": "system", "content": "system"},
                {"role": "user", "content": "user"},
            ]

    def test_falls_back_to_next_model_on_failure(self):
        with patch("openai.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.side_effect = [
                Exception("model-a down"),
                _fake_openai_response("from model-b"),
            ]

            provider = OpenAICompatibleProvider(api_key="k", models=["model-a", "model-b"])
            result = provider.complete("system", "user")

            assert result == "from model-b"
            assert mock_client.chat.completions.create.call_count == 2

    def test_raises_after_all_models_fail(self):
        with patch("openai.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.side_effect = Exception("down")

            provider = OpenAICompatibleProvider(api_key="k", models=["a", "b", "c"])
            with pytest.raises(RuntimeError, match="All models failed"):
                provider.complete("system", "user")
            assert mock_client.chat.completions.create.call_count == 3

    def test_passes_base_url_through(self):
        with patch("openai.OpenAI") as mock_openai_cls:
            OpenAICompatibleProvider(api_key="k", models=["m"], base_url="http://localhost:11434/v1")
            mock_openai_cls.assert_called_once_with(api_key="k", base_url="http://localhost:11434/v1")


class TestGroqProvider:
    def test_uses_groq_base_url_and_default_models(self):
        with patch("openai.OpenAI") as mock_openai_cls:
            provider = GroqProvider(api_key="gsk_test")
            mock_openai_cls.assert_called_once_with(api_key="gsk_test", base_url="https://api.groq.com/openai/v1")
            assert provider.models == GroqProvider.DEFAULT_MODELS

    def test_custom_models_override_default(self):
        with patch("openai.OpenAI"):
            provider = GroqProvider(api_key="gsk_test", models=["my-model"])
            assert provider.models == ["my-model"]


class TestOpenAIProvider:
    def test_uses_default_openai_base_url(self):
        with patch("openai.OpenAI") as mock_openai_cls:
            provider = OpenAIProvider(api_key="sk_test")
            mock_openai_cls.assert_called_once_with(api_key="sk_test", base_url=None)
            assert provider.models == OpenAIProvider.DEFAULT_MODELS


class TestAnthropicProvider:
    def test_returns_response_text(self):
        with patch("anthropic.Anthropic") as mock_anthropic_cls:
            mock_client = MagicMock()
            mock_anthropic_cls.return_value = mock_client
            fake_response = MagicMock()
            fake_response.content = [MagicMock(text="claude says hi")]
            mock_client.messages.create.return_value = fake_response

            provider = AnthropicProvider(api_key="sk-ant-test")
            result = provider.complete("system", "user")

            assert result == "claude says hi"
            _, kwargs = mock_client.messages.create.call_args
            assert kwargs["system"] == "system"
            assert kwargs["messages"] == [{"role": "user", "content": "user"}]

    def test_falls_back_to_next_model_on_failure(self):
        with patch("anthropic.Anthropic") as mock_anthropic_cls:
            mock_client = MagicMock()
            mock_anthropic_cls.return_value = mock_client
            ok_response = MagicMock()
            ok_response.content = [MagicMock(text="ok")]
            mock_client.messages.create.side_effect = [Exception("down"), ok_response]

            provider = AnthropicProvider(api_key="k", models=["claude-a", "claude-b"])
            result = provider.complete("system", "user")

            assert result == "ok"
            assert mock_client.messages.create.call_count == 2

    def test_raises_after_all_models_fail(self):
        with patch("anthropic.Anthropic") as mock_anthropic_cls:
            mock_client = MagicMock()
            mock_anthropic_cls.return_value = mock_client
            mock_client.messages.create.side_effect = Exception("down")

            provider = AnthropicProvider(api_key="k", models=["a", "b"])
            with pytest.raises(RuntimeError, match="All models failed"):
                provider.complete("system", "user")

    def test_friendly_error_when_anthropic_package_missing(self, monkeypatch):
        import sys
        monkeypatch.setitem(sys.modules, "anthropic", None)
        with pytest.raises(ImportError, match="pip install anthropic"):
            AnthropicProvider(api_key="k")


class TestCustomProvider:
    def test_delegates_to_wrapped_callable(self):
        calls = []

        def my_llm(system_prompt, user_prompt):
            calls.append((system_prompt, user_prompt))
            return "custom response"

        provider = CustomProvider(my_llm)
        result = provider.complete("sys", "usr")

        assert result == "custom response"
        assert calls == [("sys", "usr")]

    def test_propagates_exceptions_from_callable(self):
        def broken_llm(system_prompt, user_prompt):
            raise ValueError("boom")

        provider = CustomProvider(broken_llm)
        with pytest.raises(ValueError, match="boom"):
            provider.complete("sys", "usr")
