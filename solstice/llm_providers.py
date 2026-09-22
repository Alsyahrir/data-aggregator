"""
LLM Provider Abstraction

solstice's LLM-assisted column detection (see llm_integration.py) only ever
needs one thing from an LLM: "send this prompt, give me back the text
response." Every provider below implements that single method, so anyone
can plug in a different model/vendor without touching the detection logic
itself — no one provider is baked into the library.

Built in:
    GroqProvider              — Groq's free-tier fast inference (default)
    OpenAIProvider            — OpenAI's own API
    AnthropicProvider         — Anthropic's Claude API
    OpenAICompatibleProvider  — any OpenAI-compatible endpoint: Together,
                                 Fireworks, DeepInfra, a local Ollama/vLLM
                                 server, a corporate proxy, etc. Groq and
                                 OpenAI above are both thin presets of this.
    CustomProvider            — wrap any callable(system, user) -> str,
                                 the escape hatch for anything not listed.

Usage:
    from solstice.llm_providers import GroqProvider, OpenAIProvider, AnthropicProvider
    from solstice import LLMAnalyzer

    analyzer = LLMAnalyzer(provider=GroqProvider(api_key="gsk_..."))
    # or
    analyzer = LLMAnalyzer(provider=OpenAIProvider(api_key="sk-..."))
    # or
    analyzer = LLMAnalyzer(provider=AnthropicProvider(api_key="sk-ant-..."))
    # or point at anything OpenAI-compatible, including a local model:
    analyzer = LLMAnalyzer(provider=OpenAICompatibleProvider(
        api_key="not-needed-for-local",
        base_url="http://localhost:11434/v1",
        models=["llama3"],
    ))
"""

from abc import ABC, abstractmethod
from typing import List, Optional


class LLMProvider(ABC):
    """Interface every LLM backend implements. Subclass this directly if
    none of the built-in providers fit — solstice's detection code only
    ever calls `complete()`.
    """

    @abstractmethod
    def complete(self, system_prompt: str, user_prompt: str) -> str:
        """Send one chat-completion request, return the raw text response.

        Implementations should raise on failure (network error, bad
        response, etc.) rather than returning an empty/partial string, so
        callers can detect and handle failures explicitly.
        """
        raise NotImplementedError


class OpenAICompatibleProvider(LLMProvider):
    """Works with any endpoint that speaks the OpenAI chat-completions API
    shape: OpenAI itself, Groq, Together AI, Fireworks, DeepInfra, a local
    Ollama or vLLM server running in OpenAI-compatible mode, a corporate
    LLM gateway, etc.

    Tries each model in `models` in order, falling back to the next on
    failure — useful for rate-limited free tiers with multiple model
    options (this is how GroqProvider gets its resilience).
    """

    def __init__(
        self,
        api_key: str,
        models: List[str],
        base_url: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 2000,
    ):
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError(
                "openai package required for this provider. Run: pip install openai"
            )
        if not models:
            raise ValueError("models must be a non-empty list")
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.models = models
        self.temperature = temperature
        self.max_tokens = max_tokens

    def complete(self, system_prompt: str, user_prompt: str) -> str:
        last_error: Optional[Exception] = None
        for model in self.models:
            try:
                response = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                )
                return response.choices[0].message.content
            except Exception as e:  # noqa: BLE001 — deliberately broad: try next model
                last_error = e
        raise RuntimeError(
            f"All models failed ({', '.join(self.models)}). Last error: {last_error}"
        )


class GroqProvider(OpenAICompatibleProvider):
    """Groq's free-tier fast inference API. Groq's endpoint is
    OpenAI-compatible, so this uses the `openai` package pointed at Groq's
    base_url rather than requiring a separate `groq` package dependency.
    """

    DEFAULT_MODELS = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768"]
    BASE_URL = "https://api.groq.com/openai/v1"

    def __init__(self, api_key: str, models: Optional[List[str]] = None, **kwargs):
        super().__init__(
            api_key=api_key,
            models=models or self.DEFAULT_MODELS,
            base_url=self.BASE_URL,
            **kwargs,
        )


class OpenAIProvider(OpenAICompatibleProvider):
    """OpenAI's own API."""

    DEFAULT_MODELS = ["gpt-4o-mini", "gpt-4o"]

    def __init__(self, api_key: str, models: Optional[List[str]] = None, **kwargs):
        super().__init__(api_key=api_key, models=models or self.DEFAULT_MODELS, base_url=None, **kwargs)


class AnthropicProvider(LLMProvider):
    """Anthropic's Claude API. Kept as a separate implementation (not an
    OpenAICompatibleProvider subclass) since Anthropic's message format and
    system-prompt handling differ from the OpenAI shape.
    """

    DEFAULT_MODELS = ["claude-3-5-haiku-latest", "claude-3-5-sonnet-latest"]

    def __init__(self, api_key: str, models: Optional[List[str]] = None, max_tokens: int = 2000):
        try:
            from anthropic import Anthropic
        except ImportError:
            raise ImportError(
                "anthropic package required for this provider. Run: pip install anthropic"
            )
        self.client = Anthropic(api_key=api_key)
        self.models = models or self.DEFAULT_MODELS
        self.max_tokens = max_tokens

    def complete(self, system_prompt: str, user_prompt: str) -> str:
        last_error: Optional[Exception] = None
        for model in self.models:
            try:
                response = self.client.messages.create(
                    model=model,
                    max_tokens=self.max_tokens,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                return response.content[0].text
            except Exception as e:  # noqa: BLE001 — deliberately broad: try next model
                last_error = e
        raise RuntimeError(
            f"All models failed ({', '.join(self.models)}). Last error: {last_error}"
        )


class CustomProvider(LLMProvider):
    """Wrap an arbitrary callable as a provider — the escape hatch for a
    backend not listed above (a local model served some other way, a
    company-internal gateway with its own auth, a test double, etc.).

    Usage:
        def my_llm(system_prompt: str, user_prompt: str) -> str:
            ...  # however you want to call your model
            return response_text

        analyzer = LLMAnalyzer(provider=CustomProvider(my_llm))
    """

    def __init__(self, fn):
        self._fn = fn

    def complete(self, system_prompt: str, user_prompt: str) -> str:
        return self._fn(system_prompt, user_prompt)
