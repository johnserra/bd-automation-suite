"""Multi-provider LLM client with cost tracking for BD Automation Suite.

Supports Anthropic Claude and Google Gemini behind a single interface.
Callers select the provider by passing the appropriate model constant.
"""

import os
from typing import Optional

import anthropic

from shared.logger import get_logger

logger = get_logger("llm_client")

# ---------------------------------------------------------------------------
# Anthropic models
# ---------------------------------------------------------------------------
HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"

# ---------------------------------------------------------------------------
# Gemini models
# ---------------------------------------------------------------------------
FLASH = "gemini-2.5-flash"
GEMINI_PRO = "gemini-2.5-pro"

# ---------------------------------------------------------------------------
# Provider detection
# ---------------------------------------------------------------------------
_GEMINI_MODELS = {FLASH, GEMINI_PRO}

def _is_gemini(model: str) -> bool:
    return model in _GEMINI_MODELS or model.startswith("gemini-")

# ---------------------------------------------------------------------------
# Cost per 1M tokens (USD) — update when pricing changes
# ---------------------------------------------------------------------------
COSTS: dict[str, dict[str, float]] = {
    HAIKU:      {"input": 0.80,  "output": 4.00},
    SONNET:     {"input": 3.00,  "output": 15.00},
    FLASH:      {"input": 0.15,  "output": 0.60},
    GEMINI_PRO: {"input": 1.25,  "output": 10.00},
}


class LLMClient:
    """Unified LLM client supporting Claude and Gemini with per-model cost tracking.

    Use FLASH (default) for batch summarization — cheapest option.
    Use HAIKU for tasks where Claude's instruction-following matters but cost is key.
    Use SONNET for outreach drafting where quality matters most.
    Use GEMINI_PRO for grounded search or long-context tasks.
    """

    def __init__(
        self,
        api_key: str,
        gemini_api_key: Optional[str] = None,
    ):
        self._anthropic = anthropic.Anthropic(api_key=api_key)
        self._gemini_model = None  # lazy init
        self._gemini_api_key = gemini_api_key
        self._usage: dict[str, dict[str, int]] = {}  # model -> {input, output}
        self._calls = 0

    @classmethod
    def from_env(cls) -> "LLMClient":
        """Construct from environment variables.

        Required: ANTHROPIC_API_KEY
        Optional: GEMINI_API_KEY (Gemini calls will fail without it)
        """
        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY environment variable is not set")
        gemini_key = os.getenv("GEMINI_API_KEY", "").strip() or None
        return cls(api_key=api_key, gemini_api_key=gemini_key)

    # -------------------------------------------------------------------
    # Public interface
    # -------------------------------------------------------------------

    def complete(
        self,
        prompt: str,
        system: Optional[str] = None,
        model: str = HAIKU,
        max_tokens: int = 1024,
    ) -> str:
        """Send a prompt and return the text response.

        Args:
            prompt: User-turn message.
            system: Optional system prompt.
            model: Model ID — use module-level constants (HAIKU, SONNET, FLASH, GEMINI_PRO).
            max_tokens: Maximum output tokens.

        Returns:
            The model's text response as a string.
        """
        if _is_gemini(model):
            return self._complete_gemini(prompt, system, model, max_tokens)
        return self._complete_anthropic(prompt, system, model, max_tokens)

    def get_cost_summary(self) -> dict:
        """Return accumulated token usage and estimated cost for this session.

        Returns:
            Dict with keys: calls, input_tokens, output_tokens, cost_usd, by_model.
        """
        total_input = 0
        total_output = 0
        total_cost = 0.0
        by_model: dict[str, dict] = {}

        for model, tokens in self._usage.items():
            inp = tokens["input"]
            out = tokens["output"]
            rates = COSTS.get(model, {"input": 0, "output": 0})
            cost = inp * rates["input"] / 1_000_000 + out * rates["output"] / 1_000_000

            total_input += inp
            total_output += out
            total_cost += cost
            by_model[model] = {
                "input_tokens": inp,
                "output_tokens": out,
                "cost_usd": round(cost, 6),
            }

        return {
            "calls": self._calls,
            "input_tokens": total_input,
            "output_tokens": total_output,
            "cost_usd": round(total_cost, 6),
            "by_model": by_model,
        }

    # -------------------------------------------------------------------
    # Anthropic
    # -------------------------------------------------------------------

    def _complete_anthropic(
        self, prompt: str, system: Optional[str], model: str, max_tokens: int
    ) -> str:
        messages = [{"role": "user", "content": prompt}]
        kwargs: dict = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": messages,
        }
        if system:
            kwargs["system"] = system

        logger.debug("Anthropic call model=%s max_tokens=%s", model, max_tokens)
        response = self._anthropic.messages.create(**kwargs)

        self._track(model, response.usage.input_tokens, response.usage.output_tokens)

        text = response.content[0].text
        logger.debug(
            "Anthropic response in=%s out=%s tokens",
            response.usage.input_tokens,
            response.usage.output_tokens,
        )
        return text

    # -------------------------------------------------------------------
    # Gemini
    # -------------------------------------------------------------------

    def _get_gemini(self):
        """Lazy-import and configure google.generativeai."""
        if self._gemini_model is not None:
            return

        if not self._gemini_api_key:
            raise EnvironmentError(
                "GEMINI_API_KEY is required for Gemini models. "
                "Set it in .env or pass gemini_api_key to LLMClient."
            )

        try:
            import google.generativeai as genai
        except ImportError:
            raise ImportError(
                "google-generativeai is required for Gemini models. "
                "Install it: uv add google-generativeai"
            )

        genai.configure(api_key=self._gemini_api_key)
        self._genai = genai
        self._gemini_model = True  # marker that init is done

    def _complete_gemini(
        self, prompt: str, system: Optional[str], model: str, max_tokens: int
    ) -> str:
        self._get_gemini()

        gen_config = {"max_output_tokens": max_tokens}
        model_obj = self._genai.GenerativeModel(
            model_name=model,
            system_instruction=system,
            generation_config=gen_config,
        )

        logger.debug("Gemini call model=%s max_tokens=%s", model, max_tokens)
        response = model_obj.generate_content(prompt)

        # Track usage — Gemini reports via usage_metadata
        meta = getattr(response, "usage_metadata", None)
        input_tok = getattr(meta, "prompt_token_count", 0) if meta else 0
        output_tok = getattr(meta, "candidates_token_count", 0) if meta else 0
        self._track(model, input_tok, output_tok)

        logger.debug("Gemini response in=%s out=%s tokens", input_tok, output_tok)
        return response.text

    # -------------------------------------------------------------------
    # Cost tracking
    # -------------------------------------------------------------------

    def _track(self, model: str, input_tokens: int, output_tokens: int) -> None:
        self._calls += 1
        if model not in self._usage:
            self._usage[model] = {"input": 0, "output": 0}
        self._usage[model]["input"] += input_tokens
        self._usage[model]["output"] += output_tokens
