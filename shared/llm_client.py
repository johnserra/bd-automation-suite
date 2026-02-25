"""Claude API client with cost tracking for BD Automation Suite."""

import os
from typing import Optional

import anthropic

from shared.logger import get_logger

logger = get_logger("llm_client")

HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"

# Cost per 1M tokens (USD)
COSTS: dict[str, dict[str, float]] = {
    HAIKU:  {"input": 0.80,  "output": 4.00},
    SONNET: {"input": 3.00,  "output": 15.00},
}


class LLMClient:
    """Wrapper around the Anthropic SDK with per-session cost tracking.

    Use Haiku (default) for batch summarization tasks — it's ~4× cheaper than Sonnet.
    Use Sonnet explicitly (model=SONNET) for outreach drafting where quality matters.
    """

    def __init__(self, api_key: str):
        self._client = anthropic.Anthropic(api_key=api_key)
        self._calls = 0
        self._input_tokens = 0
        self._output_tokens = 0

    @classmethod
    def from_env(cls) -> "LLMClient":
        """Construct LLMClient from ANTHROPIC_API_KEY environment variable."""
        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY environment variable is not set")
        return cls(api_key=api_key)

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
            model: Model ID. Defaults to Haiku for cost efficiency.
            max_tokens: Maximum output tokens.

        Returns:
            The model's text response as a string.
        """
        messages = [{"role": "user", "content": prompt}]
        kwargs: dict = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": messages,
        }
        if system:
            kwargs["system"] = system

        logger.debug("LLM call model=%s max_tokens=%s", model, max_tokens)
        response = self._client.messages.create(**kwargs)

        # Accumulate token counts
        self._calls += 1
        self._input_tokens += response.usage.input_tokens
        self._output_tokens += response.usage.output_tokens

        text = response.content[0].text
        logger.debug(
            "LLM response in=%s out=%s tokens",
            response.usage.input_tokens,
            response.usage.output_tokens,
        )
        return text

    def get_cost_summary(self) -> dict:
        """Return accumulated token usage and estimated cost for this session.

        Returns:
            Dict with keys: calls, input_tokens, output_tokens, cost_usd.
            cost_usd is summed across all models called (uses COSTS table).
        """
        # Approximate cost — tracks totals, not per-model breakdown
        # For accurate tracking, extend to accumulate per-model token counts
        avg_input_cost = sum(c["input"] for c in COSTS.values()) / len(COSTS)
        avg_output_cost = sum(c["output"] for c in COSTS.values()) / len(COSTS)

        cost_usd = (
            self._input_tokens * avg_input_cost / 1_000_000
            + self._output_tokens * avg_output_cost / 1_000_000
        )
        return {
            "calls": self._calls,
            "input_tokens": self._input_tokens,
            "output_tokens": self._output_tokens,
            "cost_usd": round(cost_usd, 6),
        }
