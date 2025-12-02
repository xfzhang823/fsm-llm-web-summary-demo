"""
Async LLM API utilities.

This module provides asynchronous utility functions for interacting with
OpenAI and Anthropic LLM APIs. It handles API calls, validates responses,
and manages provider-specific nuances such as single-block vs multi-block
responses.

Key features:
- Shared, global AsyncOpenAI and AsyncAnthropic clients with custom httpx settings.
- Provider-specific rate limiting and retry with jittered backoff.
- Validation and structuring of responses into Pydantic models via:
    - validate_response_type (json / tabular / str / code)
    - validate_json_type (json subtypes like "web_summary" or "generic")
- Async wrappers:
    - call_openai_api_async
    - call_anthropic_api_async

Intended usage:
    * This module sits under your LLM API abstraction and is called by
      pipeline stages (e.g., web summary Stage B) that want a validated,
      structured response instead of raw SDK objects.
"""

from __future__ import annotations

# Built-in & external libraries
import os
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Optional, cast
import json
from random import uniform

from pydantic import ValidationError
import httpx
from httpx import Timeout, Limits
from aiolimiter import AsyncLimiter
from dotenv import load_dotenv

# LLM imports
from openai import AsyncOpenAI
from anthropic import AsyncAnthropic

# Project imports
from models.llm_response_models import (
    CodeResponse,
    JSONResponse,
    TabularResponse,
    TextResponse,
)
from models.web_content_models import WebSummaryJSON
from llm_api.llm_response_validators import (
    validate_json_type,
    validate_response_type,
)
from config.project_config import (
    OPENAI,
    ANTHROPIC,
    GPT_4_1,
    GPT_4_1_NANO,
    CLAUDE_HAIKU,
    CLAUDE_SONNET_3_5,
    CLAUDE_OPUS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP clients & API keys
# ---------------------------------------------------------------------------

# Global HTTP client config (reused by both SDKs)
_HTTP_LIMITS = Limits(max_connections=40, max_keepalive_connections=20)
_HTTP_TIMEOUT = Timeout(connect=15.0, read=60.0, write=30.0, pool=60.0)
_OPENAI_HTTP = httpx.AsyncClient(limits=_HTTP_LIMITS, timeout=_HTTP_TIMEOUT)
_ANTHROPIC_HTTP = httpx.AsyncClient(limits=_HTTP_LIMITS, timeout=_HTTP_TIMEOUT)

# Optionally load .env once at import time
load_dotenv()


def get_openai_api_key() -> str:
    """Retrieve the OpenAI API key from environment variables."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OpenAI API key not found. Please set it in the .env file.")
        raise EnvironmentError("OpenAI API key not found.")
    return api_key


def get_anthropic_api_key() -> str:
    """Retrieve the Anthropic API key from environment variables."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("Anthropic API key not found. Please set it in the .env file.")
        raise EnvironmentError("Anthropic API key not found.")
    return api_key


OPENAI_CLIENT = AsyncOpenAI(
    api_key=get_openai_api_key(),
    http_client=_OPENAI_HTTP,
    max_retries=4,  # give the SDK some internal retries too
)

ANTHROPIC_CLIENT = AsyncAnthropic(
    api_key=get_anthropic_api_key(),
    http_client=_ANTHROPIC_HTTP,
    max_retries=4,
)

# Provider-specific rate limiters (requests per minute)
# Adjust based on your OpenAI tier and Anthropic plan
RATE_LIMITERS = {
    OPENAI: AsyncLimiter(max_rate=1000, time_period=60),
    ANTHROPIC: AsyncLimiter(max_rate=50, time_period=60),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def run_in_executor_async(func, *args):
    """
    Run a synchronous function in a ThreadPoolExecutor for async compatibility.

    Currently unused, but kept for future support of sync-only providers
    (e.g., local Llama/transformers models).
    """
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(pool, func, *args)


async def with_rate_limit_and_retry(api_func, llm_provider: str):
    """
    Apply rate limiting and retries with jittered backoff for 429/5xx/timeouts.

    Args:
        api_func:
            Awaitable that performs the actual SDK call.
        llm_provider:
            Provider key used to choose the right AsyncLimiter.
    """
    async with RATE_LIMITERS[llm_provider]:
        max_retries = 5
        base = 1.25  # backoff base
        cap = 12.0  # max sleep seconds

        for attempt in range(max_retries):
            try:
                return await api_func()

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                retriable = (status == 429) or (500 <= status < 600)
                if retriable and attempt < max_retries - 1:
                    # Respect Retry-After if present; else exponential backoff with jitter
                    ra = e.response.headers.get("Retry-After")
                    ra_secs = float(ra) if ra and ra.isdigit() else None
                    wait = min(cap, (ra_secs or (base**attempt)) + uniform(0, 0.75))
                    logger.warning(
                        "HTTP %s for %s, retrying in %.2fs (attempt %d/%d)…",
                        status,
                        llm_provider,
                        wait,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(wait)
                    continue
                raise

            except (httpx.TimeoutException, httpx.TransportError) as e:
                # Network flake or read timeout: back off and retry
                if attempt < max_retries - 1:
                    wait = min(cap, (base**attempt) + uniform(0, 0.75))
                    logger.warning(
                        "Network/timeout for %s: %s — retrying in %.2fs (attempt %d/%d)…",
                        llm_provider,
                        str(e),
                        wait,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.error(
                    "Timeout/transport error for %s (final attempt): %s",
                    llm_provider,
                    e,
                )
                raise


# ---------------------------------------------------------------------------
# Unified async API calling function
# ---------------------------------------------------------------------------


async def call_api_async(
    client: Optional[Union[AsyncOpenAI, AsyncAnthropic]],
    model_id: str,
    prompt: str,
    expected_res_type: str,
    json_type: str,
    temperature: float,
    max_tokens: int,
    llm_provider: str,
) -> Union[JSONResponse, TabularResponse, CodeResponse, TextResponse, WebSummaryJSON]:
    """
    Asynchronously handle API calls to OpenAI / Anthropic.

    This method:
      - Handles provider-specific nuances (e.g. Anthropic multi-block content).
      - Applies rate limiting + retry.
      - Validates responses via validate_response_type + validate_json_type.

    Args:
        client:
            API client instance for the respective provider. If None, the
            appropriate global client is used.
        model_id:
            Model ID to use for the API call.
        prompt:
            Input prompt for the LLM.
        expected_res_type:
            Expected type of response ("json", "tabular", "str", "code").
        json_type:
            JSON subtype for validation (e.g. "web_summary", "generic").
            Only used when expected_res_type == "json".
        temperature:
            Sampling temperature for the LLM.
        max_tokens:
            Maximum number of tokens for the response.
        llm_provider:
            Provider name (OPENAI or ANTHROPIC constant).

    Returns:
        One of JSONResponse, TabularResponse, CodeResponse, TextResponse.

    Raises:
        ValueError, TypeError, or generic Exception for unexpected errors.
    """
    try:
        logger.info(
            "Making API call with expected response type: %s", expected_res_type
        )
        response_content: str = ""

        if llm_provider == OPENAI:
            openai_client = cast(AsyncOpenAI, client)

            async def openai_request():
                return await openai_client.chat.completions.create(
                    model=model_id,
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )

            response = await with_rate_limit_and_retry(openai_request, llm_provider)

            if not response or not response.choices:
                raise ValueError("OpenAI API returned an invalid or empty response.")

            response_content = response.choices[0].message.content

        elif llm_provider == ANTHROPIC:
            anthropic_client = cast(AsyncAnthropic, client)
            system_instruction = (
                "You are a helpful assistant who adheres to instructions."
            )

            async def anthropic_request():
                return await anthropic_client.messages.create(
                    model=model_id,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": system_instruction + prompt}],
                    temperature=temperature,
                )

            response = await with_rate_limit_and_retry(anthropic_request, llm_provider)

            if not response or not response.content:
                raise ValueError("Empty response received from Anthropic API")

            first_block = response.content[0]
            response_content = (
                first_block.text if hasattr(first_block, "text") else str(first_block)
            )

            if not response_content:
                raise ValueError("Empty content in response from Anthropic API")

        else:
            raise ValueError(f"Unsupported llm_provider: {llm_provider!r}")

        logger.info("Raw %s response: %s", llm_provider, response_content)

        # Validation 1: base response-type structure
        validated_response_model = validate_response_type(
            response_content, expected_res_type
        )
        logger.info(
            "Validated response after validate_response_type:\n%s",
            validated_response_model,
        )

        # Validation 2: JSON subtype (e.g. web_summary) if needed
        if expected_res_type == "json":
            if isinstance(validated_response_model, JSONResponse):
                validated_response_model = validate_json_type(
                    response_model=validated_response_model, json_type=json_type
                )
            else:
                raise TypeError(
                    "Expected validated response content to be a JSONResponse model."
                )

        logger.info(
            "Validated response after validate_json_type:\n%s",
            validated_response_model,
        )

        return validated_response_model

    except (json.JSONDecodeError, ValidationError) as e:
        logger.error("Validation or parsing error: %s", e)
        raise ValueError(f"Invalid format received from {llm_provider} API: {e}") from e
    except Exception as e:  # noqa: BLE001
        logger.error("%s API call failed: %s", llm_provider, e)
        raise


# ---------------------------------------------------------------------------
# Provider-specific async wrappers
# ---------------------------------------------------------------------------


async def call_openai_api_async(
    prompt: str,
    model_id: str = GPT_4_1_NANO,
    expected_res_type: str = "str",
    json_type: str = "generic",
    temperature: float = 0.4,
    max_tokens: int = 1056,
    client: Optional[AsyncOpenAI] = None,
) -> Union[JSONResponse, TabularResponse, TextResponse, CodeResponse, WebSummaryJSON]:
    """
    Asynchronously call the OpenAI API using the global AsyncOpenAI client
    (unless a custom client is provided).
    """
    if client is None:
        client = OPENAI_CLIENT

    logger.info("OpenAI client ready for async API call.")
    return await call_api_async(
        client=client,
        model_id=model_id,
        prompt=prompt,
        expected_res_type=expected_res_type,
        json_type=json_type,
        temperature=temperature,
        max_tokens=max_tokens,
        llm_provider=OPENAI,
    )


async def call_anthropic_api_async(
    prompt: str,
    model_id: str = CLAUDE_SONNET_3_5,
    expected_res_type: str = "str",
    json_type: str = "generic",
    temperature: float = 0.4,
    max_tokens: int = 1056,
    client: Optional[AsyncAnthropic] = None,
) -> Union[JSONResponse, TabularResponse, TextResponse, CodeResponse, WebSummaryJSON]:
    """
    Asynchronously call the Anthropic API using the global AsyncAnthropic client
    (unless a custom client is provided).
    """
    if client is None:
        client = ANTHROPIC_CLIENT

    logger.info("Anthropic client ready for async API call.")
    return await call_api_async(
        client=client,
        model_id=model_id,
        prompt=prompt,
        expected_res_type=expected_res_type,
        json_type=json_type,
        temperature=temperature,
        max_tokens=max_tokens,
        llm_provider=ANTHROPIC,
    )
