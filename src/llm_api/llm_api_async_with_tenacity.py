"""
Async version of llm_api_utils_async.py

This module provides asynchronous utility functions for interacting with various LLM APIs,
including OpenAI, Anthropic, and Llama3. It handles API calls, validates responses,
and manages provider-specific nuances such as single-block versus multi-block responses.

Key Features:
- Asynchronous support for OpenAI and Anthropic APIs using global clients.
- Provider-specific rate limiting with AsyncLimiter.
- Validation and structuring of responses into Pydantic models.
- Modular design for provider-specific handling.

Modules and Methods:
- `call_openai_api_async`: Asynchronously interacts with the OpenAI API.
- `call_anthropic_api_async`: Asynchronously interacts with the Anthropic API.
- `call_llama3_async`: Asynchronously interacts with the Llama3 API using
a synchronous executor.
- `call_api_async`: Unified async function for handling API calls with validation.
- `run_in_executor_async`: Executes synchronous functions in an async context.
- Validation utilities (e.g., `validate_response_type`, `validate_json_type`).

Modules and Methods:
- `call_openai_api_async`: Asynchronously interacts with the OpenAI API.
- `call_anthropic_api_async`: Asynchronously interacts with the Anthropic API.
- `call_api_async`: Unified async function for API calls with validation.
- Validation utilities (e.g., `validate_response_type`, `validate_json_type`).

This module provides asynchronous utility functions for interacting with OpenAI
and Anthropic APIs.
It uses global clients, rate limiting, and Tenacity for retries.

Key Features:
- Asynchronous support for OpenAI and Anthropic APIs.
- Global clients for resource efficiency.
- Rate limiting with AsyncLimiter.
- Retry logic with Tenacity.
- Validation and structuring of responses into Pydantic models.
"""

import asyncio
import time
import atexit
from typing import Union, cast
import json
import logging
from pydantic import ValidationError
import httpx
from aiolimiter import AsyncLimiter
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

# LLM imports
from openai import AsyncOpenAI
from anthropic import AsyncAnthropic

# From own modules
from models.llm_response_models import (
    CodeResponse,
    JSONResponse,
    TabularResponse,
    TextResponse,
    EditingResponse,
    JobSiteResponse,
    RequirementsResponse,
)
from job_bot.llm_providers.llm_api_utils import (
    get_anthropic_api_key,
    get_openai_api_key,
)
from job_bot.llm_providers.llm_response_validators import (
    validate_json_type,
    validate_response_type,
)
from job_bot.config.project_config import (
    OPENAI,
    ANTHROPIC,
    GPT_35_TURBO,
    GPT_4_1,
    GPT_4_1_NANO,
    CLAUDE_HAIKU,
    CLAUDE_SONNET_3_5,
    CLAUDE_OPUS,
)

logger = logging.getLogger(__name__)

# Global clients
OPENAI_CLIENT = AsyncOpenAI(api_key=get_openai_api_key(), timeout=httpx.Timeout(10.0))
ANTHROPIC_CLIENT = AsyncAnthropic(
    api_key=get_anthropic_api_key(), timeout=httpx.Timeout(10.0)
)


# Automatic shutdown
async def shutdown_clients():
    await OPENAI_CLIENT.aclose()
    await ANTHROPIC_CLIENT.aclose()
    logger.info("Global clients shut down.")


def close_clients_sync():
    asyncio.run(shutdown_clients())


atexit.register(close_clients_sync)

# Provider-specific rate limiters (requests per minute)
RATE_LIMITERS = {
    OPENAI: AsyncLimiter(max_rate=5000, time_period=60),  # Tier 2: 5,000 RPM
    ANTHROPIC: AsyncLimiter(max_rate=50, time_period=60),  # Free tier: 50 RPM
}

# Tenacity retry decorator
retry_policy = retry(
    stop=stop_after_attempt(5),  # Max 5 retries
    wait=wait_exponential_jitter(
        initial=1, max=10, jitter=1
    ),  # 1s base, 10s cap, jitter
    retry=retry_if_exception_type(httpx.HTTPStatusError),  # Retry on HTTP errors
    before_sleep=before_sleep_log(logger, logging.WARNING),  # Log retries
)


async def call_api_async(
    client: Union[AsyncOpenAI, AsyncAnthropic],
    model_id: str,
    prompt: str,
    expected_res_type: str,
    json_type: str,
    temperature: float,
    max_tokens: int,
    llm_provider: str,
) -> Union[
    JSONResponse,
    TabularResponse,
    CodeResponse,
    TextResponse,
    EditingResponse,
    JobSiteResponse,
    RequirementsResponse,
]:
    """Asynchronous function for OpenAI and Anthropic API calls with rate limiting and retries."""
    async with RATE_LIMITERS[llm_provider]:

        @retry_policy
        async def make_request():
            if llm_provider.lower() == OPENAI:
                return await client.chat.completions.create(
                    model=model_id,
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            elif llm_provider.lower() == ANTHROPIC:
                return await client.messages.create(
                    model=model_id,
                    max_tokens=max_tokens,
                    messages=[
                        {
                            "role": "user",
                            "content": "You are a helpful assistant.\n" + prompt,
                        }
                    ],
                    temperature=temperature,
                )

        try:
            logger.info(
                f"Making API call to {llm_provider} with expected type: {expected_res_type}"
            )
            response = await make_request()

            # Extract response content
            if llm_provider.lower() == OPENAI:
                if not response or not response.choices:
                    raise ValueError(
                        "OpenAI API returned an invalid or empty response."
                    )
                response_content = response.choices[0].message.content
            elif llm_provider.lower() == ANTHROPIC:
                if not response or not response.content:
                    raise ValueError("Empty response from Anthropic API")
                first_block = response.content[0]
                response_content = (
                    first_block.text
                    if hasattr(first_block, "text")
                    else str(first_block)
                )

            logger.info(f"Raw {llm_provider} Response: {response_content}")

            validated_response_model = validate_response_type(
                response_content, expected_res_type
            )

            if expected_res_type == "json":
                if isinstance(validated_response_model, JSONResponse):
                    validated_response_model = validate_json_type(
                        response_model=validated_response_model, json_type=json_type
                    )
                else:
                    raise TypeError("Expected JSONResponse model for JSON type.")

            return validated_response_model

        except httpx.HTTPStatusError as e:
            if e.response.status_code in [429, 529]:
                logger.error(f"Rate limit or overload error for {llm_provider}: {e}")
                raise  # Let Tenacity retry
            else:
                logger.error(f"HTTP error for {llm_provider}: {e}")
                raise
        except httpx.TimeoutException as e:
            logger.error(f"Timeout for {llm_provider}: {e}")
            raise
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(f"Validation error for {llm_provider}: {e}")
            raise ValueError(f"Invalid format from {llm_provider} API: {e}")
        except Exception as e:
            logger.error(f"{llm_provider} API call failed: {e}")
            raise


async def call_openai_api_async(
    prompt: str,
    model_id: str = GPT_4_1_NANO,
    expected_res_type: str = "str",
    json_type: str = "",
    temperature: float = 0.4,
    max_tokens: int = 1056,
    client: AsyncOpenAI = OPENAI_CLIENT,
) -> Union[
    JSONResponse,
    TabularResponse,
    TextResponse,
    CodeResponse,
    EditingResponse,
    JobSiteResponse,
    RequirementsResponse,
]:
    """Asynchronously calls OpenAI API using the global client."""
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
    json_type: str = "",
    temperature: float = 0.4,
    max_tokens: int = 1056,
    client: AsyncAnthropic = ANTHROPIC_CLIENT,
) -> Union[
    JSONResponse,
    TabularResponse,
    TextResponse,
    CodeResponse,
    EditingResponse,
    JobSiteResponse,
    RequirementsResponse,
]:
    """Asynchronously calls Anthropic API using the global client."""
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


async def process_text_group(prompts):
    start = time.time()
    tasks = [call_openai_api_async(p) for p in prompts[: len(prompts) // 2]] + [
        call_anthropic_api_async(p) for p in prompts[len(prompts) // 2 :]
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Task {i} failed: {result}")
    print(f"{len(prompts)} calls took {time.time() - start:.2f}s")
    return results


if __name__ == "__main__":
    asyncio.run(process_text_group(["Test prompt"] * 10))
