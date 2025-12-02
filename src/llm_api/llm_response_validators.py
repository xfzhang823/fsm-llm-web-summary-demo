"""
llm_api/llm_response_validators.py

Utilities for parsing and validating LLM responses by *response_type* and
(optional) *json_type*.

Layers:

1) validate_response_type(...)
   - Normalize raw LLM output into:
       • "json"    → JSONResponse
       • "tabular" → TabularResponse (pandas DataFrame)
       • "str"     → TextResponse
       • "code"    → CodeResponse

2) validate_json_type(...)
   - Second step for JSONResponse; currently supports:
       • "web_summary" → validate against WebSummaryJSON, then return JSONResponse
       • "generic"     → return JSONResponse as-is
"""

from __future__ import annotations

from io import StringIO
import re
import json
import logging
from typing import Any, Union, Optional, List, Dict

import pandas as pd

# Import from project
from models.llm_response_models import (
    CodeResponse,
    TabularResponse,
    TextResponse,
    JSONResponse,
)
from models.web_content_models import WebSummaryJSON

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON parsing / cleaning helpers
# ---------------------------------------------------------------------------
def clean_and_extract_json(
    response_content: str,
) -> Optional[Union[Dict[str, Any], List[Any]]]:
    """
    Extracts, cleans, and parses JSON content from the API response.
    Strips out any non-JSON content like extra text before the JSON block.
    Also removes JavaScript-style comments and trailing commas.

    Args:
        response_content (str): Raw response content.

    Returns:
        Optional[Union[Dict[str, Any], List[Any]]]: Parsed JSON data as a dictionary or list,
        or None if parsing fails.
    """
    try:
        # Attempt direct parsing
        return json.loads(response_content)
    except json.JSONDecodeError:
        logger.warning("Initial JSON parsing failed. Attempting fallback extraction.")

    # Extract JSON-like structure (object or array)
    match = re.search(r"({.*}|\[.*\])", response_content, re.DOTALL)
    if not match:
        logger.error("No JSON-like content found.")
        return None

    candidate = match.group(0)

    try:
        # Remove trailing commas like ", }" or ", ]"
        clean_content = re.sub(r",\s*([}\]])", r"\1", candidate)
        return json.loads(clean_content)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON in fallback: {e}")
        return None


# ---------------------------------------------------------------------------
# Response-type validation (json / tabular / str / code)
# ---------------------------------------------------------------------------
def validate_response_type(
    response_content: Union[str, Any], expected_res_type: str
) -> Union[
    CodeResponse,
    JSONResponse,
    TabularResponse,
    TextResponse,
]:
    """
    Validates and structures the response content based on the expected response type.

    Args:
        response_content (Any): The raw response content from the LLM API.
        expected_res_type (str): The expected type of the response
            (e.g., "json", "tabular", "str", "code").

    Returns:
        Union[CodeResponse, JSONResponse, TabularResponse, TextResponse]:
            The validated and structured response as a Pydantic model instance.
    """

    if expected_res_type == "json":
        if isinstance(response_content, str):
            cleaned_content = clean_and_extract_json(response_content)
            if cleaned_content is None:
                raise ValueError("Failed to extract valid JSON from the response.")
        else:
            cleaned_content = response_content

        if isinstance(cleaned_content, (dict, list)):
            return JSONResponse(data=cleaned_content)
        else:
            raise TypeError(
                f"Expected dict or list for JSON response, got {type(cleaned_content)}"
            )

    elif expected_res_type == "tabular":
        if not isinstance(response_content, str):
            raise TypeError(
                "Tabular response must be provided as CSV text (str). "
                f"Got {type(response_content)}."
            )

        try:
            df = pd.read_csv(StringIO(response_content))
            return TabularResponse(data=df)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error parsing tabular data: {e}")
            raise ValueError("Response is not valid tabular data.") from e

    elif expected_res_type == "str":
        return TextResponse(content=str(response_content))

    elif expected_res_type == "code":
        return CodeResponse(code=str(response_content))

    else:
        raise ValueError(f"Unsupported response type: {expected_res_type}")


# ---------------------------------------------------------------------------
# JSON-type validation (web_summary / generic)
# ---------------------------------------------------------------------------
def validate_web_summary_response(response_model: JSONResponse) -> JSONResponse:
    """
    Validate a JSONResponse for the \"web_summary\" json_type using WebSummaryJSON.

    Strategy:
      - Expect response_model.data to be a dict.
      - Construct WebSummaryJSON(**data) to enforce structure.
      - Dump back to a cleaned dict and return a new JSONResponse(data=<cleaned_dict>).

    This preserves the JSONResponse type so downstream code that checks
    `isinstance(llm_response, JSONResponse)` continues to work unchanged.
    """
    response_data = response_model.data

    if response_data is None:
        raise ValueError("Response data is None and cannot be processed.")

    if not isinstance(response_data, dict):
        raise ValueError(
            f"Expected response_model.data to be a dictionary for web_summary, "
            f"got {type(response_data)}"
        )

    # Validate against WebSummaryJSON model
    summary_model = WebSummaryJSON(**response_data)

    # Support both Pydantic v1 and v2 style dumps
    if hasattr(summary_model, "model_dump"):
        cleaned_dict = summary_model.model_dump(exclude_none=True)
    else:
        cleaned_dict = summary_model.dict(exclude_none=True)

    validated_response = JSONResponse(data=cleaned_dict)
    logger.info("Validated web_summary JSON payload: %s", cleaned_dict)
    return validated_response


def validate_json_type(response_model: JSONResponse, json_type: str) -> JSONResponse:
    """
    Validates JSON data against a specific Pydantic model based on 'json_type'.

    Args:
        response_model (JSONResponse): The generic JSON response to validate.
        json_type (str): The expected JSON type ('web_summary' or 'generic').

    Returns:
        JSONResponse: Validated model instance (still a JSONResponse).

    Raises:
        ValueError: If 'json_type' is unsupported.
    """
    logger.info("Validating JSON type (%s).", json_type)

    json_model_mapping = {
        "web_summary": validate_web_summary_response,
        "generic": lambda model: model,  # Return as is, no extra validation
    }

    validator = json_model_mapping.get(json_type)
    if not validator:
        raise ValueError(f"Unsupported json_type: {json_type}")

    validated = validator(response_model)
    logger.info("JSON type (%s) validated.", json_type)
    return validated
