"""
File: models/llm_response_models.py
Last Updated on: 2025-12-02

Pydantic models for validating and structuring LLM responses.

This module provides a small set of generic response wrappers used
across the web summary demo:

- BaseResponseModel: common status + message fields
- JSONResponse: generic JSON payload wrapper (dict or list)
- TextResponse: plain-text content
- TabularResponse: pandas DataFrame wrapper
- CodeResponse: code snippet wrapper
- OptimizedTextData + EditingResponse: optional editing/rewriting shape
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class BaseResponseModel(BaseModel):
    """
    Base model that provides common fields for various response models.

    Attributes:
        status:
            Indicates the success status of the response, defaults to "success".
        message:
            Optional field to provide additional feedback or a message.

    Config:
        arbitrary_types_allowed:
            Allows non-standard types like pandas DataFrame.

    This base lets validation functions add status/message in a consistent way.
    """

    status: str = "success"
    message: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class JSONResponse(BaseModel):
    """
    General-purpose model for handling JSON-based responses.

    Attributes:
        data:
            Holds JSON data, which can be either:
              - a dictionary (single object), or
              - a list of dictionaries/values (array response).

    Config:
        arbitrary_types_allowed:
            Allows non-standard types in JSON responses if needed.
    """

    data: Union[Dict[str, Any], List[Any]]

    class Config:
        arbitrary_types_allowed = True


class TextResponse(BaseResponseModel):
    """
    Model for plain text responses.

    Attributes:
        content:
            Plain text content of the response.
    """

    content: str

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Text response processed.",
                "content": "This is the plain text content.",
            }
        }


class TabularResponse(BaseResponseModel):
    """
    Model for handling tabular data responses using pandas DataFrame.

    Attributes:
        data:
            The tabular data as a pandas DataFrame.

    Notes:
        This is primarily useful when the LLM returns CSV/TSV-like text that
        you parse into a DataFrame before passing it downstream.
    """

    data: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Tabular data processed.",
                "data": "Pandas DataFrame object",
            }
        }


class CodeResponse(BaseResponseModel):
    """
    Model for responses containing code snippets.

    Attributes:
        code:
            The code as a string.
    """

    code: str

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Code response processed.",
                "code": "print('Hello, world!')",
            }
        }
