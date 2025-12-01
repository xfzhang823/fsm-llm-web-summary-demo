"""
Filename: validate_json_type_response.py
Last updated: 2025 Feb

This module contains the logic for processing and validating various types
of response data
from the LLM API.

It defines several functions that handle validation based on the response type
(e.g., JSON, tabular, text, or code).

The module ensures that the response data is appropriately structured and validated
according to its type.

*Key Functions:
1. **`validate_response_type`**:
   - The **first step** in the validation process. It validates and structures
   the raw response content based on the expected response type (e.g., "json",
   "tabular", "str", "code").
   - It checks the type of the response content and parses it accordingly:
     - If `json`, it cleans and extracts valid JSON.
     - If `tabular`, it parses the content as tabular data (CSV).
     - If `str`, it wraps the content as plain text.
     - If `code`, it wraps the content as code.

2. **`validate_json_type`**:
   - Once the raw response has been structured by `validate_response_type`, this function
   is called to validate the **structured JSON data** based on the specific `json_type`
   (e.g., "job_site", "editing", "requirements").
   - It ensures that the response conforms to the expected model (e.g.,
   `JobSiteResponseModel`, `EditingResponseModel`, `RequirementsResponse`).

3. **Specific Response Validation Functions**:
   - `validate_editing_response`: Validates a response of type "editing" and ensures that
   the `data` matches the `OptimizedTextData` model.
   - `validate_job_site_response`: Validates a response of type "job_site" and ensures that
   the `data` matches the `JobSiteData` model.
   - `validate_requirements_response`: Validates a response of type "requirements"
   and ensures that the `data` matches the `Requirements` model.

*Overview of json type validation logic:
The module provides:
- Specific validation functions for each type of response (e.g., `validate_job_site_response`,
'validate_editing_response`, etc.).
- A centralized `validate_json_type` function that maps the provided `json_type` to
the appropriate validation function.
- A unified approach for handling validation for responses from the LLM API,
regardless of their type.

*Steps in Validation:
1. **Validation Functions**:
    - `validate_editing_response`: Validates responses of type "editing" and ensures
    the response `data` matches the expected structure of `OptimizedTextData`.
    - `validate_job_site_response`: Validates responses of type "job_site" and ensures
    the response `data` matches the expected structure of `JobSiteData`.
    - `validate_requirements_response`: Validates responses of type "requirements"
    and ensures the response `data` matches the expected structure of `Requirements`.

2. **Unified Validation Interface**:
    - `validate_json_type`: This function is responsible for determining which validation
    function to call based on the provided `json_type`. It maps each `json_type` to
    the corresponding validation function and invokes it to validate the response.
    This function returns the validated model instance.

*Logic Flow:
1. When a response is received, after it is validated as a JSON type, it is passed to
the `validate_json_type` function, specifying the type of the response (e.g.,
"job_site", "editing", or "requirements").
2. The function checks which validation function to use by mapping the `json_type` to
the appropriate validation function in the `json_model_mapping` dictionary.
3. The corresponding validation function is called, which processes and validates
the response data.
4. Each validation function performs the following:
    - Checks if the `data` field is present and of the expected type (usually a dictionary).
    - Validates the data using the appropriate Pydantic model.
    - Returns the validated response model (e.g., `JobSiteResponseModel`,
    `EditingResponseModel`, `RequirementsResponse`).
5. If the data is invalid, an error is raised, and a `ValueError` is thrown indicating
the specific issue.

### Example Usage:
Once the module is imported, you can call the `validate_json_type` function in your code
to validate responses:

```python
from response_validators import validate_json_type

# Assuming response_model is already populated
validated_response = validate_json_type(response_model, json_type="requirements")
"""

from io import StringIO
import re
import json
import logging
from typing import Any, Union, Optional, List, Dict
from pydantic import ValidationError
import pandas as pd

from job_bot.models.llm_response_models import (
    JobSiteData,
    OptimizedTextData,
    CodeResponse,
    TabularResponse,
    TextResponse,
    JSONResponse,
    JobSiteResponse,
    EditingResponse,
    NestedRequirements,
    RequirementsResponse,
)


logger = logging.getLogger(__name__)


# Parsing & validation utils functions
# Function to clean/extract JSON content
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
    match = re.search(r"({.*}|\\[.*\\])", response_content, re.DOTALL)
    if not match:
        logger.error("No JSON-like content found.")
        return None

    try:
        # Remove trailing commas
        clean_content = re.sub(r",\\s*([}\\]])", r"\\1", match.group(0))
        return json.loads(clean_content)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON in fallback: {e}")
        return None


# Response type validation
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
            - CodeResponse: Returned when expected_res_type is "code", wraps code content.
            - JSONResponse, JobSiteResponseModel, or EditingResponseModel:
              Returned when expected_res_type is "json", based on json_type.
            - TabularResponse: Returned when expected_res_type is "tabular", wraps a DataFrame.
            - TextResponse: Returned when expected_res_type is "str", wraps plain text content.
    """

    if expected_res_type == "json":
        # Check if response_content is a string that needs parsing
        if isinstance(response_content, str):
            # Only parse if it's a string
            cleaned_content = clean_and_extract_json(response_content)
            if cleaned_content is None:
                raise ValueError("Failed to extract valid JSON from the response.")
        else:
            # If it's already a dict or list, use it directly
            cleaned_content = response_content

        # Create a JSONResponse instance with the cleaned content
        if isinstance(cleaned_content, (dict, list)):
            return JSONResponse(data=cleaned_content)
        else:
            raise TypeError(
                f"Expected dict or list for JSON response, got {type(cleaned_content)}"
            )

    elif expected_res_type == "tabular":
        try:
            # Parse as DataFrame and wrap in TabularResponse model
            df = pd.read_csv(StringIO(response_content))
            return TabularResponse(data=df)
        except Exception as e:
            logger.error(f"Error parsing tabular data: {e}")
            raise ValueError("Response is not valid tabular data.")

    elif expected_res_type == "str":
        # Wrap text response in TextResponse model
        return TextResponse(content=response_content)

    elif expected_res_type == "code":
        # Wrap code response in CodeResponse model
        return CodeResponse(code=response_content)

    else:
        raise ValueError(f"Unsupported response type: {expected_res_type}")


def validate_editing_response(response_model: JSONResponse) -> EditingResponse:
    """
    Processes and validates a JSON response for the "editing" type.

    This function ensures that the "data" field in the response is an instance
    of OptimizedTextData.

    Args:
        response_model (JSONResponse): The generic JSON response model to validate.

    Returns:
        EditingResponseModel: The validated EditingResponseModel instance containing
        "optimized_text".

    Raises:
        ValueError: If the data is None or not an instance of OptimizedTextData.
    """
    response_data = response_model.data  # parse -> data (dictionary)

    if response_data is None:
        raise ValueError("Response data is None and cannot be processed.")

    if not isinstance(response_data, dict):
        raise ValueError(
            f"Expected response_data_model to be a dictionary, got {type(response_data)}"
        )

    parsed_data = OptimizedTextData(**response_data)
    validated_response_model = EditingResponse(
        status="success",
        message="Text editing processed successfully.",
        data=parsed_data,
    )

    logger.info(f"Validated response model - editing: {validated_response_model}")

    return validated_response_model


def validate_job_site_response(response_model: JSONResponse) -> JobSiteResponse:
    """
    Processes and validates a JSON response for the "job_site" type.

    This function ensures that the "data" field in the response is an instance of
    JobSiteData.

    Args:
        response_model (JSONResponse): The generic JSON response model to validate.

    Returns:
        JobSiteResponseModel: The validated JobSiteResponseModel instance containing
        job-specific data.

    Raises:
        ValueError: If the "data" field is None or not an instance of JobSiteData.
    """
    response_data = response_model.data  # parse -> data dictionary

    if response_data is None:
        raise ValueError("Response data is None and cannot be processed.")

    if not isinstance(response_data, dict):
        raise ValueError(
            f"Expected response_data_model to be a dictionary, got {type(response_data)}"
        )

    try:
        # Parse the data field into JobSiteData
        parsed_data = JobSiteData(**response_data)

    except ValidationError as e:
        raise ValueError(f"Invalid data structure for job site response: {e}")

    validated_response_model = JobSiteResponse(
        status="success",
        message="Job site data processed successfully.",
        data=parsed_data,
    )

    logger.info(f"Validated response model - job site: {validated_response_model}")

    return validated_response_model


def validate_requirements_response(
    response_model: JSONResponse,
) -> RequirementsResponse:
    """
    Validates and processes a JSON response for the "requirements" type.

    Args:
        response_model (JSONResponse): The generic JSON response model to validate.

    Returns:
        RequirementsResponse: The validated RequirementsResponseModel instance.

    Raises:
        ValueError: If the data is None or not an instance of the Requirements model.
    """
    response_data = response_model.data  # Get the raw data

    if response_data is None:
        raise ValueError("Response data is None and cannot be processed.")

    if not isinstance(response_data, dict):
        raise ValueError(
            f"Expected response_data_model to be a dictionary, got {type(response_data)}"
        )

    # Validate the data using the Requirements model
    try:
        parsed_data = NestedRequirements(**response_data)
    except ValidationError as e:
        raise ValueError(f"Invalid structure for requirements: {e}")

    validated_response_model = RequirementsResponse(
        status="success",
        message="Job site data processed successfully.",
        data=parsed_data,
    )

    logger.info(f"Validated response model - job site: {validated_response_model}")

    return validated_response_model


def validate_json_type(
    response_model: JSONResponse, json_type: str
) -> Union[JobSiteResponse, EditingResponse, JSONResponse, RequirementsResponse]:
    """
    Validates JSON data against a specific Pydantic model based on 'json_type'.

    Args:
        - response_model (JSONResponse): The generic JSON response to validate.
        - json_type (str): The expected JSON type ('job_site', 'editing', 'requirements',
        or 'generic').

    Returns:
        Union[JobSiteResponseModel, EditingResponseModel, JSONResponse]:
        Validated model instance.

    Raises:
        ValueError: If 'json_type' is unsupported or validation fails.
    """
    logger.info(f"Validating JSON type ({json_type}).")
    # Map json_type to the correct model class
    json_model_mapping = {
        "editing": validate_editing_response,
        "job_site": validate_job_site_response,
        "requirements": validate_requirements_response,
        "generic": lambda model: model,  # Return as is
    }

    # Pick the right function
    validator = json_model_mapping.get(json_type)
    if not validator:
        raise ValueError(f"Unsupported json_type: {json_type}")

    logger.info(f"JSON type ({json_type}) validated.")
    return validator(response_model)
