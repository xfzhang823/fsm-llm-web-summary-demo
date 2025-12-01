"""
File: base_models.py
Last Updated on:

pydantic models for validate LLM responses
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    field_serializer,
    ConfigDict,
    field_validator,
)
import pandas as pd


# Set up logger
logger = logging.getLogger(__name__)


class BaseResponseModel(BaseModel):
    """
    Base model that provides common fields for various response models.

    Attributes:
        status (str): Indicates the success status of the response, defaults to "success".
        message (Optional[str]): Optional field to provide additional feedback or a message.

    Config:
        arbitrary_types_allowed (bool): Allows non-standard types like pandas DataFrame.

    *Allows validation functions to add status and message easily!
    """

    status: str = "success"
    message: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class JSONResponse(BaseModel):
    """
    General-purpose model for handling JSON-based responses.

    Attributes:
        data (Union[Dict[str, Any], List[Dict[str, Any]]]): Holds JSON data,
        which can be either a dictionary or a list of dictionaries.

    Config:
        arbitrary_types_allowed (bool): Allows non-standard types in
        JSON responses.
    """

    data: Union[Dict[str, Any], List[Dict[str, Any]]]

    class Config:
        arbitrary_types_allowed = True


class TextResponse(BaseResponseModel):
    """
    Model for plain text responses.

    Attributes:
        content (str): Holds plain text content of the response.

    Config:
        json_schema_extra (dict): Provides an example structure for documentation.
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
        data (pd.DataFrame): Contains the tabular data.

    Config:
        - arbitrary_types_allowed (bool): Allows DataFrame as a valid type.
        - json_schema_extra (dict): Example structure for tabular data documentation.
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
        code (str): Holds the code as a string.

    Config:
        json_schema_extra (dict): Example structure for code response documentation.
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


class OptimizedTextData(BaseModel):
    """Inner model to specify required 'optimized_text' field."""

    optimized_text: str = Field(..., description="The optimized text after editing.")


class EditingResponse(BaseResponseModel):
    """
    Model for responses involving text editing operations.

    Attributes:
        data (OptimizedTextData): Contains 'optimized_text' as a required field.
    """

    data: OptimizedTextData  # Use inner model to enforce 'optimized_text' structure

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Text editing processed successfully.",
                "data": {"optimized_text": "This is the optimized text after editing."},
            }
        }


class JobSiteData(BaseModel):
    """
    Inner model containing detailed job site information.
    """

    url: str | HttpUrl = Field(..., description="Job posting URL")
    job_title: str = Field(..., description="Job title")
    company: str = Field(..., description="Company name")
    location: Optional[str] = Field(None, description="Job location")
    salary_info: Optional[str] = Field(None, description="Salary information")
    posted_date: Optional[str] = Field(None, description="Job posting date")
    content: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Dictionary containing job description, responsibilities, "
            "and qualifications"
        ),
    )

    # -------------------------
    # URL serializer
    # -------------------------
    @field_serializer("url")
    def serialize_url(self, v: str | HttpUrl) -> str:
        return str(v)

    # -------------------------
    # LOCATION normalizer
    # -------------------------
    @field_validator("location", mode="before")
    @classmethod
    def _normalize_location(cls, v):
        """
        Accept:
            • str → keep as-is
            • list[str] → join into comma-separated string
            • None → None
        """
        if v is None:
            return None

        # If LLM returns e.g. ["Remote - Michigan", "Remote - Arizona"]
        if isinstance(v, list):
            return ", ".join(s for s in v if isinstance(s, str) and s.strip())

        # Anything else → convert to string and strip
        v = str(v).strip()
        return v or None

    # -------------------------
    # POSTED DATE normalizer
    # -------------------------
    @field_validator("posted_date", mode="before")
    @classmethod
    def _normalize_posted_date(cls, v):
        if v is None:
            return None

        # Coerce list to string
        if isinstance(v, list):
            return ", ".join(s for s in v if isinstance(s, str) and s.strip())

        # Convert to string
        s = str(v).strip()
        if s == "" or s.lower() in {"null", "none", "n/a", "na"}:
            return None

        # Already a date/datetime object
        if isinstance(v, (date, datetime)):
            return (v.date() if isinstance(v, datetime) else v).isoformat()

        # Try common date formats
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except ValueError:
                pass

        # Unparseable → treat as unknown
        return None


class JobSiteResponse(BaseResponseModel):
    """
    Model for handling job site response data, standardizing job-related information.

    Attributes:
        data (JobSiteData): Holds detailed job site information as a nested
        JobSiteData instance.

    The `JobSiteResponse` model wraps structured job posting information in
    a standard response format used across pipelines. This includes the job title,
    company, location, salary, and parsed content such as responsibilities and
    qualifications.

    ✅ Example Output:
    -------------------
    {
        "status": "success",
        "message": "Job site data processed successfully.",
        "data": {
            "url": "https://example.com/job-posting",
            "job_title": "Software Engineer",
            "company": "Tech Corp",
            "location": "San Francisco, CA",
            "salary_info": "$100,000 - $120,000",
            "posted_date": "2024-11-08",
            "content": {
                "description": "We are looking for a Software Engineer...",
                "responsibilities": [
                    "Develop software",
                    "Collaborate with team"
                ],
                "qualifications": [
                    "BS in Computer Science",
                    "2+ years experience"
                ]
            }
        }
    }

    This model is used as a standardized return type after scraping or LLM-parsing
    a job webpage (e.g., in the `process_single_url_async` pipeline step).
    """

    data: JobSiteData

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Job site data processed successfully.",
                "data": {
                    "url": "https://example.com/job-posting",
                    "job_title": "Software Engineer",
                    "company": "Tech Corp",
                    "location": "San Francisco, CA",
                    "salary_info": "$100,000 - $120,000",
                    "posted_date": "2024-11-08",
                    "content": {
                        "description": "We are looking for a Software Engineer...",
                        "responsibilities": [
                            "Develop software",
                            "Collaborate with team",
                        ],
                        "qualifications": [
                            "BS in Computer Science",
                            "2+ years experience",
                        ],
                    },
                },
            }
        }


class NestedRequirements(BaseModel):
    """
    Model to handle nested job requirements structure as provided by the Claude API.

    The response structure is nested, where each section contains multiple items,
    each represented by a key-value pair (the key being a numeric identifier and
    the value being the actual requirement text).
    """

    pie_in_the_sky: List[str] = Field(
        ..., description="High-level pie-in-the-sky requirements."
    )
    down_to_earth: List[str] = Field(
        ..., description="Realistic requirements that are directly applicable."
    )
    bare_minimum: List[str] = Field(
        ..., description="Essential or bare minimum qualifications."
    )
    cultural_fit: List[str] = Field(
        ..., description="Requirements related to cultural fit."
    )
    other: List[str] = Field([], description="Any other requirements.")

    class Config:
        json_schema_extra = {
            "example": {
                "pie_in_the_sky": [
                    "10+ years in emerging tech",
                    "PhD from top-tier university",
                ],
                "down_to_earth": [
                    "3+ years in related field",
                    "2",
                    "Proficiency in Excel",
                ],
                "bare_minimum": [
                    "Bachelor's degree",
                    "Customer service experience",
                ],
                "cultural_fit": ["Team player", "Committed to diversity"],
                "other": [],
            }
        }


class RequirementsResponse(BaseResponseModel):
    """
    Response model for job requirements extracted from job descriptions.

    Attributes:
        data (NestedRequirements): Structured job requirements organized into
            high-level categories such as "pie_in_the_sky", "down_to_earth",
            "bare_minimum", "cultural_fit", and "other".

    This model is used after parsing and structuring job requirements into logical tiers.
    It serves as the output format for requirement extraction steps in the job parsing
    pipeline.

    ✅ Example Output:
    -------------------
    {
        "status": "success",
        "message": "Job requirements data processed successfully.",
        "data": {
            "pie_in_the_sky": [
                "Top-tier strategy consulting experience",
                "Motivated by high impact, high visibility work"
            ],
            "down_to_earth": [
                "Bachelor's degree and MBA required",
                "3-5 years of work experience preferred",
                "Strong analytical and communication skills"
            ],
            "bare_minimum": [
                "Bachelor's degree",
                "Ability to manage multiple priorities"
            ],
            "cultural_fit": [
                "Excited about contributing to a dynamic team"
            ],
            "other": [
                "Insurance industry experience is a plus"
            ]
        }
    }

    The `data` field wraps a `NestedRequirements` model and reflects categorized insights from the
    job post. This structure is used throughout the alignment, editing, and evaluation stages.
    """

    data: NestedRequirements

    model_config = ConfigDict(
        extra="ignore",
        use_enum_values=True,
        json_schema_extra={
            "example": {
                "status": "success",
                "message": "Job requirements data processed successfully.",
                "data": {
                    "pie_in_the_sky": [
                        "Top-tier strategy consulting experience",
                        "Motivated by high impact, high visibility work",
                    ],
                    "down_to_earth": [
                        "Bachelor's degree and MBA degree required",
                        "3-5 years of work experience preferred",
                    ],
                    "bare_minimum": [
                        "Bachelor's degree",
                        "Ability to manage multiple priorities",
                    ],
                    "cultural_fit": ["Excited about contributing to a dynamic team"],
                    "other": ["Insurance industry experience is a plus"],
                },
            },
        },
    )

    @property
    def requirements_dict(self) -> Dict[str, List[str]]:
        """
        Canonical, cleaned dict view of requirements:
        - keys: category names
        - values: non-empty, stripped strings
        """
        raw = self.data.model_dump(exclude_none=True)  # dict from NestedRequirements
        out: Dict[str, List[str]] = {}
        for cat, items in raw.items():
            if isinstance(items, list):
                cleaned = [s.strip() for s in items if isinstance(s, str) and s.strip()]
                if cleaned:
                    out[str(cat)] = cleaned
        return out

    def ensure_requirements_dict(self) -> Dict[str, List[str]]:
        """Return the normalized dict, raising if it’s empty."""
        rd = self.requirements_dict
        if not rd or sum(len(v) for v in rd.values()) == 0:
            raise ValueError("RequirementsResponse has no non-empty requirements.")
        return rd
