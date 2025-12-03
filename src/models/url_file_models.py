from __future__ import annotations

from pathlib import Path
from typing import Optional, Union
import json
from pydantic import BaseModel, Field, HttpUrl, field_validator


class UrlEntry(BaseModel):
    """
    One URL entry from the seed JSON file.

    This is intentionally close to UrlRow but:
    - has no created_at/updated_at
    - is more forgiving about tag formats (list or csv string)
    """

    url: Union[HttpUrl, str]
    source: Optional[str] = Field(
        default=None,
        description="Where this URL came from (e.g. 'manual', 'csv_import').",
    )
    tags: Optional[list[str]] = Field(
        default=None,
        description="Optional list of tag strings.",
    )

    @field_validator("tags", mode="before")
    @classmethod
    def _normalize_tags(cls, v):
        """Accept list, comma-separated string, or None; normalize to list[str]."""
        if v is None:
            return None
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        # Any other single value → wrap in a list
        return [str(v).strip()]


class UrlFile(BaseModel):
    """
    Top-level model for the URL seed JSON.

    Supports either:
    - { "urls": [ { ... }, { ... } ] }
    - [ { ... }, { ... } ]
    """

    urls: list[UrlEntry]

    @classmethod
    def from_path(cls, path: Path | str) -> "UrlFile":
        """Function to load from json -> model"""
        path = Path(path)
        data = json.loads(path.read_text(encoding="utf-8"))

        # Flexible top-level shape
        if isinstance(data, list):
            return cls(urls=data)
        if isinstance(data, dict) and "urls" in data:
            return cls(urls=data["urls"])
        raise ValueError(
            f"Unsupported URL JSON format in {path}: "
            "expected a list of objects or a {'urls': [...]} wrapper."
        )
