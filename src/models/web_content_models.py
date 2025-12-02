"""
models/web_content_models.py

Structured content models for the *web summary demo* project.

Purpose
-------
These models represent the **content-layer** of the pipeline—the structured,
in-memory objects used for LLM summarization (Stage B). These models are
*separate* from the DuckDB storage tables (`web_page`, `web_summary`) and serve
as the clean, unified interface between:

  • Stage A (scraping → WebPageRow in DuckDB)
  • Stage B (summarization → WebSummaryRow in DuckDB)

Why a separate content model?
-----------------------------
The `WebPageRow` table stores raw strings and fetch metadata. For high-quality
LLM summarization, we prefer a normalized, structured representation:

  - URL
  - Clean text
  - Optional extracted title/headings
  - Optional metadata
  - Optional raw HTML (future: readability extraction)

This mirrors the `JobSiteResponse` → `JobPostingsBatch` pattern in job_bot,
but intentionally much lighter.

Models
------
1. WebPageContent
      Structured representation of a scraped page (LLM input).

2. WebPageBatch
      RootModel container mapping URL → WebPageContent.

3. WebSummaryJSON
      Optional structured LLM summary output (JSON form), complementary to
      the free-text `summary_text` stored in `web_summary`.

These are used only in memory and are **not** directly written to DuckDB.
"""

from __future__ import annotations

import logging
from typing import Optional, List, Dict, Union
from pydantic import BaseModel, Field, HttpUrl, RootModel

logger = logging.getLogger(__name__)


class WebPageContent(BaseModel):
    """
    Structured representation of a fetched webpage.
    Used as the LLM input model for summarization (Stage B).

    This model is NOT stored directly in DuckDB.

    Flow:
        Stage A → WebPageRow (DB)
        Stage B → load WebPageRow → build WebPageContent → pass to LLM

    Fields:
        url:       Source URL.
        title:     Extracted or heuristic title (optional).
        clean_text: Core plain text extracted from the webpage (required).
        headings:  Optional list of page headings (h1/h2/h3).
        metadata:  Optional dict for extra context (author, date, site info).
        raw_html:  Optional raw HTML for future processing.
    """

    url: Union[str, HttpUrl]

    title: Optional[str] = Field(
        default=None, description="Extracted or heuristic page title."
    )

    clean_text: str = Field(
        ..., description="Cleaned plain text extracted from the webpage."
    )

    # Optional structured fields for future expansion
    headings: Optional[List[str]] = Field(
        default=None, description="Top-level extracted headings (h1/h2/h3)."
    )

    metadata: Optional[Dict[str, str]] = Field(
        default=None, description="Optional metadata extracted from page."
    )

    raw_html: Optional[str] = Field(
        default=None, description="Raw HTML if available for deeper processing."
    )


class WebPageBatch(RootModel[Dict[str, WebPageContent]]):
    """
    Container for multiple WebPageContent objects keyed by URL.

    This mirrors job_bot's batch models (e.g., JobPostingsBatch), enabling
    consistent handling of multi-item LLM operations or batched processing.
    """

    pass


class WebSummaryJSON(BaseModel):
    """
    Optional structured summary produced by an LLM.

    This format complements the free-text `summary_text` stored in the
    `web_summary` table. It is validated and stored as JSON in the
    `summary_json` column of WebSummaryRow if used.

    Fields:
        title:      Optional LLM-generated title or headline.
        bullets:    Main bullet-point summary.
        key_points: Optional list of emphasized key takeaways.
        sections:   Optional hierarchical structure (e.g. intro/body/outro).
    """

    title: Optional[str] = None
    bullets: List[str] = Field(default_factory=list)
    key_points: Optional[List[str]] = None
    sections: Optional[Dict[str, List[str]]] = None
