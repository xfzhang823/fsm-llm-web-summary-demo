"""
models/db_table_models.py

Pydantic models for ALL DuckDB tables used in the *web summary demo*.

Data model (4 tables)
---------------------
1) url
   - Canonical list of URLs to process
   - Fields: url, source, tags, created_at, updated_at

2) pipeline_control
   - FSM state + lease metadata (one row per (url, iteration))
   - Fields: url, iteration, stage, status, task_state,
             is_claimed, worker_id, lease_until,
             notes,
             source_file, created_at, updated_at

3) web_page
   - Raw and cleaned page text (fetch result)
   - Fields: url, iteration, status_code, html, clean_text,
             fetched_at, fetch_error, created_at, updated_at

4) web_summary
   - LLM output and metadata
   - Fields: url, iteration, summary_text, summary_json,
             llm_provider, model_id, tokens_prompt,
             tokens_completion, tokens_total,
             created_at, updated_at

Mixins
------
- Mixins (TimestampedMixin, LLMStampedMixin) DO NOT inherit from BaseModel.
- AppBaseModel is the ONLY base that subclasses pydantic.BaseModel.
- Always list mixins BEFORE AppBaseModel in class bases.

Design principles
-----------------
- Each table has only the fields it truly needs.
- `pipeline_control` is FSM-only (no history; no provider/model metadata).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Union, Any, Dict
import math

import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    ValidationInfo,
)

from fsm.pipeline_enums import (
    PipelineStage,
    PipelineStatus,
    PipelineTaskState,
    LLMProvider,
)

# ──────────────────────────────────────────────────────────────────────────────
# Base & PURE mixins (no BaseModel inheritance in mixins)
# ──────────────────────────────────────────────────────────────────────────────


class AppBaseModel(BaseModel):
    """Project-wide defaults for parsing/serialization behavior."""

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        use_enum_values=False,  # keep Enums as Enum objects internally
    )


class TimestampedMixin:
    """
    Opt-in audit timestamps.

    Use ONLY where you truly need them:
    - created_at: set on insert
    - updated_at: set on update
    """

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] | None = None


class LLMStampedMixin:
    """Opt-in LLM metadata for artifacts produced by an LLM."""

    llm_provider: LLMProvider = Field(default=LLMProvider.OPENAI)
    model_id: Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# Control-plane (FSM snapshot ONLY — no history here)
# ──────────────────────────────────────────────────────────────────────────────


class PipelineState(TimestampedMixin, AppBaseModel):
    """
    One row in `pipeline_control`.

    Conventions
    -----------
    - stage:       UPPER-CASE enum value (e.g., "URL", "WEB_PAGE", "WEB_SUMMARY")
    - status:      UPPER-CASE enum value (e.g., "NEW", "IN_PROGRESS", "COMPLETED")
    - task_state:  UPPER-CASE enum value (e.g., "READY", "PAUSED", "SKIP", "HOLD")

    Purpose
    -------
    Tracks a URL's progress through atomic FSM stages with a *human gate*
    (`task_state`) and *machine lease* fields (claim/lease).

    Cleanups & Coercions (automatic)
    --------------------------------
    - `iteration`      → int (handles float64/str/None)
    - `stage`          → PipelineStage (forced UPPER)
    - `status`         → PipelineStatus (forced UPPER)
    - `task_state`     → PipelineTaskState (forced UPPER + legacy mapping):
        DONE/COMPLETED → HOLD, SKIPPED → SKIP, RUNNING/RETRY/ABANDONED → READY
    - `is_claimed`     → bool (accepts 0/1/"true"/"false")
    - `lease_until`    → datetime (accepts str/pandas.Timestamp/None)
    - `worker_id`      → None if blank/whitespace
    - `url`            → HttpUrl **or** str (Union), preserves non-standard URLs

    Invariants
    ----------
    - task_state ∈ {READY, PAUSED, SKIP, HOLD}
    - status ∈ {NEW, IN_PROGRESS, COMPLETED, ERROR, SKIPPED}

    Notes
    -----
    Prefer using this model to *load* from DuckDB/pandas and to *persist* back,
    so normalization stays centralized here.
    """

    url: Union[HttpUrl, str]
    iteration: int = 0
    stage: PipelineStage
    status: PipelineStatus = Field(default=PipelineStatus.NEW)

    task_state: PipelineTaskState = Field(default=PipelineTaskState.READY)

    is_claimed: bool = False
    worker_id: Optional[str] = None
    lease_until: Optional[datetime] = None

    notes: Optional[str] = None
    source_file: Optional[str] = None

    # ---------- SINGLE iteration validator ----------
    @field_validator("iteration", mode="before")
    @classmethod
    def _coerce_iteration(cls, v: Any) -> int:
        if v is None:
            return 0
        if isinstance(v, int):
            return v
        try:
            # handles "0", "0.0", numpy/pandas floats
            return int(float(v))
        except Exception:
            return 0

    @field_validator("stage", mode="before")
    @classmethod
    def _coerce_stage(cls, v: Any) -> PipelineStage:
        if isinstance(v, PipelineStage):
            return v
        if v is None:
            raise ValueError("stage is required")
        return PipelineStage(str(v).strip().upper())

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v: Any) -> PipelineStatus:
        if isinstance(v, PipelineStatus):
            return v
        if v is None:
            return PipelineStatus.NEW
        return PipelineStatus(str(v).strip().upper())

    @field_validator("task_state", mode="before")
    @classmethod
    def _coerce_task_state(cls, v: Any, info: ValidationInfo) -> PipelineTaskState:
        if isinstance(v, PipelineTaskState):
            return v
        if v is None or str(v).strip() == "":
            status = info.data.get("status")
            if status in (
                PipelineStatus.COMPLETED,
                getattr(PipelineStatus, "SKIPPED", None),
            ):
                return PipelineTaskState.HOLD
            return PipelineTaskState.READY

        s = str(v).strip().upper()
        legacy_map = {
            "DONE": "HOLD",
            "COMPLETED": "HOLD",
            "SKIPPED": "SKIP",
            "RUNNING": "READY",
            "RETRY": "READY",
            "ABANDONED": "READY",
        }
        s = legacy_map.get(s, s)
        return PipelineTaskState(s)

    @field_validator("is_claimed", mode="before")
    @classmethod
    def _coerce_is_claimed(cls, v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        s = str(v).strip().lower()
        if s in {"1", "true", "t", "yes", "y"}:
            return True
        if s in {"0", "false", "f", "no", "n"}:
            return False
        try:
            return bool(int(v))
        except Exception:
            return False

    @field_validator("lease_until", mode="before")
    @classmethod
    def _coerce_lease_until(cls, v: Any) -> Optional[datetime]:
        if v is None or v == "":
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        try:
            return pd.to_datetime(v).to_pydatetime()
        except Exception:
            return None

    @field_validator("worker_id", mode="before")
    @classmethod
    def _normalize_worker_id(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    @field_validator("url", mode="before")
    @classmethod
    def _coerce_url(cls, v: Any) -> Union[HttpUrl, str]:
        if v is None:
            raise ValueError("url is required")
        # If you want strict HttpUrl validation, return v and let pydantic check.
        # If you want to allow non-standard URLs, keep returning str:
        return str(v).strip()


# ──────────────────────────────────────────────────────────────────────────────
# URL registry (seed list; NO iteration)
# ──────────────────────────────────────────────────────────────────────────────


class UrlRow(TimestampedMixin, AppBaseModel):
    """
    Canonical registry of URLs to process.

    This table is intentionally small: iteration does NOT belong here.
    """

    url: Union[HttpUrl, str]
    source: Optional[str] = Field(
        default=None,
        description="Where this URL came from (e.g. 'manual', 'csv_import').",
    )
    tags: Optional[str] = Field(
        default=None,
        description="Optional free-form tags (comma-separated or JSON string).",
    )


# ─────────────────────────────────────────────────────────────────────────────-
# Stage/data tables
# ─────────────────────────────────────────────────────────────────────────────-


class WebPageRow(TimestampedMixin, AppBaseModel):
    """
    Raw and cleaned page text for a single fetch attempt.

    One row per (url, iteration).
    """

    url: Union[HttpUrl, str]
    iteration: int = 0

    status_code: Optional[int] = None
    html: Optional[str] = Field(
        default=None,
        description="Raw HTML returned by the HTTP client.",
    )
    clean_text: Optional[str] = Field(
        default=None,
        description="Extracted plain text / readability-cleaned content.",
    )
    fetched_at: Optional[datetime] = None
    fetch_error: Optional[str] = Field(
        default=None,
        description="Error message if fetch/parse failed.",
    )

    @field_validator("iteration", mode="before")
    @classmethod
    def _coerce_iteration(cls, v: Any) -> int:
        return PipelineState._coerce_iteration(v)  # reuse logic


class WebSummaryRow(LLMStampedMixin, TimestampedMixin, AppBaseModel):
    """
    LLM-generated summary and metadata for a URL.

    One row per (url, iteration, llm_provider, model_id).
    """

    url: Union[HttpUrl, str]
    iteration: int = 0

    summary_text: Optional[str] = Field(
        default=None,
        description="Human-readable summary text of the web page.",
    )
    summary_json: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional structured summary payload (JSON object).",
    )

    tokens_prompt: Optional[int] = None
    tokens_completion: Optional[int] = None
    tokens_total: Optional[int] = None

    @field_validator("iteration", mode="before")
    @classmethod
    def _coerce_iteration(cls, v: Any) -> int:
        return PipelineState._coerce_iteration(v)  # reuse logic


# ─────────────────────────────────────────────────────────────────────────────-
# Explicit export list
# ─────────────────────────────────────────────────────────────────────────────-


__all__ = [
    # Core tables
    "PipelineState",
    "UrlRow",
    "WebPageRow",
    "WebSummaryRow",
    # Bases / mixins
    "AppBaseModel",
    "TimestampedMixin",
    "LLMStampedMixin",
]
