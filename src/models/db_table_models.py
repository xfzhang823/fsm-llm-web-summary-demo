"""
duckdb_table_models.py

Refactored with PURE mixins to avoid MRO issues:
- Mixins (TimestampedMixin, LLMStampedMixin) DO NOT inherit from BaseModel.
- AppBaseModel is the ONLY base that subclasses pydantic.BaseModel.
- Always list mixins BEFORE AppBaseModel in class bases.

Design principles:
- Each table has only the fields it truly needs.
- `pipeline_control` is FSM-only (no history; no provider).
- `job_urls` is a seed list (no iteration).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Union, Any
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


# User defined
from job_bot.db_io.pipeline_enums import (
    PipelineStage,
    PipelineStatus,
    PipelineTaskState,
    Version,
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


class TimestampedMixin(BaseModel):
    """Opt-in audit timestamps. Use ONLY where you truly need them."""

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] | None = None


class LLMStampedMixin:
    """Opt-in LLM metadata for artifacts produced by an LLM."""

    llm_provider: LLMProvider = Field(default=LLMProvider.OPENAI)
    model_id: Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# Control-plane (FSM snapshot ONLY — no history here)
# ──────────────────────────────────────────────────────────────────────────────


class PipelineState(AppBaseModel, TimestampedMixin):
    """
    One row in `pipeline_control`.

    Conventions
    -----------
    - stage:       UPPER-CASE enum value (e.g., "JOB_URLS", "JOB_POSTINGS")
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
    - `transition_flag`→ int in {0,1} (defaults to 0)
    - `lease_until`    → datetime (accepts str/pandas.Timestamp/None)
    - `worker_id`      → None if blank/whitespace
    - `url`            → HttpUrl **or** str (Union), preserves non-standard job-board URLs

    Invariants
    ----------
    - task_state ∈ {READY, PAUSED, SKIP, HOLD}
    - status ∈ {NEW, IN_PROGRESS, COMPLETED, ERROR, SKIPPED}

    Notes
    -----
    Prefer using this model to *load* from DuckDB/pandas and to *persist* back,
    so normalization stays centralized here.
    """

    # If you want enums to serialize as their .value automatically:
    # model_config = ConfigDict(use_enum_values=True)

    url: Union[HttpUrl, str]  # or just str, if you don't want strict URL checks
    iteration: int = 0
    stage: PipelineStage
    status: PipelineStatus = Field(default=PipelineStatus.NEW)

    task_state: PipelineTaskState = Field(default=PipelineTaskState.READY)

    is_claimed: bool = False
    worker_id: Optional[str] = None
    lease_until: Optional[datetime] = None

    decision_flag: Optional[int] = None  # legacy
    transition_flag: int = 0  # 0|1
    notes: Optional[str] = None
    source_file: Optional[str] = None
    version: Optional["Version"] = None  # keep Enum in-code

    @property
    def version_str(self) -> Optional[str]:
        return self.version.value if self.version else None

    # ---------- SINGLE iteration validator (keep only this) ----------
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

    # ---------- FLAGS: clamp to {0,1} and never return None ----------
    @field_validator("transition_flag", mode="before")
    @classmethod
    def _coerce_transition_flag(cls, v: Any) -> int:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return 0
        try:
            return 1 if int(float(v)) != 0 else 0
        except Exception:
            s = str(v).strip().lower()
            return 1 if s in {"true", "t", "yes", "y"} else 0

    # ---------- VERSION: accept Enum, int/float, or strings ----------
    @field_validator("version", mode="before")
    @classmethod
    def _coerce_version(cls, v: Any):
        if v is None:
            return None
        # Already an Enum instance
        from job_bot.db_io.pipeline_enums import Version  # adjust import path if needed

        if isinstance(v, Version):
            return v
        # float/nan -> None or int
        if isinstance(v, float):
            if math.isnan(v):
                return None
            v = int(v)
        # ints -> try IntEnum mapping
        if isinstance(v, int):
            try:
                return Version(v)
            except Exception:
                # If your Version is StrEnum ('v1'), map int -> f"v{int}"
                try:
                    return Version(f"v{v}")
                except Exception:
                    return None
        # strings -> try by name/value
        if isinstance(v, str):
            s = v.strip()
            if s == "" or s.lower() in {"null", "none", "n/a"}:
                return None
            # allow "1" -> Version(1), or "v1" -> Version("v1"), or "V1" name
            if s.lstrip("-").isdigit():
                try:
                    return Version(int(s))
                except Exception:
                    pass
            try:
                return Version(s)  # value
            except Exception:
                try:
                    return Version[s.upper()]  # name
                except Exception:
                    return None
        # Anything else -> give up
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Input table (seed list; no iteration)
# ──────────────────────────────────────────────────────────────────────────────


class JobUrlsRow(AppBaseModel, TimestampedMixin):
    """
    Registry of job posting URLs. Iteration does NOT belong here.
    """

    url: Union[HttpUrl, str]
    # Optional descriptive metadata (kept minimal)
    company: Optional[str] = None
    job_title: Optional[str] = None
    source_file: Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# Stage/data bases
# ──────────────────────────────────────────────────────────────────────────────


class BaseStageRow(AppBaseModel):
    """
    Minimal shared fields for per-URL, per-iteration stage tables.
    Intentionally excludes timestamps and LLM metadata.
    """

    url: Union[HttpUrl, str]
    iteration: int = 0
    # Optional provenance/versioning knobs (only if you actually use them)
    source_file: Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# Stage/data tables
# ──────────────────────────────────────────────────────────────────────────────


class JobPostingsRow(BaseStageRow, LLMStampedMixin, TimestampedMixin):
    """
    Raw scraped job posting content in flat format.
    Timestamps are useful here for freshness/debugging.
    """

    job_title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    salary_info: Optional[str] = None
    posted_date: Optional[str] = None
    content: Optional[str] = Field(
        default=None,
        description="Full job description text or serialized JSON content.",
    )


class ExtractedRequirementsRow(BaseStageRow, LLMStampedMixin):
    """
    Structured job requirements extracted from a job posting.
    """

    requirement_key: str
    requirement: str
    # Optional grouping you actually use
    requirement_category: Optional[str] = None
    requirement_category_key: Optional[int] = None  # per-category order


class FlattenedRequirementsRow(BaseStageRow, LLMStampedMixin):
    """
    Flattened (normalized) job requirements used for downstream matching.
    """

    requirement_key: str
    requirement: str


class FlattenedResponsibilitiesRow(BaseStageRow):
    """
    Flattened canonical resume responsibilities (provider-agnostic).
    """

    responsibility_key: str
    responsibility: str

    # Optional resume structuring fields (only if used)
    # section: Optional[str] = None
    # role_title: Optional[str] = None
    # start_year: Optional[int] = None
    # end_year: Optional[int] = None


class PrunedResponsibilitiesRow(BaseStageRow):
    """
    Responsibilities pruned via rules/review (still provider-agnostic).
    """

    responsibility_key: str
    responsibility: str
    pruned_by: str = Field(
        ..., description="Who/what performed pruning (e.g., 'llm', 'xfz', 'heuristic')."
    )


class EditedResponsibilitiesRow(BaseStageRow, LLMStampedMixin, TimestampedMixin):
    """
    LLM-edited responsibilities tailored to requirements.
    This is where LLM metadata belongs.
    """

    responsibility_key: str
    requirement_key: str
    responsibility: str


class SimilarityMetricsRow(BaseStageRow):
    """
    Pairwise similarity/entailment between resume responsibilities and
    job requirements.

    Not an LLM artifact; track your metrics backends separately if needed.
    """

    # join keys
    responsibility_key: str
    requirement_key: str

    # denormalized text (handy for inspection/debug)
    responsibility: str
    requirement: str

    # editorial pass (ORIGINAL vs EDITED)
    version: Version = Field(default=Version.ORIGINAL)

    # editor provenance (ONLY populated when version=EDITED)
    resp_llm_provider: Optional[str] = None
    resp_model_id: Optional[str] = None

    # metrics backends (these identify the scoring engines)
    similarity_backend: Optional[str] = (
        None  # e.g. "sentence-transformers/all-mpnet-base-v2"
    )
    nli_backend: Optional[str] = None  # e.g. "microsoft/deberta-v3-large-mnli"

    # raw scores
    bert_score_precision: Optional[float] = None
    soft_similarity: Optional[float] = None
    word_movers_distance: Optional[float] = None
    deberta_entailment_score: Optional[float] = None
    roberta_entailment_score: Optional[float] = None

    # categorized
    bert_score_precision_cat: Optional[str] = None
    soft_similarity_cat: Optional[str] = None
    word_movers_distance_cat: Optional[str] = None
    deberta_entailment_score_cat: Optional[str] = None
    roberta_entailment_score_cat: Optional[str] = None

    # derived/scaled
    scaled_bert_score_precision: Optional[float] = None
    scaled_soft_similarity: Optional[float] = None
    scaled_word_movers_distance: Optional[float] = None
    scaled_deberta_entailment_score: Optional[float] = None
    scaled_roberta_entailment_score: Optional[float] = None
    composite_score: Optional[float] = None
    pca_score: Optional[float] = None


# ──────────────────────────────────────────────────────────────────────────────
# Explicit export list
# ──────────────────────────────────────────────────────────────────────────────

__all__ = [
    "PipelineState",
    "JobUrlsRow",
    "JobPostingsRow",
    "ExtractedRequirementsRow",
    "FlattenedRequirementsRow",
    "FlattenedResponsibilitiesRow",
    "PrunedResponsibilitiesRow",
    "EditedResponsibilitiesRow",
    "SimilarityMetricsRow",
    # bases/mixins (export if the registry/type checks need them)
    "AppBaseModel",
    "BaseStageRow",
    "TimestampedMixin",
    "LLMStampedMixin",
]
