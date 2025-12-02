"""
db_io/pipeline_enums.py

Core Enum types used across the *web summary demo* pipeline.

Centralizes canonical value definitions for:
- Pipeline stages (FSM state tracking)
- Pipeline status (job progression)
- LLM providers (used during summarization)
- Content versioning (kept for compatibility; may be unused in this demo)
- DuckDB table names (typed reference to all tables)

These enums are reused across:
- DuckDB schema generation
- Pydantic model validation
- FSM control logic
- Filtering, logging, and pipeline introspection
"""

from enum import Enum
from config.project_config import OPENAI, ANTHROPIC


# ──────────────────────────────────────────────────────────────────────────────
# LLM Provider
# ──────────────────────────────────────────────────────────────────────────────


class LLMProvider(str, Enum):
    """Name of the LLM provider used during this pipeline pass."""

    OPENAI = OPENAI
    ANTHROPIC = ANTHROPIC
    MISTRAL = "mistral"
    LLAMA = "llama"

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline stages (FSM)
# ──────────────────────────────────────────────────────────────────────────────


class PipelineStage(str, Enum):
    """
    Defines all pipeline stages in the web summary FSM.

    Conventions
    -----------
    - Enum member names:  UPPER_SNAKE_CASE
    - Enum values (stored in DB):  UPPER_SNAKE_CASE (e.g., "URL")

    Stages
    ------
    URL
        Seed URL registry stage.
    WEB_PAGE
        HTTP fetch + parse + clean stage.
    WEB_SUMMARY
        LLM summarization stage.
    """

    URL = "URL"
    WEB_PAGE = "WEB_PAGE"
    WEB_SUMMARY = "WEB_SUMMARY"

    @classmethod
    def list(cls) -> list["PipelineStage"]:
        """Return all defined stages as a list."""
        return list(cls)


# ──────────────────────────────────────────────────────────────────────────────
# Machine status (per-stage lifecycle)
# ──────────────────────────────────────────────────────────────────────────────


class PipelineStatus(str, Enum):
    """
    Machine lifecycle per stage.
    Tracks progress within a stage.

    Distinct from `PipelineTaskState`, which is human-facing
    (READY / PAUSED / SKIP / HOLD).
    """

    NEW = "NEW"  # Not yet started
    IN_PROGRESS = "IN_PROGRESS"  # Work in progress for this stage
    COMPLETED = "COMPLETED"  # This stage completed successfully
    ERROR = "ERROR"  # Current stage failed
    SKIPPED = "SKIPPED"  # Explicitly skipped (filtered out / not applicable)


# ──────────────────────────────────────────────────────────────────────────────
# Human gate (task availability)
# ──────────────────────────────────────────────────────────────────────────────


class PipelineTaskState(str, Enum):
    """
    Human gate for pipeline tasks.
    Controls *availability* of rows to the automated pipeline.

    Distinct from `PipelineStatus`, which represents machine lifecycle.

    States:
        READY  - eligible for machine processing
        PAUSED - temporarily held by human (do not process)
        SKIP   - permanently skip this record
        HOLD   - optional: used for pending manual review (intermediate)
    """

    READY = "READY"
    PAUSED = "PAUSED"
    SKIP = "SKIP"
    HOLD = "HOLD"


# ──────────────────────────────────────────────────────────────────────────────
# DuckDB table names (demo schema)
# ──────────────────────────────────────────────────────────────────────────────


class TableName(str, Enum):
    """
    Enum representing all DuckDB table names used in the web summary demo.

    Members correspond to the 4 core tables:

    - URL
    - PIPELINE_CONTROL
    - WEB_PAGE
    - WEB_SUMMARY
    """

    PIPELINE_CONTROL = "pipeline_control"
    """Controls pipeline progression with the FSM (one row per url+iteration)."""

    URL = "url"
    """Canonical registry of URLs to process."""

    WEB_PAGE = "web_page"
    """Raw + cleaned web page content and fetch metadata."""

    WEB_SUMMARY = "web_summary"
    """LLM-generated summaries and associated token/LLM metadata."""

    @classmethod
    def list(cls) -> list[str]:
        """Returns a list of all table names as strings."""
        return [t.value for t in cls]

    @classmethod
    def from_value(cls, value: str) -> "TableName":
        """
        Converts a string to the corresponding TableName enum member.

        Args:
            value: A valid table name string.

        Returns:
            TableName: The corresponding enum member.
        """
        return cls(value)

    def __str__(self) -> str:
        """Return the string value of the enum member."""
        return self.value


# ──────────────────────────────────────────────────────────────────────────────
# Content Version (kept for compatibility, may be unused in demo)
# ──────────────────────────────────────────────────────────────────────────────


class Version(str, Enum):
    """Content version (original, LLM-edited, human-finalized)."""

    ORIGINAL = "original"
    EDITED = "edited"
    FINAL = "final"

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]
