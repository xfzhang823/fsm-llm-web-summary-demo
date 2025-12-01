"""
db_io/pipeline_enums.py

This module defines all core Enum types used across the resume-job
alignment pipeline.

It centralizes canonical value definitions for:
- Pipeline stages (FSM state tracking)
- Pipeline status (job progression)
- LLM providers (used during editing/evaluation)
- Versioning (original vs. edited content)
- DuckDB table names (typed reference to all tables)

These enums serve as a single source of truth and are reused across:
- DuckDB schema generation
- Pydantic model validation
- FSM control logic
- Filtering, logging, and pipeline introspection
- Prompt generation and result parsing

Included Components:
- ✅ `PipelineStage`: Defines linear FSM stages
(e.g., job_urls → final_responsibilities)
- ✅ `PipelineStatus`: High-level job status in the control table
(new, in_progress, etc.)
- ✅ `LLMProvider`: Supported LLM provider values
(e.g., openai, anthropic)
- ✅ `Version`: Content lifecycle versions
(original, edited, final)
- ✅ `TableName`: Enum of all DuckDB tables used throughout the pipeline
"""

from enum import Enum
from job_bot.config.project_config import OPENAI, ANTHROPIC


# * ✅ Metadata ENUM Classes
class LLMProvider(str, Enum):
    """Name of the LLM provider used during this pipeline pass."""

    OPENAI = OPENAI
    ANTHROPIC = ANTHROPIC
    MISTRAL = "mistral"
    LLAMA = "llama"

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class PipelineStage(str, Enum):
    """
    Defines all pipeline stages in the FSM, using explicit table names as stages.

    Conventions
    -----------
    - Enum member names:  UPPER_SNAKE_CASE
    - Enum values (stored in DB):  UPPER_SNAKE_CASE (e.g., "JOB_URLS")
      This ensures consistency with DuckDB and PipelineState model coercion.

    Example
    -------
        df["stage"] = PipelineStage.JOB_URLS.value
        assert stage in PipelineStage.list()
    """

    # ✅ Preprocessing Stages
    JOB_URLS = "JOB_URLS"
    JOB_POSTINGS = "JOB_POSTINGS"
    # EXTRACTED_REQUIREMENTS = "EXTRACTED_REQUIREMENTS"  # commented out intentionally

    # ✅ Flattened Requirements
    FLATTENED_REQUIREMENTS = "FLATTENED_REQUIREMENTS"
    FLATTENED_RESPONSIBILITIES = "FLATTENED_RESPONSIBILITIES"

    # ✅ Evaluation
    SIM_METRICS_EVAL = "SIMILARITY_METRICS_EVAL"  # original responsibilities vs reqs

    # ✅ Editing (LLM)
    EDITED_RESPONSIBILITIES = "EDITED_RESPONSIBILITIES"

    # Optional pruning stage (not used currently)
    # PRUNED_RESPONSIBILITIES = "PRUNED_RESPONSIBILITIES"

    # ✅ Revaluation
    SIM_METRICS_REVAL = "SIMILARITY_METRICS_REVAL"  # edited responsibilities vs reqs

    # ✅ Human Review (optional)
    ALIGNMENT_REVIEW = "ALIGNMENT_REVIEW"  # cross-tab visualization + feedback

    # ✅ Export / Finalization
    FINAL_RESPONSIBILITIES = "FINAL_RESPONSIBILITIES"  # manually trimmed/pruned output

    @classmethod
    def list(cls) -> list["PipelineStage"]:
        """Return all defined stages as a list."""
        return list(cls)


class PipelineStatus(str, Enum):
    """
    Machine lifecycle per stage.
    Tracks progress within a stage.

    - Represents the automated system's internal notion of
      progress or completion.
    - Distinct from `PipelineTaskState`, which is human-facing
      (READY / PAUSED / SKIP / HOLD).

    ! Pipeline Status is different from status in JobPostings and ExtractedRequirements tables,
    ! which refers LLM API call status
    """

    NEW = "NEW"  # Not yet started
    IN_PROGRESS = (
        "IN_PROGRESS"  # This stage completed successfully, but the pipeline continues
    )
    COMPLETED = "COMPLETED"  # Final stage completed successfully (end of pipeline)
    ERROR = "ERROR"  # Current stage failed
    SKIPPED = "SKIPPED"  # Explicitly skipped (optional path or filtered out)


class PipelineTaskState(str, Enum):
    """
    Human gate for pipeline tasks.
    Controls *availability* of rows to the automated pipeline.

    Distinct from `PipelineStatus`, which represents machine lifecycle.
    - `PipelineStatus` tracks work progress *within* a stage
      (e.g., NEW → IN_PROGRESS → DONE/ERROR).
    - `PipelineTaskState` tracks the *entire pipeline iteration*
      from start to finish.

    States:
        READY = "READY"  # ✅ eligible for machine processing
        PAUSED = "PAUSED"  # ⏸️ temporarily held by human (do not process)
        SKIP = "SKIP"  # 🚫 permanently skip this record
        HOLD = "HOLD"  # 🕓 optional: used for pending manual review (intermediate)

    Note: Use upper case per convention.
    """

    READY = "READY"
    PAUSED = "PAUSED"
    SKIP = "SKIP"
    HOLD = "HOLD"


class TableName(str, Enum):
    """
    Enum representing all DuckDB table names used in the resume-job alignment pipeline.

    Each member corresponds to a specific stage or artifact in the data pipeline,
    and is used to enforce consistent naming across ingestion, transformation, and storage.

    >>> Example Usages:

        for table_name in TableName:
        print(f"Processing table: {table_name.value}")
        # hypothetical read
        df = read_table(table_name.value)
        print(df.shape)

        print("Available DuckDB tables:")
        for table in TableName.list():
            print(f" - {table}")

    """

    PIPELINE_CONTROL = "pipeline_control"
    """Controls pipeline progression with state machine."""

    JOB_URLS = "job_urls"
    """Registry of job posting URLs and associated metadata (company, title)."""

    JOB_POSTINGS = "job_postings"
    """Scraped job descriptions including job title, location, and content."""

    EXTRACTED_REQUIREMENTS = "extracted_requirements"
    """Structured LLM-extracted requirements categorized into tiers (e.g., pie_in_the_sky)."""

    FLATTENED_REQUIREMENTS = "flattened_requirements"
    """Flattened job requirements in key-value format, used for alignment."""

    FLATTENED_RESPONSIBILITIES = "flattened_responsibilities"
    """Original resume responsibilities, flattened for semantic evaluation."""

    PRUNED_RESPONSIBILITIES = "pruned_responsibilities"
    """Responsibilities trimmed or filtered by heuristics or LLMs."""

    EDITED_RESPONSIBILITIES = "edited_responsibilities"
    """LLM-optimized responsibilities aligned with job requirements."""

    SIMILARITY_METRICS = "similarity_metrics"
    """Similarity scores and entailment metrics between responsibilities and requirements."""

    ALIGNMENT_CROSSTAB = "alignment_crosstab"
    """Cross-tabulated similarity and entailment metrics mapping requirements \
to responsibilities."""

    @classmethod
    def list(cls) -> list[str]:
        """
        Returns a list of all table names as strings.
        Useful for validation, logging, and iteration.
        """
        return [t.value for t in cls]

    @classmethod
    def from_value(cls, value: str) -> "TableName":
        """
        Converts a string to the corresponding TableName enum member.

        Args:
            value (str): A valid table name string.

        Returns:
            TableName: The corresponding enum member.
        """
        return cls(value)

    def __str__(self) -> str:
        """
        Returns the string value of the enum member.
        Enables clean usage in f-strings, logs, and file naming.
        """
        return self.value


class Version(str, Enum):
    """Content version (original, LLM-edited, human-finalized)."""

    ORIGINAL = "original"
    EDITED = "edited"
    FINAL = "final"

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]
