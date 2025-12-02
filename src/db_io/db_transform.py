"""
transform.py

This module contains tools such as add_metadata, helping transformating pyd models
to df, and vice versa.

This utility is central to ensuring schema-aligned ingestion and downstream
consistency across the DuckDB database.
"""

from pathlib import Path
import logging
import pandas as pd

# User defined
from db_io.db_schema_registry import DUCKDB_SCHEMA_REGISTRY
from fsm.pipeline_enums import (
    TableName,
    LLMProvider,
    Version,
)

logger = logging.getLogger(__name__)


# Alternative and more flexible function to add metadata
def add_metadata(
    df: pd.DataFrame,
    table: TableName,
    *,
    source_file: Path | str | None = None,
    iteration: int | None = None,
    version: Version | str | None = None,
    llm_provider: LLMProvider | str | None = None,
    model_id: str | None = None,
) -> pd.DataFrame:
    """
    Add table-aware metadata columns to a DataFrame without overwriting
    existing values.

    Purpose
    -------
    Each DuckDB table defines a small set of metadata fields in
    `DUCKDB_SCHEMA_REGISTRY[table].metadata_fields`. This function ensures
    those fields are added consistently during ingestion while avoiding
    accidental over-stamping.

    What this function does
    -----------------------
    • Adds `source_file` if the table owns that field.
    • Adds `iteration`, defaulting to 0 if the table requires it.
    • Adds `version`, `llm_provider`, and `model_id` only if:
        – the caller provides them, and
        – the table defines those fields.
    • Never overwrites columns already present in the DataFrame.

    What this function does NOT do
    ------------------------------
    • Does NOT set `stage`, `created_at`, or `updated_at`
      (these belong to FSM or database defaults).

    When to call
    ------------
    Call this immediately after flattening a validated model and just before
    inserting the DataFrame into DuckDB.
    """

    schema = DUCKDB_SCHEMA_REGISTRY[table]
    fields = set(schema.metadata_fields)

    def set_if_needed(col: str, value):
        if col in fields and col not in df.columns:
            df[col] = value

    # Basic stamps
    if source_file is not None:
        set_if_needed("source_file", str(source_file))

    if "iteration" in fields and "iteration" not in df.columns:
        set_if_needed("iteration", 0 if iteration is None else iteration)

    # Optional stamps
    if version is not None:
        set_if_needed("version", getattr(version, "value", version))

    if llm_provider is not None:
        set_if_needed("llm_provider", getattr(llm_provider, "value", llm_provider))

    if model_id is not None:
        set_if_needed("model_id", model_id)

    return df
