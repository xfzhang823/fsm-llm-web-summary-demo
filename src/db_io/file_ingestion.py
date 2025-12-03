"""
db_io/file_ingestion.py

High-level ingestion entrypoints for loading JSON files into DuckDB
for the *web summary demo*.

Current scope:
- Ingest a URL seed JSON file into the `url` table.

Design:
- Validate + load JSON into a Pydantic model (UrlFile).
- Flatten the model into a DataFrame compatible with UrlRow/url table.
- Hand off to `insert_df_with_config`, which aligns schema + stamps timestamps.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Literal, Union, Optional
import pandas as pd

# Import from project
from fsm.pipeline_enums import TableName
from db_io.db_inserters import insert_df_with_config
from models.url_file_models import UrlFile  # your loader-side model
from models.pydantic_model_loaders_for_files import load_url_file_model
from config.project_config import URL_JSON_FILE

logger = logging.getLogger(__name__)

__all__ = [
    "ingest_single_file",
    "ingest_url_file",
]


# ---------------------------------------------------------------------------
# Core generic helper
# ---------------------------------------------------------------------------

ModelLoaderFn = Callable[[Path], Optional[UrlFile]]


def ingest_single_file(
    table_name: TableName,
    source_file: Union[Path, str],
    loader_fn: ModelLoaderFn,
    *,
    mode: Literal["append", "replace"] = "append",
) -> None:
    """
    Ingest one validated file into a DuckDB table.

    Steps:
      1. Validate the file path and load a Pydantic model via `loader_fn`.
      2. Flatten the model into a DataFrame for the given table.
      3. Insert the DataFrame into DuckDB via `insert_df_with_config`, which
         handles schema alignment, metadata stamping, and dedup logic.

    Args:
        table_name: Target DuckDB table (e.g., TableName.URL).
        source_file: Path to the input file to ingest.
        loader_fn: Function that loads and validates the file into a
            Pydantic model.
        mode: "append" (default) or "replace".
    """
    source_path = Path(source_file)

    logger.info("📥 Ingesting %s from %s", table_name.value, source_path)

    if not source_path.exists():
        logger.warning("⚠️ File not found: %s", source_path)
        return

    model = loader_fn(source_path)

    # For now this demo only supports URL ingestion via this path.
    if table_name is TableName.URL:
        df = _flatten_url_file(model)
    else:
        raise NotImplementedError(
            f"ingest_single_file does not yet support table {table_name}"
        )

    logger.info(
        "🔎 DataFrame for %s → %d rows, %d cols",
        table_name.value,
        df.shape[0],
        df.shape[1],
    )
    logger.debug("First few rows:\n%s", df.head(3).to_string(index=False))

    insert_df_with_config(df=df, table_name=table_name, mode=mode)
    logger.info("✅ Inserted rows into %s", table_name.value)


# ---------------------------------------------------------------------------
# URL-specific convenience
# ---------------------------------------------------------------------------


def ingest_url_file(
    file_path: Union[Path, str] = URL_JSON_FILE,
    *,
    mode: Literal["append", "replace"] = "append",
) -> None:
    """
    Ingest the URL seed JSON file into the `url` table.

    This is the high-level entrypoint used by the URL update pipeline.

    Args:
        file_path: Path to the URL JSON file (defaults to URL_JSON_FILE).
        mode: "append" (default) or "replace".
    """
    ingest_single_file(
        table_name=TableName.URL,
        source_file=file_path,
        loader_fn=load_url_file_model,
        mode=mode,
    )


# ---------------------------------------------------------------------------
# Flattening helpers (model → DataFrame)
# ---------------------------------------------------------------------------


def _flatten_url_file(model: UrlFile) -> pd.DataFrame:
    """
    Flatten a UrlFile model into a DataFrame compatible with the `url` table.

    Expected UrlFile shape (example):
        class UrlFile(BaseModel):
            urls: list[UrlEntry]

    This flattener:
      - converts each UrlEntry into a row
      - joins tags list into a comma-separated string (or leaves None)
    """
    if not isinstance(model, UrlFile):
        # If you ever extend this to accept other models, adjust here.
        raise TypeError(f"_flatten_url_file expected UrlFile, got {type(model)}")

    rows = []
    for entry in model.urls:
        tags_str = None
        if entry.tags:
            tags_str = ",".join(t.strip() for t in entry.tags if t and t.strip())

        rows.append(
            {
                "url": str(entry.url),
                "source": entry.source,
                "tags": tags_str,
            }
        )

    return pd.DataFrame(rows)
