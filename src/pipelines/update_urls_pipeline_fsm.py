"""pipelines_with_fsm/update_urls_pipeline_fsm.py"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

# From project
from db_io.create_db_tables import create_all_db_tables
from db_io.file_ingestion import ingest_single_file
from fsm.pipeline_enums import TableName
from models.pydantic_model_loaders_for_files import (
    load_url_file_model,
)
from config.project_config import URL_JSON_FILE

logger = logging.getLogger(__name__)


def run_update_urls_pipeline_fsm(
    file_path: Path | str = URL_JSON_FILE,
    mode: Literal["append", "replace"] = "append",
) -> None:
    """
    Ingest the URL seed JSON file into DuckDB (`url` table).

    - Default `mode="append"` inserts only new rows (dedup aware).
    - Use `mode="replace"` to refresh matching rows based on the table’s PKs.

    Args:
        file_path: Path to the URL JSON file (default: URL_JSON_FILE).
        mode: "append" | "replace" (default: "append").

    Returns:
        None
    """
    logger.info("✅ Running ingest job urls pipeline...")
    logger.info("🏗️ Ensuring DuckDB schema exists …")
    create_all_db_tables()

    file_path = Path(file_path)
    logger.info(f"📥 Ingesting job URLs from: {file_path} (mode={mode})")

    # job_urls is a seed table → no iteration/version/provider/model_id needed
    ingest_single_file(
        TableName.URL,
        file_path,
        load_url_file_model,
        mode=mode,
    )

    logger.info("✅ urls ingestion complete.")
