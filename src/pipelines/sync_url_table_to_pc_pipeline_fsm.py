"""
pipelines/state_orchestration_pipeline.py

Lightweight control-plane sync for the web summary demo.

Responsibility:
    - Ensure `pipeline_control` has one row per URL at stage=URL.
    - Does NOT run scraping or LLM work.
"""

from __future__ import annotations

import logging
from typing import Literal

import pandas as pd

from db_io.get_db_connection import get_db_connection
from db_io.db_inserters import insert_df_with_config
from fsm.pipeline_enums import (
    TableName,
    PipelineStage,
    PipelineStatus,
    PipelineTaskState,
)
from models.db_table_models import PipelineState

logger = logging.getLogger(__name__)


def sync_url_table_to_pc_pipeline_fsm(
    *,
    iteration: int = 0,
    mode: Literal["append", "replace"] = "append",
) -> None:
    """
    Sync the `url` table into `pipeline_control`.

    - Reads distinct URLs from `url`.
    - Ensures one PipelineState row per (url, iteration) at stage=URL.
    - Does *not* touch files; operates entirely in DuckDB.
    """
    con = get_db_connection()
    try:
        df_urls = con.execute("SELECT DISTINCT url FROM url").df()
    finally:
        con.close()

    if df_urls.empty:
        logger.info("📭 No URLs found in `url` table to sync into pipeline_control.")
        return

    con = get_db_connection()
    try:
        df_existing = con.execute(
            "SELECT url, iteration FROM pipeline_control WHERE iteration = ?",
            [iteration],
        ).df()
    finally:
        con.close()

    existing_keys = (
        set(zip(df_existing["url"], df_existing["iteration"]))
        if not df_existing.empty
        else set()
    )

    rows: list[dict] = []

    for url in df_urls["url"].tolist():
        key = (url, iteration)
        if mode == "append" and key in existing_keys:
            continue

        state = PipelineState(
            url=url,
            iteration=iteration,
            stage=PipelineStage.URL,
            status=PipelineStatus.NEW,
            task_state=PipelineTaskState.READY,
        )
        rows.append(state.model_dump(exclude_none=False))

    if not rows:
        logger.info("📭 No new pipeline_control rows to insert.")
        return

    df_pc = pd.DataFrame(rows)

    if mode == "replace":
        con = get_db_connection()
        try:
            con.execute(
                "DELETE FROM pipeline_control WHERE iteration = ?",
                [iteration],
            )
        finally:
            con.close()

    insert_df_with_config(df=df_pc, table_name=TableName.PIPELINE_CONTROL)
    logger.info(
        "✅ Synced %d URL(s) into pipeline_control (mode=%s, iteration=%s)",
        len(rows),
        mode,
        iteration,
    )


def run_sync_url_table_to_pc_pipeline_fsm(
    *, iteration: int = 0, mode: Literal["append", "replace"] = "append"
) -> None:
    """
    FSM-friendly wrapper to sync URLs into pipeline_control.

    Called from main.py as part of the control-plane setup.
    """
    sync_url_table_to_pc_pipeline_fsm(iteration=iteration, mode=mode)
