"""
main_fsm_driven_duckdb.py

High-level orchestrator for the DuckDB-backed FSM pipelines
for the *web summary demo*.

Pipeline overview
-----------------
Stages (FSM rail):
    URL → WEB_PAGE → WEB_SUMMARY

This module coordinates the full sequence:

    0) Ensure all DuckDB tables exist (schema bootstrap).
    1) Ingest URLs from JSON into the `url` table.
    2) Sync URLs into `pipeline_control` at stage=URL.
    3) Run Stage A (URL → WEB_PAGE):
         - scrape webpages via Playwright
         - store cleaned text into `web_page`.
    4) Run Stage B (WEB_PAGE → WEB_SUMMARY):
         - load `web_page.clean_text`
         - call LLM to summarize
         - store summary into `web_summary`.

Each stage pipeline is FSM- and lease-aware, using `pipeline_control`
to determine its worklist. This orchestrator just runs them in order.

Usage:
    python main_fsm_driven_duckdb.py
"""

from __future__ import annotations

import asyncio
import logging
import matplotlib

from db_io.create_db_tables import create_all_db_tables
import config.logging_config  # ensure this defines init_logging()

from pipelines.update_urls_pipeline_fsm import run_update_urls_pipeline_fsm
from pipelines.urls_pipeline_fsm import run_urls_pipeline_fsm
from pipelines.sync_url_table_to_pc_pipeline_fsm import (
    run_sync_url_table_to_pc_pipeline_fsm,
)
from pipelines.web_scrape_pipeline_async_fsm import run_web_scrape_pipeline_async_fsm
from pipelines.web_summarize_pipeline_async_fsm import (
    run_web_summary_pipeline_async_fsm,
)

logger = logging.getLogger(__name__)

# Avoid interactive backends in CLI / server environments
matplotlib.use("Agg")


async def run_all_fsm(*, append_only_urls: bool = True) -> None:
    """
    Run the full FSM-driven DuckDB pipeline in sequence.

    Steps
    -----
    0. Ensure DB schema exists (idempotent).
    1. Ingest URL JSON → `url` table.
    2. Sync `url` into `pipeline_control` at stage=URL.
    3. Stage A: URL → WEB_PAGE
        - scrape webpages
        - persist cleaned text into `web_page`.
    4. Stage B: WEB_PAGE → WEB_SUMMARY
        - load cleaned text
        - call LLM
        - persist summaries into `web_summary`.

    Notes
    -----
    - All heavy lifting is done inside the per-stage pipelines.
    - This function only orders their execution and ensures the
      control-plane is initialized first.
    - Stage A and Stage B are async and process many URLs concurrently.
    """
    # 0) Ensure schema
    create_all_db_tables()

    # 1) Ingest URLs from JSON into `url`
    run_update_urls_pipeline_fsm(mode="append" if append_only_urls else "replace")

    # 2) Seed pipeline_control from `url` at stage=URL
    run_sync_url_table_to_pc_pipeline_fsm()

    # 3) Move stage from url -> webpage
    run_urls_pipeline_fsm()

    # 4) Stage A: scrape webpages → `web_page`
    await run_web_scrape_pipeline_async_fsm(
        max_concurrent_tasks=3,
        retry_errors=False,
    )

    # 4) Stage B: summarize webpages → `web_summary`
    await run_web_summary_pipeline_async_fsm(
        max_concurrent_tasks=3,
        retry_errors=True,
    )


if __name__ == "__main__":
    # Optional: initialize logging once at startup if your config module exposes it
    try:
        config.logging_config.init_logging()  # type: ignore[attr-defined]
    except Exception:
        # Fallback: basicConfig if init_logging is not available
        logging.basicConfig(level=logging.INFO)

    asyncio.run(run_all_fsm())
