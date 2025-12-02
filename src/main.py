"""
main_fsm_driven_duckdb.py

High-level orchestrator for the DuckDB-backed FSM pipelines.

This module coordinates the entire pipeline execution sequence:
1. Ensure all DuckDB tables exist (schema bootstrap).
2. Pre-flight updates:
   - Ingest `job_urls` from JSON and sync into `pipeline_control`.
   - Optionally refresh `flattened_responsibilities` if the resume has
   been updated.
3. Run stage pipelines in order (async for concurrent URL-level tasks).
4. Optionally run reporting/analytics at the end (e.g., crosstab export).

The design separates:
- Control-plane sync (`state_orchestration_pipeline`)
- Stage-specific pipelines (job_postings, requirements extraction/flattening,
  resume editing, similarity metrics, etc.)
- Reporting/export pipelines

Each pipeline is FSM-aware and uses the control table to determine its worklist.
This orchestrator simply drives them in a logical sequence.

Usage:
    python main_fsm_driven_duckdb.py
"""

# Dependencies
from __future__ import annotations

# from standard/third-party
import asyncio
import logging
import matplotlib

# User defined
from db_io.create_db_tables import create_all_db_tables
import config.logging_config


# Pipelines
from pipelines.update_urls_pipeline_fsm import (
    run_update_urls_pipeline_fsm,
)
from pipelines.urls_pipeline_fsm import run_urls_pipeline_fsm
from pipelines.web_scrape_pipeline_async_fsm import run_web_scrape_pipeline_async_fsm
from pipelines.web_summarize_pipeline_async_fsm import (
    run_web_summary_pipeline_async_fsm,
)


logger = logging.getLogger(__name__)

matplotlib.use("Agg")  # Prevent interactive mode


async def run_all_fsm(*, append_only_urls: bool = True) -> None:
    """
    Run the full FSM-driven DuckDB pipeline in sequence.

    Steps
    -----
    0. Ensure DB schema exists (idempotent).
    1. Seed or refresh `pipeline_control` to align with job_urls/job_postings/etc.
       (initial run uses full=True to bootstrap).
    2. Execute the main pipelines in order:
       - Preprocessing
        (job_postings → extracted_requirements & flattened_requirements)
       - Metrics/evaluation stages
       - Resume editing stage
    3. Refresh `pipeline_control` with integrity checks enabled.
    4. Run optional reporting/export pipelines (e.g.,
        responsibilities–requirements crosstab).

    Notes
    -----
    - All heavy lifting is done inside the per-stage pipelines.
    - This function only orders their execution and ensures control-plane sync
      before and after.
    - Async/await is used so that stage runners can process many URLs concurrently.
    """
    # Ensure schema
    create_all_db_tables()

    # --- PRE-FLIGHT: update job_urls, then seed control-plane from it ---
    run_update_urls_pipeline_fsm(mode="append" if append_only_urls else "replace")

    # Push JOB_URL stage to webscraping stage
    run_urls_pipeline_fsm()

    # Webscrape webpages
    await run_web_scrape_pipeline_async_fsm(max_concurrent_tasks=3, retry_errors=True)

    # Summarize webpages on SIM METRICS - EVAL
    await run_web_summary_pipeline_async_fsm(max_concurrent_tasks=3, retry_errors=True)


if __name__ == "__main__":
    asyncio.run(run_all_fsm())
