"""
pipelines/web_scrape_pipeline_async.py

Stage A: URL → WEB_PAGE
=======================

Asynchronous FSM-aware pipeline responsible ONLY for:
- Fetching webpage content (via Playwright, through webpage_reader_async)
- Cleaning text (inside webpage_reader_async)
- Inserting into DuckDB (web_page table)
- Updating pipeline_control with COMPLETED or ERROR
- Advancing FSM stage from URL → WEB_PAGE

NO LLMs. NO parsing. NO summarization.

This pipeline mirrors the lease + human-gate + FSM behavior of the job_bot
project but is simplified for the web summary demo.

---------------------------------------------------------------------------
📦 High-Level Flow
---------------------------------------------------------------------------

pipeline_control (stage=URL, status=NEW/ERROR/IN_PROGRESS, task_state=READY)
    |
    v
get_claimable_worklist(stage=URL, status in {...})
    |
    v
try_claim_one(url, iteration, worker_id, lease_minutes)
    |
    v
fetch_and_persist_webpage_async()
    - read_webpages_async([url])
    - build WebPageRow(url, iteration, clean_text, html=None, ...)
    - insert_df_with_config(TableName.WEB_PAGE)
    |
    v
release_one(final_status=COMPLETED or ERROR)
    |
    v
fsm.step()   # URL → WEB_PAGE

---------------------------------------------------------------------------
Design notes
---------------------------------------------------------------------------

- Human gate:
    * `task_state` is enforced inside `get_claimable_worklist`
      (only READY rows are returned).

- Machine lease:
    * `try_claim_one` sets `is_claimed`, `worker_id`, and `lease_until`.
    * `release_one` clears the lease and sets final status.

- Retry behavior:
    * By default we only process `NEW` rows.
    * If `retry_errors=True`, we also include `ERROR` and `IN_PROGRESS`.
      `IN_PROGRESS` rows are only claimable again if their lease has expired.

- FSM:
    * A defensive stage check ensures we only process rows at stage=URL.
    * On successful fetch + persist, we call `fsm.step()` to move to WEB_PAGE.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Tuple, Optional

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Project imports
# ─────────────────────────────────────────────────────────────────────────────

from db_io.db_inserters import insert_df_with_config
from db_io.db_utils import (
    get_claimable_worklist,
    try_claim_one,
    release_one,
    generate_worker_id,
)
from fsm.pipeline_enums import PipelineStage, PipelineStatus, TableName
from fsm.pipeline_fsm_manager import PipelineFSMManager

from models.db_table_models import WebPageRow  # clean_text, html, fetch_error
from pipelines.webpage_reader_async import read_webpages_async  # Playwright fetcher

logger = logging.getLogger(__name__)


# ============================================================================
# 1. Low-level worker: fetch webpage + persist into DuckDB
# ============================================================================


async def fetch_and_persist_webpage_async(
    url: str,
    iteration: int,
    semaphore: asyncio.Semaphore,
) -> bool:
    """
    Fetch webpage content via Playwright and persist a `WebPageRow` into DuckDB.

    This function is **purely data-plane**:
        - It does not mutate pipeline_control.
        - It does not interact with the FSM directly.
        - It is safe to call multiple times (idempotence depends on
          insert_df_with_config semantics).

    Args:
        url:
            The URL to fetch.
        iteration:
            The iteration index for this URL (part of the (url, iteration) PK).
        semaphore:
            Shared asyncio.Semaphore limiting concurrent Playwright fetches.

    Returns:
        bool:
            True  → successfully fetched and inserted into `web_page`.
            False → fetch error or DB insertion error.

    Behavior:
        - On fetch failure:
            * Writes a WebPageRow with clean_text=None, fetch_error="FETCH_FAILED".
        - On success:
            * Writes a WebPageRow with clean_text set to the cleaned body text.
            * `status_code` and `html` are left None for now (Stage A only
               stores cleaned text).
    """
    logger.info("🌐 Fetching webpage: %s [iter=%s]", url, iteration)

    # Limit concurrent Playwright calls using the shared semaphore
    async with semaphore:
        try:
            documents, failed_urls = await read_webpages_async([url])
        except Exception:
            logger.exception("❌ Exception reading webpage for %s", url)
            return False

    # ----------------------------------------------------------------------
    # Failure case: URL failed or missing in result
    # ----------------------------------------------------------------------
    if url in failed_urls or url not in documents:
        logger.warning("🚫 Failed to fetch %s", url)

        row = WebPageRow(
            url=url,
            iteration=iteration,
            status_code=None,
            html=None,
            clean_text=None,
            fetch_error="FETCH_FAILED",
            fetched_at=datetime.now(timezone.utc),
        )

        df = pd.DataFrame([row.model_dump()])
        try:
            await asyncio.to_thread(insert_df_with_config, df, TableName.WEB_PAGE)
        except Exception:
            logger.exception("💾 Failed to persist fetch-error row for %s", url)

        return False

    # ----------------------------------------------------------------------
    # Success case
    # ----------------------------------------------------------------------
    clean_text = documents[url]

    row = WebPageRow(
        url=url,
        iteration=iteration,
        status_code=None,  # Playwright text-only path doesn't surface HTTP code
        html=None,  # Stage A does not store raw HTML
        clean_text=clean_text,
        fetch_error=None,
        fetched_at=datetime.now(timezone.utc),
    )

    df = pd.DataFrame([row.model_dump()])

    try:
        await asyncio.to_thread(insert_df_with_config, df, TableName.WEB_PAGE)
        logger.info("✅ Stored webpage content for %s [iter=%s]", url, iteration)
        return True
    except Exception:
        logger.exception("💾 Failed persisting WebPageRow for %s", url)
        return False


# ============================================================================
# 2. Batch processor (lease-aware)
# ============================================================================


async def process_web_scrape_batch_async_fsm(
    items: List[Tuple[str, int]],
    *,
    worker_id: str,
    lease_minutes: int = 10,
    no_of_concurrent_workers: int = 5,
) -> List[asyncio.Task]:
    """
    Prepare asyncio tasks to process a batch of (url, iteration) rows
    using the lease + FSM model.

    For each (url, iteration) pair:
        1. `try_claim_one()`:
           - Acquires a machine lease, setting worker_id and lease_until.
        2. Stage gate:
           - Ensures the FSM is currently at stage=web_page for this URL.
           - If not, immediately releases the lease with ERROR and skips.
        3. `fetch_and_persist_webpage_async()`:
           - Fetches cleaned text and inserts into `web_page`.
        4. `release_one()`:
           - Releases the lease with COMPLETED or ERROR.
        5. `fsm.step()`:
           - On success, advances the FSM: URL → WEB_PAGE.

    Args:
        items:
            List of (url, iteration) pairs considered claimable upstream.
        worker_id:
            Stable identifier for this runner (e.g. "web_scrape_20250101_abc123").
        lease_minutes:
            Duration of the machine lease for each claimed row.
        no_of_concurrent_workers:
            Max number of concurrent Playwright/DB operations
            (controls the semaphore).

    Returns:
        List[asyncio.Task]:
            One task per (url, iteration). The caller should await them
            (e.g. via `asyncio.gather(*tasks)`).
    """
    semaphore = asyncio.Semaphore(no_of_concurrent_workers)

    async def run_one(url: str, iteration: int):
        # ---------------------------------------------------------
        # 1) Try to claim row (machine lease)
        # ---------------------------------------------------------
        claimed = await asyncio.to_thread(
            try_claim_one,
            url=url,
            iteration=iteration,
            worker_id=worker_id,
            lease_minutes=lease_minutes,
        )
        if not claimed:
            logger.info("🔒 Could not claim %s [%s]", url, iteration)
            return

        released = False
        fsm_manager = PipelineFSMManager()
        fsm = fsm_manager.get_fsm(url)

        # ---------------------------------------------------------
        # 2) Stage gate check (defensive).
        #    If the row has a different stage than URL, do not process it.
        #    Always release the lease so it doesn't remain stuck.
        # ---------------------------------------------------------
        if fsm.state != PipelineStage.WEB_PAGE.value:
            logger.info(
                "⏩ Wrong stage=%s for %s — should be URL; skipping",
                fsm.state,
                url,
            )

            await asyncio.to_thread(
                release_one,
                url=url,
                iteration=iteration,
                worker_id=worker_id,
                final_status=PipelineStatus.ERROR,  # or SKIPPED, depending on policy
            )
            released = True
            return

        # ---------------------------------------------------------
        # 3) Do the work: fetch + persist
        # ---------------------------------------------------------
        try:
            ok = await fetch_and_persist_webpage_async(url, iteration, semaphore)

            final_status = PipelineStatus.COMPLETED if ok else PipelineStatus.ERROR

            await asyncio.to_thread(
                release_one,
                url=url,
                iteration=iteration,
                worker_id=worker_id,
                final_status=final_status,
            )
            released = True

            if ok:
                # Advance FSM: URL → WEB_PAGE
                try:
                    fsm.mark_status(
                        status=final_status
                    )  #! need to update status in PipelineFSM class
                    fsm.step()
                except Exception as e:
                    logger.exception(
                        "⚠️ FSM step() failed for %s [%s]: %s",
                        url,
                        iteration,
                        e,
                    )

        except Exception:
            logger.exception("💥 Unhandled exception for %s [%s]", url, iteration)
            try:
                await asyncio.to_thread(
                    release_one,
                    url=url,
                    iteration=iteration,
                    worker_id=worker_id,
                    final_status=PipelineStatus.ERROR,
                )
                released = True
            except Exception:
                logger.exception(
                    "Failed releasing lease after crash for %s [%s]",
                    url,
                    iteration,
                )

        finally:
            # Extremely defensive: ensure we never leave a lease dangling
            if not released:
                try:
                    await asyncio.to_thread(
                        release_one,
                        url=url,
                        iteration=iteration,
                        worker_id=worker_id,
                        final_status=PipelineStatus.ERROR,
                    )
                except Exception:
                    logger.exception("Final cleanup failed for %s [%s]", url, iteration)

    return [asyncio.create_task(run_one(u, it)) for (u, it) in items]


# ============================================================================
# 3. Stage A orchestrator
# ============================================================================


async def run_web_scrape_pipeline_async_fsm(
    *,
    max_concurrent_tasks: int = 5,
    filter_keys: Optional[list[str]] = None,
    retry_errors: bool = False,
    lease_minutes: int = 10,
) -> None:
    """
    Top-level async runner for Stage A (URL → WEB_PAGE).

    This function:
        - Selects claimable rows from `pipeline_control`.
        - Applies an optional URL filter.
        - Generates a worker_id for this run.
        - Dispatches per-URL tasks via `process_web_scrape_batch_async_fsm`.
        - Waits for all tasks to complete.

    Selection criteria (delegated to `get_claimable_worklist`):
        - `stage = PipelineStage.URL`
        - `task_state = READY` (human gate open)
        - Lease not active (unclaimed or lease_until < now)
        - `status` in:
            * If retry_errors=False:
                - { NEW }
            * If retry_errors=True:
                - { NEW, ERROR, IN_PROGRESS }
              IN_PROGRESS rows are only returned if their lease has expired.

    Args:
        max_concurrent_tasks:
            Max number of URLs to process concurrently in this run.
            This also controls the semaphore size for Playwright fetches.
        filter_keys:
            Optional list of URLs to restrict processing to.
            If provided, any (url, iteration) not in this set is ignored.
        retry_errors:
            If True, include rows with status=ERROR and IN_PROGRESS
            (subject to lease expiration) in the worklist.
            If False, only rows with status=NEW are considered.
        lease_minutes:
            Lease duration (in minutes) for each claimed row. If a worker crashes
            or hangs beyond this period, other workers may reclaim the row.

    Returns:
        None. Side effects:
            - Inserts rows into `web_page`.
            - Updates `pipeline_control` claim + status fields.
            - Steps FSM from URL to WEB_PAGE on successful rows.
    """
    statuses = (
        (PipelineStatus.NEW, PipelineStatus.ERROR, PipelineStatus.IN_PROGRESS)
        if retry_errors
        else (PipelineStatus.NEW,)
    )

    worklist: list[tuple[str, int]] = get_claimable_worklist(
        stage=PipelineStage.WEB_PAGE,
        status=statuses,
        max_rows=max_concurrent_tasks * 4 or 1000,
    )

    # Optional filtering by URL
    if filter_keys:
        filter_set = set(filter_keys)
        worklist = [(u, it) for (u, it) in worklist if u in filter_set]

    if not worklist:
        logger.info("📭 No URLs to fetch at stage 'URL'.")
        return

    worker_id = generate_worker_id(prefix="web_scrape")

    logger.info(
        "🚀 Starting web scrape pipeline | %d items | worker_id=%s",
        len(worklist),
        worker_id,
    )

    tasks = await process_web_scrape_batch_async_fsm(
        items=worklist,
        worker_id=worker_id,
        lease_minutes=lease_minutes,
        no_of_concurrent_workers=max_concurrent_tasks,
    )

    await asyncio.gather(*tasks)
    logger.info("✅ Finished Stage A (URL → WEB_PAGE).")
