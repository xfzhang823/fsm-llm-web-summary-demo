"""
web_summarize_pipeline_async.py

Stage B: WEB_PAGE → WEB_SUMMARY
===============================

Asynchronous FSM-aware pipeline responsible ONLY for:
- Loading cleaned webpage text from DuckDB (`web_page` table)
- Calling an LLM to summarize the content
- Inserting summaries into DuckDB (`web_summary` table)
- Updating `pipeline_control` with COMPLETED or ERROR
- Advancing FSM stage from WEB_PAGE → WEB_SUMMARY

NO scraping. NO Playwright. This stage assumes Stage A has already populated
`web_page.clean_text` for (url, iteration).

This pipeline mirrors the lease + human-gate + FSM behavior of the job_bot
project but simplified for the web summary demo.

---------------------------------------------------------------------------
📦 High-Level Flow
---------------------------------------------------------------------------

pipeline_control (stage=WEB_PAGE, status=NEW[/ERROR/IN_PROGRESS], task_state=READY)
    |
    v
get_claimable_worklist(stage=WEB_PAGE, ...)
    |
    v
try_claim_one(url, iteration)
    |
    v
summarize_and_persist_webpage_async()
    - load WebPageRow from `web_page` (url, iteration)
    - build WebPageContent
    - call LLM with SUMMARIZE_WEBPAGE_TEXT_PROMPT
    - build WebSummaryRow(url, iteration, summary_text, summary_json, ...)
    - insert_df_with_config(TableName.WEB_SUMMARY)
    |
    v
release_one(final_status=COMPLETED or ERROR)
fsm.step()   # WEB_PAGE → WEB_SUMMARY

---------------------------------------------------------------------------
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Tuple, Optional, Dict, Any

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
from db_io.get_db_connection import get_db_connection
from fsm.pipeline_enums import (
    PipelineStage,
    PipelineStatus,
    TableName,
    LLMProvider,
)
from fsm.pipeline_fsm_manager import PipelineFSMManager
from llm_api.llm_api_utils_async import (
    call_openai_api_async,
    call_anthropic_api_async,
)
from models.llm_response_models import JSONResponse
from models.web_content_models import WebSummaryJSON
from models.web_content_models import WebPageContent
from models.db_table_models import WebPageRow, WebSummaryRow

from config.project_config import (
    OPENAI,
    ANTHROPIC,
    GPT_4_1_NANO,
    CLAUDE_HAIKU,
)

from llm_api.prompt_templates_web import (
    SUMMARIZE_WEBPAGE_TEXT_PROMPT,
    SUMMARIZE_WEBPAGE_TO_JSON_PROMPT,
)

logger = logging.getLogger(__name__)


# ============================================================================
# 0. Small helpers
# ============================================================================


def _load_web_page_row(url: str, iteration: int) -> Optional[WebPageRow]:
    """
    Load the latest `WebPageRow` for a given (url, iteration) from DuckDB.

    This is a **synchronous** helper meant to be wrapped with `asyncio.to_thread`.

    Selection rules:
      - Match exactly on (url, iteration)
      - Prefer the most recently created row (ORDER BY created_at DESC LIMIT 1)

    Returns:
        WebPageRow instance if found and validated, otherwise None.
    """
    con = get_db_connection()
    query = """
        SELECT *
        FROM web_page
        WHERE url = ? AND iteration = ?
        ORDER BY created_at DESC
        LIMIT 1
    """
    df = con.execute(query, [url, iteration]).df()

    if df.empty:
        logger.warning("No web_page row found for %s [iter=%s]", url, iteration)
        return None

    row_dict = df.iloc[0].to_dict()

    try:
        row = WebPageRow.model_validate(row_dict)
        return row
    except Exception:
        logger.exception(
            "Failed to validate WebPageRow for %s [iter=%s]", url, iteration
        )
        return None


def _derive_summary_text(summary_payload: Dict[str, Any]) -> Optional[str]:
    """
    Best-effort construction of a human-readable `summary_text`
    from a structured JSON payload (compatible with WebSummaryJSON shape).

    Expected (but not required) keys:
      - title: str
      - bullets: list[str]
      - key_points: list[str]
      - sections: dict[str, list[str]]

    Returns:
        A newline-joined string if anything meaningful is present, else None.
    """
    lines: list[str] = []

    title = summary_payload.get("title")
    if isinstance(title, str) and title.strip():
        lines.append(title.strip())
        lines.append("")  # blank line after title

    # bullets and key_points
    for key in ("bullets", "key_points"):
        val = summary_payload.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str) and item.strip():
                    lines.append(f"- {item.strip()}")
            if val:
                lines.append("")

    # sections: {"Section Name": ["point1", "point2", ...], ...}
    sections = summary_payload.get("sections")
    if isinstance(sections, dict):
        for section_name, items in sections.items():
            if isinstance(section_name, str) and section_name.strip():
                lines.append(section_name.strip() + ":")
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and item.strip():
                        lines.append(f"  - {item.strip()}")
            lines.append("")

    # Final cleanup
    text = "\n".join(lines).strip()
    return text or None


def _normalize_llm_provider_enum(provider: str) -> LLMProvider:
    """
    Map provider string (e.g., 'openai', 'anthropic') to LLMProvider enum.

    Falls back to LLMProvider.OPENAI if mapping fails.
    """
    p = provider.lower().strip()
    try:
        if p == OPENAI:
            return LLMProvider.OPENAI
        if p == ANTHROPIC:
            return LLMProvider.ANTHROPIC
        # If provider already matches an enum value, use it directly.
        return LLMProvider(p)
    except Exception:
        logger.warning("Unknown llm_provider=%s, defaulting to OPENAI", provider)
        return LLMProvider.OPENAI


# ============================================================================
# 1. Low-level worker: summarize webpage + persist into DuckDB
# ============================================================================


async def summarize_and_persist_webpage_async(
    url: str,
    iteration: int,
    semaphore: asyncio.Semaphore,
    *,
    llm_provider: str = OPENAI,
    model_id: str = GPT_4_1_NANO,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> bool:
    """
    Summarize a single webpage (by url, iteration) and persist into `web_summary`.

    Steps:
      1. Load `WebPageRow` from `web_page` for (url, iteration).
      2. Build a `WebPageContent` instance.
      3. Call the configured LLM with SUMMARIZE_WEBPAGE_TEXT_PROMPT.
      4. Interpret the response as a generic JSON payload
        (`JSONResponse` with json_type="generic").
      5. Derive:
            - `summary_json`: the full JSON payload from the LLM
            - `summary_text`: human-readable text synthesized from the JSON
      6. Insert a `WebSummaryRow` into `web_summary` via `insert_df_with_config`.

    Returns:
        True  → successfully summarized + inserted
        False → failure (missing input, LLM error, or DB insert error)

    IMPORTANT:
        - No FSM mutations here.
        - No lease logic here.
        - Caller handles try_claim_one / release_one / fsm.step().
    """
    logger.info("📝 Summarizing webpage: %s [iter=%s]", url, iteration)

    # 1) Load web_page row (blocking → wrap in to_thread)
    web_page_row = await asyncio.to_thread(_load_web_page_row, url, iteration)
    if web_page_row is None:
        logger.warning("Cannot summarize %s [iter=%s] — no WebPageRow", url, iteration)
        return False

    if not web_page_row.clean_text or not str(web_page_row.clean_text).strip():
        logger.warning(
            "Cannot summarize %s [iter=%s] — empty clean_text", url, iteration
        )
        return False

    # 2) Build WebPageContent (LLM input model)
    page_content = WebPageContent(
        url=url,
        title=None,  # can be populated later via metadata if desired
        clean_text=str(web_page_row.clean_text),
        raw_html=web_page_row.html,
    )

    # 3) Call LLM under concurrency semaphore
    async with semaphore:
        prompt = SUMMARIZE_WEBPAGE_TO_JSON_PROMPT.format(
            url=url, content=page_content.clean_text or ""
        )
        logger.debug("LLM summarization prompt for %s:\n%s", url, prompt)

        try:
            if llm_provider.lower() == OPENAI:
                llm_response = await call_openai_api_async(
                    prompt=prompt,
                    model_id=model_id,
                    expected_res_type="json",
                    json_type="web_summary",
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            elif llm_provider.lower() == ANTHROPIC:
                llm_response = await call_anthropic_api_async(
                    prompt=prompt,
                    model_id=model_id,
                    expected_res_type="json",
                    json_type="web_summary",
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            else:
                raise ValueError(f"Unsupported llm_provider: {llm_provider}")
        except Exception:
            logger.exception("❌ LLM call failed for %s [iter=%s]", url, iteration)
            return False

    # 4) Interpret LLM response as validated web_summary JSON
    summary_json: Dict[str, Any] | None = None
    summary_text: str | None = None

    # For this function we *always* call with expected_res_type="json"
    # and json_type="web_summary", so we expect a JSONResponse here.
    if not isinstance(llm_response, JSONResponse):
        logger.error(
            "Expected JSONResponse for web_summary, got %s for %s [iter=%s]",
            type(llm_response),
            url,
            iteration,
        )
        return False

    data = llm_response.data
    if not isinstance(data, dict):
        # validate_web_summary_response should already have enforced dict here,
        # but we guard defensively for safety and to keep the type-checker happy.
        logger.error(
            "Expected dict payload for web_summary, got %s for %s [iter=%s]",
            type(data),
            url,
            iteration,
        )
        return False

    summary_json = data
    summary_text = _derive_summary_text(summary_json)

    if not summary_text and not summary_json:
        logger.warning(
            "LLM response for %s [iter=%s] produced no usable summary",
            url,
            iteration,
        )
        return False

    # 5) Build WebSummaryRow (LLM metadata fields kept minimal for now)
    provider_enum = _normalize_llm_provider_enum(llm_provider)

    summary_row = WebSummaryRow(
        url=url,
        iteration=iteration,
        summary_text=summary_text,
        summary_json=summary_json,
        llm_provider=provider_enum.value,  # type: ignore[call-arg]
        model_id=model_id,  # type: ignore[call-arg]
        tokens_prompt=None,  # can be wired from API usage later
        tokens_completion=None,  # can be wired from API usage later
        tokens_total=None,  # can be wired from API usage later
    )

    df = pd.DataFrame([summary_row.model_dump()])

    # 6) Persist into DuckDB
    try:
        await asyncio.to_thread(insert_df_with_config, df, TableName.WEB_SUMMARY)
        logger.info("✅ Stored summary for %s [iter=%s]", url, iteration)
        return True
    except Exception:
        logger.exception("💾 Failed persisting WebSummaryRow for %s", url)
        return False


# ============================================================================
# 2. Batch processor (lease-aware)
# ============================================================================


async def process_web_summary_batch_async_fsm(
    items: List[Tuple[str, int]],
    *,
    worker_id: str,
    lease_minutes: int = 10,
    no_of_concurrent_workers: int = 5,
    llm_provider: str = OPENAI,
    model_id: str = GPT_4_1_NANO,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> List[asyncio.Task]:
    """
    Prepare asyncio tasks for each (url, iteration) to run Stage B:

        1. try_claim_one(url, iteration, worker_id, lease_minutes)
        2. Stage gate: ensure FSM state is WEB_PAGE
        3. summarize_and_persist_webpage_async(...)
        4. release_one(COMPLETED/ERROR)
        5. fsm.step() once successful (WEB_PAGE → WEB_SUMMARY)

    This function **only** wires together the control-plane logic;
    the actual summarization work is delegated to `summarize_and_persist_webpage_async`.
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
        # 2) Stage gate check (IMPORTANT: release lease if mismatch)
        #    We only want to summarize when FSM is **at** WEB_PAGE.
        # ---------------------------------------------------------
        if fsm.state != PipelineStage.WEB_SUMMARY.value:
            logger.info(
                "⏩ Wrong stage=%s for %s — expected WEB_PAGE; skipping",
                fsm.state,
                url,
            )

            # Release the lease so another stage can pick it up or an operator can fix it
            await asyncio.to_thread(
                release_one,
                url=url,
                iteration=iteration,
                worker_id=worker_id,
                final_status=PipelineStatus.ERROR,  # or SKIPPED depending on policy
            )
            released = True
            return

        try:
            ok = await summarize_and_persist_webpage_async(
                url=url,
                iteration=iteration,
                semaphore=semaphore,
                llm_provider=llm_provider,
                model_id=model_id,
                max_tokens=max_tokens,
                temperature=temperature,
            )

            final_status = PipelineStatus.COMPLETED if ok else PipelineStatus.ERROR

            # todo: debug; delete later
            logger.debug(f"ok is {ok}")
            logger.debug(f"stage before release: {fsm.state}")
            logger.debug(f"status before release: {final_status}")

            released = await asyncio.to_thread(
                release_one,
                url=url,
                iteration=iteration,
                worker_id=worker_id,
                final_status=final_status,
            )
            if not released:
                logger.warning(
                    "⚠️ release_one failed for %s [%s] (worker_id mismatch or no lease row)",
                    url,
                    iteration,
                )

            if ok:
                # Advance FSM: WEB_PAGE → WEB_SUMMARY
                try:
                    fsm.mark_status(
                        status=final_status
                    )  #! need to update status in PipelineFSM class
                    fsm.step()
                except Exception as e:
                    logger.exception(
                        "⚠️ FSM step() failed for %s [%s]: %s", url, iteration, e
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
                    "Failed releasing lease after crash for %s [%s]", url, iteration
                )

        finally:
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
# 3. Stage B orchestrator
# ============================================================================


async def run_web_summary_pipeline_async_fsm(
    *,
    max_concurrent_tasks: int = 3,
    filter_keys: Optional[list[str]] = None,
    retry_errors: bool = False,
    lease_minutes: int = 10,
    llm_provider: str = OPENAI,
    model_id: str = GPT_4_1_NANO,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> None:
    """
    Top-level async runner for Stage B (WEB_PAGE → WEB_SUMMARY).

    Selection criteria for `pipeline_control` rows:
      - stage      = WEB_PAGE
      - status     ∈
          - {NEW}                               if retry_errors == False
          - {NEW, ERROR, IN_PROGRESS}           if retry_errors == True
      - task_state = READY      (enforced inside get_claimable_worklist)
      - lease      = expired or unclaimed

    Then, for each (url, iteration) pair:
      - attempt to claim the row (try_claim_one)
      - run summarization worker (summarize_and_persist_webpage_async)
      - release lease with final status (COMPLETED or ERROR)
      - advance FSM (WEB_PAGE → WEB_SUMMARY) on success

    Args:
        max_concurrent_tasks:
            Max number of (url, iteration) pairs to process in this run.
            Also used to size the inner LLM concurrency semaphore.

        filter_keys:
            Optional list of URLs; if provided, restricts processing to URLs
            present in this list.

        retry_errors:
            If True, allow rows with status in {ERROR, IN_PROGRESS} to be
            re-claimed in addition to NEW.

        lease_minutes:
            Lease duration for each claimed row in `pipeline_control`.

        llm_provider:
            String identifier of the LLM provider. Typically one of:
                - OPENAI (from config.project_config)
                - ANTHROPIC

        model_id:
            Model identifier for the chosen provider (e.g. GPT_4_1_NANO, CLAUDE_HAIKU).

        max_tokens:
            Maximum tokens for the LLM completion.

        temperature:
            Sampling temperature for the LLM.

    Returns:
        None. Logs progress and errors. All DB mutations are handled internally.
    """
    statuses = (
        (PipelineStatus.NEW, PipelineStatus.ERROR, PipelineStatus.IN_PROGRESS)
        if retry_errors
        else (PipelineStatus.NEW,)
    )

    worklist: list[tuple[str, int]] = get_claimable_worklist(
        stage=PipelineStage.WEB_SUMMARY,
        status=statuses,
        max_rows=max_concurrent_tasks * 4 or 1000,
    )

    if filter_keys:
        filter_set = set(filter_keys)
        worklist = [(u, it) for (u, it) in worklist if u in filter_set]

    if not worklist:
        logger.info("📭 No items to summarize at stage 'WEB_SUMMARY'.")
        return

    worker_id = generate_worker_id(prefix="web_summary")

    logger.info(
        "🚀 Starting web summary pipeline | %d items | worker_id=%s | provider=%s | model=%s",
        len(worklist),
        worker_id,
        llm_provider,
        model_id,
    )

    tasks = await process_web_summary_batch_async_fsm(
        items=worklist,
        worker_id=worker_id,
        lease_minutes=lease_minutes,
        no_of_concurrent_workers=max_concurrent_tasks,
        llm_provider=llm_provider,
        model_id=model_id,
        max_tokens=max_tokens,
        temperature=temperature,
    )

    await asyncio.gather(*tasks)
    logger.info("✅ Finished Stage B (WEB_SUMMARY).")
