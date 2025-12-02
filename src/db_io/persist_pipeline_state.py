# job_bot/db_io/persist_pipeline_state.py
from __future__ import annotations
import logging
from typing import Optional

import pandas as pd

from job_bot.db_io.pipeline_enums import (
    TableName,
    PipelineStatus,
    PipelineTaskState,
    PipelineStage,
)
from job_bot.models.db_table_models import PipelineState
from job_bot.db_io.db_inserters import insert_df_with_config
from job_bot.fsm.fsm_stage_config import PIPELINE_STAGE_SEQUENCE

logger = logging.getLogger(__name__)

URL_ONLY_STAGES: set[PipelineStage] = {PipelineStage.JOB_URLS}  # extend if needed


def _normalize_task_state_for_stage(state_model: PipelineState) -> PipelineTaskState:
    """
    Normalize the human gate for persistence.

    Rules (new model):
      • Preserve explicit human choices:
          - SKIP  → terminal (never process)
          - PAUSED → terminal (machine finished or manually paused)
          - HOLD → non-terminal, keep as-is (manual review)
      • If machine has COMPLETED the stage:
          - If we're at the final stage → PAUSED (pipeline cycle complete)
          - Else                       → READY (eligible for next stage)
      • Otherwise (not completed) → READY (eligibility is managed by lease fields)
    """
    # Safe reads
    status: Optional[PipelineStatus] = getattr(state_model, "status", None)
    task_state: Optional[PipelineTaskState] = getattr(state_model, "task_state", None)
    stage: Optional[PipelineStage] = getattr(state_model, "stage", None)

    # Canonical final stage (import PIPELINE_STAGE_SEQUENCE from your config/module)
    final_stage: PipelineStage = PIPELINE_STAGE_SEQUENCE[-1]

    # 1) Preserve explicit human choices first
    if task_state in (
        PipelineTaskState.SKIP,
        PipelineTaskState.PAUSED,
        PipelineTaskState.HOLD,
    ):
        return task_state  # type: ignore[return-value]

    # 2) Map machine COMPLETED to human gate
    if status == PipelineStatus.COMPLETED:
        if stage == final_stage:
            return PipelineTaskState.PAUSED  # terminal human gate at end of pipeline
        return PipelineTaskState.READY  # allow next stage to pick it up

    # 3) Default: eligible (actual "in-progress" is tracked by lease fields)
    return PipelineTaskState.READY


def update_and_persist_pipeline_state(
    state_model: PipelineState,
    table_name: TableName = TableName.PIPELINE_CONTROL,
) -> None:
    """
    Update and persist a PipelineState by delegating to `insert_df_with_config`.

    FSM semantics
    -------------
    • Called whenever a PipelineState is initialized, advanced, retried, or marked.
    • Guarantees a single authoritative row per (url, iteration) in `pipeline_control`.

    DB semantics
    ------------
    • Implemented as an upsert (update-or-insert) via `insert_df_with_config`.
    • Central inserter applies:
        1. Deduplication (e.g., pk_scoped → delete existing PK rows before insert).
        2. Stamping (created_at/updated_at, iteration, etc., per YAML config).
        3. Schema alignment (column order, defaults).

    Notes
    -----
    • The state is converted to a one-row DataFrame; enums are flattened to `.value`.
    • If you need to carry an existing `created_at`, include it explicitly;
        the inserter’s stamping rules will set it on first insert.

    Args
    ----
    state_model : PipelineState
        The validated PipelineState to persist.
    table_name : TableName, default = PIPELINE_CONTROL
        Target DuckDB table.
    """

    try:
        # Ensure correct project status
        state_model.task_state = _normalize_task_state_for_stage(state_model)

        # todo: debug; delete later after debugging
        for name, field in state_model.model_fields.items():
            val = getattr(state_model, name, None)
            # Avoid calling model_dump to not trigger the error
            try:
                t = type(val).__name__
            except Exception:
                t = "<unrepr>"
            logger.debug("state_model.%s -> %r (%s)", name, val, t)
            # 2) Dump the ENTIRE model in JSON mode so Enums → values, HttpUrl → str
            #    exclude_none=False ensures optional columns (e.g., decision_flag) are included

        for name, field in state_model.model_fields.items():  # Pydantic v2
            value = getattr(state_model, name)
            logger.debug(
                "state_model.%s -> %r (%s)",
                name,
                value,
                type(value).__name__,
            )
            # todo: delete later

        # payload = state_model.model_dump(mode="json", exclude_none=False)
        payload = state_model.model_dump(
            exclude_none=False
        )  # not using mode=json b/c it was causing timestamp related errors

        # 3) (belt-and-suspenders) Ensure url is a plain string
        if "url" in payload:
            payload["url"] = str(payload["url"])

        df = pd.DataFrame([payload])

        # 4) Insert with validating table schemas
        insert_df_with_config(
            df=df,
            table_name=table_name,
            url=str(
                state_model.url
            ),  # Provide url - let inserter resolve iteration inheritance
            # Any stamp:param fields can be supplied via kwargs here if needed:
            # iteration=state_model.iteration, resp_llm_provider=..., resp_model_id=...
        )

        logger.info("✅ Upserted pipeline state for %s", state_model.url)
    except Exception as e:
        logger.error(
            "❌ Failed to upsert pipeline state for %s: %s",
            getattr(state_model, "url", "<unknown>"),
            e,
            exc_info=True,
        )
        raise
