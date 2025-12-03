"""
fsm/state_sync.py

Control-plane helpers that delegate to the unified loaders/inserters.

- load_pipeline_state(url): returns the latest PipelineState for a URL
  using db_loaders.load_table with table-level order_by config.

- update_and_persist_pipeline_state(state): YAML-driven insert via
  db_inserters.insert_df_with_config (dedup + stamps handled centrally).
"""

from __future__ import annotations

import logging
import json
import time
from typing import Any, cast, Optional
from enum import Enum
import pandas as pd


# User defined
from db_io.get_db_connection import get_db_connection
from db_io.db_loaders import load_table
from db_io.persist_pipeline_state import update_and_persist_pipeline_state
from models.db_table_models import PipelineState
from fsm.pipeline_fsm import PipelineFSM
from fsm.pipeline_enums import (
    TableName,
    PipelineStatus,
    PipelineStage,
)
from fsm.fsm_stage_config import next_stage


logger = logging.getLogger(__name__)

URL_ONLY_STAGES: set[PipelineStage] = {PipelineStage.URL}  # extend if needed


def load_pipeline_state(
    url: str,
    table_name: TableName = TableName.PIPELINE_CONTROL,
) -> Optional[PipelineState]:
    """
    Load the current (latest) PipelineState for a given URL.

    This leverages `db_loaders.load_table`, which applies the loader YAML:
      • filters:   (url, iteration, etc.)
      • order_by:  table/default order (e.g., updated_at DESC, created_at DESC)
      • rehydrate: returns a PipelineState model when configured

    Args:
        url: The job URL to fetch.
        table_name: Target table (default: PIPELINE_CONTROL).

    Returns:
        PipelineState if found; otherwise None.
    """
    try:
        model = load_table(table_name, url=url)
        # When a single URL is requested and a rehydrator exists, load_table
        # returns the typed model (not a DataFrame).
        if isinstance(model, PipelineState):
            return model
        # If your loader for pipeline_control isn’t modeled yet, `model` could be a DF.
        if isinstance(model, pd.DataFrame) and not model.empty:
            # Take first row (loader should already order newest first)
            return PipelineState(**model.iloc[0].to_dict())
        return None
    except Exception as e:
        logger.error(
            "❌ Failed to load pipeline state for %s: %s", url, e, exc_info=True
        )
        return None


def retry_error_one(url: str, *, table: TableName = TableName.PIPELINE_CONTROL) -> bool:
    """
    If row's *status* == ERROR, set it back to NEW at the same stage, persisted.
    Uses PipelineFSM for invariant handling, but does not touch task_state.
    """
    state = load_pipeline_state(url, table_name=table)
    if state is None:
        logger.warning("retry_error_one: no PipelineState for url=%s", url)
        return False

    # Only status gate; do not consult/mutate task_state
    if str(state.status).lower() != PipelineStatus.ERROR.value:
        return False

    fsm = PipelineFSM(state)
    # status-only change at same stage
    fsm.mark_status(
        PipelineStatus.NEW,
        notes="auto-reset from ERROR by state_sync.retry_error_one",
        table_name=table,
    )

    logger.info("♻️ retry_error_one: %s → status=NEW @ %s", url, state.stage.value)
    return True


def advance_completed_one(
    url: str, *, table: TableName = TableName.PIPELINE_CONTROL
) -> bool:
    """
    If row's *status* == COMPLETED, advance exactly one stage and set status=NEW.
    Does NOT touch task_state (i.e., we avoid PipelineFSM.step()).
    """
    t0 = time.perf_counter()

    # 1) Load state (NOTE: if you’re clamping iteration to 0, ensure load_pipeline_state does the same)
    state = load_pipeline_state(url, table_name=table)
    if state is None:
        logger.warning(
            "advance_completed_one: no PipelineState for url=%s (table=%s)",
            url,
            table.value if hasattr(table, "value") else table,
        )
        return False

    logger.debug("advance_completed_one: loaded state %s", _state_summary(state))

    # 2) Normalize and guard status
    raw_status = getattr(state, "status", None)
    try:
        status_str = (
            raw_status.value
            if hasattr(raw_status, "value") and raw_status is not None
            else str(raw_status).lower()
        )
    except Exception:
        status_str = str(raw_status)

    logger.debug(
        "advance_completed_one: raw_status=%r normalized=%r", raw_status, status_str
    )

    if status_str != PipelineStatus.COMPLETED.value:
        logger.debug(
            "advance_completed_one: (%s, iter=%s) not COMPLETED; status=%r",
            url,
            getattr(state, "iteration", None),
            status_str,
        )
        return False

    # 3) Normalize stage and compute next
    raw_stage = getattr(state, "stage", None)
    try:
        cur_stage = (
            raw_stage
            if isinstance(raw_stage, PipelineStage)
            else PipelineStage(str(raw_stage))
        )
    except Exception as e:
        logger.error(
            "advance_completed_one: invalid stage for url=%s: %r (%s)",
            url,
            raw_stage,
            e,
        )
        return False

    nxt = next_stage(cur_stage)
    logger.debug(
        "advance_completed_one: current_stage=%s next_stage=%s",
        getattr(cur_stage, "value", cur_stage),
        getattr(nxt, "value", nxt),
    )

    if nxt is None:
        logger.info(
            "advance_completed_one: %s already at final stage %s; no-op",
            url,
            getattr(cur_stage, "value", cur_stage),
        )
        return False

    # 4) Persist change (single upsert), log pre/post snapshots
    pre_snapshot = _state_summary(state)
    state.stage = nxt
    state.status = PipelineStatus.NEW

    try:
        update_and_persist_pipeline_state(state, table)
        decision_flag.approve(url, nxt)  # or approve(url, nxt) if you want auto-advance

    except Exception:
        logger.exception("advance_completed_one: persist failed for url=%s", url)
        return False

    # 5) Quick read-back verification (helps catch “allowed_fields”/upsert masks)
    try:
        post = load_pipeline_state(url, table_name=table)
        post_snapshot = _state_summary(post) if post else "<missing>"
        logger.debug(
            "advance_completed_one: pre=%s -> post=%s", pre_snapshot, post_snapshot
        )
    except Exception:
        logger.exception("advance_completed_one: read-back failed for url=%s", url)

    dt_ms = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "➡️ advance_completed_one: %s %s → %s (status=NEW) in %dms",
        url,
        getattr(cur_stage, "value", cur_stage),
        getattr(nxt, "value", nxt),
        dt_ms,
    )
    return True


def _enum_val(x: Any) -> Any:
    """Return Enum.value for Enum instances; identity otherwise."""
    try:
        if isinstance(x, Enum):
            return x.value
    except Exception:
        pass
    return x


def _get_control_row(
    url: str, iteration: Optional[int] = None, table_name: str = "pipeline_control"
):
    con = get_db_connection()
    if iteration is None:
        df = con.execute(
            f"SELECT * FROM {table_name} WHERE url = ? ORDER BY iteration DESC LIMIT 1",
            (url,),
        ).df()
    else:
        df = con.execute(
            f"SELECT * FROM {table_name} WHERE url = ? AND iteration = ?",
            (url, iteration),
        ).df()
    return None if df.empty else df.iloc[0].to_dict()


# for logging
def _state_summary(state) -> str:
    """
    Compact one-line summary for PipelineState-like objects.
    Avoids huge dumps while surfacing the key fields we care about.
    """

    # Safely pull attrs whether they're Enums or strings
    def _val(x):
        if x is None:
            return None
        try:
            return x.value  # Enum
        except AttributeError:
            return str(x)

    # Collect a stable subset of fields (add/remove as needed)
    fields = {
        "url": getattr(state, "url", None),
        "iteration": getattr(state, "iteration", None),
        "stage": _val(getattr(state, "stage", None)),
        "status": _val(getattr(state, "status", None)),
        "task_state": _val(getattr(state, "task_state", None)),
        "updated_at": getattr(state, "updated_at", None),
        "version": getattr(state, "version", None),
    }
    # Render compact JSON for readability in logs
    return json.dumps(fields, default=str, separators=(",", ":"))
