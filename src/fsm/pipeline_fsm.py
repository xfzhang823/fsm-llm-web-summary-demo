"""
fsm/pipeline_fsm.py

Finite State Machine (FSM) for managing pipeline stage progression.

This module implements a state machine to control sequential advancement through
a predefined pipeline workflow.

Key features:

1. Linear Stage Progression:
   - Defined stages: JOB_URLS → JOB_POSTINGS → ... → FINAL_RESPONSIBILITIES
   - Strictly sequential transitions via 'advance' trigger
   - Automatic persistence to DuckDB after each transition

2. Core Components:
   - PipelineFSM: Main controller class wrapping transitions.Machine
   - State tracking via PipelineState Pydantic model
   - Automatic timestamping of state changes

3. Key Operations:
   - step(): Advance to next stage and persist
   - get_current_stage(): Check current position
   - get_next_stage(): Preview upcoming stage

Usage:
    state = PipelineState(url="...")
    fsm = PipelineFSM(state)
    fsm.step()  # Advances pipeline and saves state


    * Example Walk-Through Flowchart:
    +-------------------+
    |   pipeline_control|
    |   (DuckDB table)  |
    +-------------------+
            |
            | Initial insert (status='new', stage='job_urls')
            v
    +-----------------------------+
    | url: https://job.com/abc123|
    | stage: job_urls            |
    | status: new                |
    +-----------------------------+

                    |
                    |   job_postings pipeline runs
                    |   └─ queries where stage = 'job_urls'
                    |   └─ scrapes content
                    |   └─ FSM: fsm.step() ➝ 'job_postings'
                    v

    +-----------------------------+
    | url: https://job.com/abc123|
    | stage: job_postings        |
    | status: new                |
    +-----------------------------+

                    |
                    |   extracted_requirements pipeline runs
                    |   └─ queries where stage = 'job_postings'
                    |   └─ extracts requirements
                    |   └─ FSM: fsm.step() ➝ 'extracted_requirements'
                    v

    +-------------------------------+
    | url: https://job.com/abc123   |
    | stage: extracted_requirements |
    | status: new                   |
    +-------------------------------+

Note: Designed for forward-only progression through irreversible stages.
"""

# Imports
import logging
from datetime import datetime
from typing import Optional

# User defined
from db_io.persist_pipeline_state import update_and_persist_pipeline_state
from fsm.pipeline_enums import (
    PipelineStage,
    PipelineStatus,
    PipelineTaskState,
    TableName,
)
from fsm.fsm_stage_config import (
    PIPELINE_STAGE_SEQUENCE,  # ordered enums
    next_stage as guarded_next_stage,
)
from models.db_table_models import PipelineState


logger = logging.getLogger(__name__)


class PipelineFSM:
    """
    A finite state machine (FSM) controller for managing job progress through
    atomic pipeline stages.

    This controller wraps a `PipelineState` model and tracks a job's current
    `stage` and `status`, providing stepwise advancement through a linear sequence
    of well-defined stages
    (e.g., job_urls → job_postings → extracted_requirements → … → final_export).

    After each successful transition, the FSM updates the associated
    `PipelineState` record and persists it to the DuckDB `pipeline_control` table.

    ---
    Workflow:
        1. Initialize with a `PipelineState` object (defaults to the first stage
           if unset).
        2. Call `.step()` to advance from the current stage to the next defined stage.
        3. After advancing, the `PipelineState` is updated with the new stage,
           status, and `updated_at` timestamp.
        4. FSM persists the state via the upsert helper
           (`update_and_persist_pipeline_state`).
        5. If the job is already at the final stage, the FSM does not advance further.

    ---
    Class-level Constants:
        STAGES (list[PipelineStage]): Ordered list of atomic pipeline stages.
        TRANSITIONS (list[dict]): Advance transitions between each stage
        (used by the FSM engine).

    ---
    Instance Fields:
        state_model (PipelineState): Pydantic model containing job metadata and
            pipeline tracking info.
        _state (PipelineStage): Internal tracker of the FSM's current stage
            (mirrors `state_model.stage`).
        _status (PipelineStatus): Internal tracker of the FSM's current status
            (mirrors `state_model.status`).

    ---
    Key Methods:
        step(): Advance to the next stage and persist updated state.
        get_current_stage(): Returns the current stage (enum).
        get_next_stage(): Returns the next stage based on the transition map.
        mark_status(): Set a new status and persist the change.

    ---
    Example:
        >>> fsm = PipelineFSM(state)
        >>> fsm.step()  # Moves from current stage to the next one and updates DuckDB
        >>> fsm.mark_status(PipelineStatus.IN_PROGRESS,
        ...                 notes="Requirements parsed and stored.")
    """

    STAGES = PIPELINE_STAGE_SEQUENCE
    TRANSITIONS = [
        {"source": s, "dest": d}
        for s, d in zip(PIPELINE_STAGE_SEQUENCE[:-1], PIPELINE_STAGE_SEQUENCE[1:])
    ]

    def __init__(self, state: PipelineState):
        """
        Initialize the FSM with a given PipelineState.

        The initializer stores the control-plane record (`stage`, `status`)
        as strongly typed enums (`PipelineStage`, `PipelineStatus`) and keeps
        a reference to the full Pydantic model for persistence.

        Args:
            state (PipelineState):
                The current pipeline state record loaded from the
                `pipeline_control` table.
        """
        # New control-plane fields: stage/status (enums)
        self.state_model = state
        # Normalize to PipelineStage enum internally
        self._state: PipelineStage = (
            state.stage
            if isinstance(state.stage, PipelineStage)
            else PipelineStage(str(state.stage))
        )
        self._status: PipelineStatus = (
            state.status
            if isinstance(state.status, PipelineStatus)
            else PipelineStatus(str(state.status))
        )

    @property
    def state(self) -> PipelineStage:
        """Current state of the FSM."""
        return self._state

    def get_current_stage(self) -> PipelineStage:
        """
        Get the current stage from the pipeline state.

        Returns:
            str: The current pipeline stage.
        """
        return self._state

    def get_next_stage(self) -> Optional[PipelineStage]:
        """
        Get the next stage in the pipeline based on the current stage.

        Returns:
            str | None: The next pipeline stage or None if at final stage.
        """
        try:
            return guarded_next_stage(self._state)
        except Exception as e:
            logger.warning(
                "⚠️ Could not determine next stage from '%s': %s", self._state, e
            )
            return None

    def mark_status(
        self,
        status: PipelineStatus,
        notes: Optional[str] = None,
        table_name: TableName = TableName.PIPELINE_CONTROL,
    ) -> None:
        """
        Update the pipeline status (without advancing the stage) and persist.

        Args:
            status: New PipelineStatus (e.g., IN_PROGRESS, ERROR, SKIPPED).
            notes: Optional free-text note to append (timestamped).
            table_name: Control-plane table name (default: pipeline_control).
        """
        try:
            # Update internal and model status as enums
            self._status = status
            self.state_model.status = status

            # Append timestamped note (preserve prior notes)
            if notes:
                line = f"[{datetime.now().isoformat(timespec='seconds')}] {notes}"
                self.state_model.notes = (
                    f"{line}\n{self.state_model.notes}".strip()
                    if self.state_model.notes
                    else line
                )

            # Touch updated_at only (created_at is immutable after insert)
            self.state_model.updated_at = datetime.now()

            update_and_persist_pipeline_state(self.state_model, table_name)

            logger.info(
                "📝 Status updated for %s: %s — %s",
                self.state_model.url,
                status.name,
                notes or "",
            )
        except Exception as e:
            logger.error(
                "❌ Failed to update status for %s: %s",
                self.state_model.url,
                e,
                exc_info=True,
            )

    def step(self, table_name: TableName = TableName.PIPELINE_CONTROL) -> None:
        """
        Advance to the next stage (mutates both `stage` and `status` and may
            set `task_state`).
        Contract:
        - If already at the final stage, idempotently set `task_state=COMPLETED`
            and return.
        - Otherwise:
            1) mark current stage as COMPLETED (in-memory),
            2) hop to next stage,
            3) set next stage status to NEW,
            4) if we landed on final stage, set task_state=COMPLETED,
            5) persist all changes in a single UPDATE that targets the row by
                (url, iteration) only.
        """
        try:
            # --- Resolve canonical final stage explicitly (avoid ordering bugs) ---
            ordered: tuple[PipelineStage, ...] = tuple(self.STAGES)
            final_stage: PipelineStage = ordered[-1]

            # --- Already final? Just idempotently stamp lifecycle and return ---
            if self._state == final_stage:
                if self.state_model.task_state not in (
                    PipelineTaskState.PAUSED,
                    PipelineTaskState.HOLD,
                    PipelineTaskState.SKIP,
                ):
                    # mark human gate as PAUSED to indicate no further machine actions
                    self.state_model.task_state = PipelineTaskState.PAUSED

                    #! 🔓 release any stale lease as we are final
                    self._release_lease()

                    self.state_model.updated_at = datetime.now()
                    update_and_persist_pipeline_state(self.state_model, table_name)
                self._log_info("✅ Already at final stage: %s", self._state.name)
                return

            self._log_info(
                "⏩ Advancing %s from '%s'", self.state_model.url, self._state.name
            )

            # --- 1) Close current stage ---
            self.state_model.status = PipelineStatus.COMPLETED

            # --- 2) Compute next stage deterministically ---
            try:
                idx = ordered.index(self._state)
            except ValueError:
                raise RuntimeError(
                    f"Current stage {self._state} not in canonical order."
                )
            if idx >= len(ordered) - 1:
                # defensive: shouldn't happen due to early return above
                self._log_info("✅ Already at final stage (defensive branch).")
                self.state_model.updated_at = datetime.now()
                update_and_persist_pipeline_state(
                    self.state_model,
                    table_name,
                )
                return

            next_stage = ordered[idx + 1]

            # --- 3) Hop & prime next stage ---
            self._state = next_stage
            self.state_model.stage = next_stage
            self.state_model.status = PipelineStatus.NEW

            # --- 4) If we just landed on final, close lifecycle ---
            if next_stage == final_stage:
                self.state_model.task_state = PipelineTaskState.PAUSED
            else:
                # Make the next stage pickable
                self.state_model.task_state = PipelineTaskState.READY
                # Optional gating reset if you use it

            #! 🔓 always release lease when we hop to a new stage
            self._release_lease()

            # --- 5) Persist once, by PK only (NOT filtering by old stage!) ---
            self.state_model.updated_at = datetime.now()

            update_and_persist_pipeline_state(
                self.state_model,
                table_name,
            )

            self._log_info(
                "✅ Advanced to: %s (status=%s)",
                self.state_model.stage.name,
                self.state_model.status.value,
            )

        except Exception as e:
            self._log_error(
                "❌ Failed to advance stage for %s: %s",
                self.state_model.url,
                e,
                exc_info=True,
            )

    # Helper: normalize enum fields so your DB sees strings, not Enum objects
    def _serialize_for_update(self, m):
        return {
            "stage": getattr(m.stage, "value", str(m.stage)),
            "status": getattr(m.status, "value", str(m.status)),
            "task_state": (
                getattr(m.task_state, "value", str(m.task_state))
                if m.task_state is not None
                else None
            ),
            "updated_at": m.updated_at,
            # include any other columns you persist here (notes, version, etc.)
        }

    def _log_info(self, msg, *args):  # tiny sugar so this patch is drop-in
        logger.info(msg, *args)

    def _log_error(self, msg, *args, **kwargs):
        logger.error(msg, *args, **kwargs)

    def _release_lease(self) -> None:
        """Clear worker lease fields so the row becomes claimable again."""
        # Only set these if the model actually has them (future-proofing)
        if hasattr(self.state_model, "is_claimed"):
            self.state_model.is_claimed = False
        if hasattr(self.state_model, "worker_id"):
            self.state_model.worker_id = None
        if hasattr(self.state_model, "lease_until"):
            self.state_model.lease_until = None
