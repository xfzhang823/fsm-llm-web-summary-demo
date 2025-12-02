"""
pipelines_with_fsm/pipeline_stage_config.py

Defines the canonical ordered rail for the *web summary demo* pipeline.

FSM Stages:
    URL → WEB_PAGE → WEB_SUMMARY

This ordering drives:
    - pipeline_control.stage transitions
    - PipelineFSMManager.step() progression
    - Stage gate checks in Stage A and Stage B
"""

from typing import Optional
from fsm.pipeline_enums import PipelineStage

# ---------------------------------------------------------------------------
# Canonical FSM rail for the web summary pipeline
# ---------------------------------------------------------------------------

PIPELINE_STAGE_SEQUENCE: list[PipelineStage] = [
    PipelineStage.URL,
    PipelineStage.WEB_PAGE,
    PipelineStage.WEB_SUMMARY,
]

# String values (useful for SQL, logging, or UI)
PIPELINE_STAGE_SEQUENCE_VALUES = [s.value for s in PIPELINE_STAGE_SEQUENCE]

# Derived maps for next/previous lookup
_NEXT = {
    cur: (
        PIPELINE_STAGE_SEQUENCE[i + 1] if i + 1 < len(PIPELINE_STAGE_SEQUENCE) else None
    )
    for i, cur in enumerate(PIPELINE_STAGE_SEQUENCE)
}

_PREV = {
    cur: (PIPELINE_STAGE_SEQUENCE[i - 1] if i - 1 >= 0 else None)
    for i, cur in enumerate(PIPELINE_STAGE_SEQUENCE)
}

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def next_stage(cur: PipelineStage) -> Optional[PipelineStage]:
    """Return the next stage in the FSM rail (or None if at the end)."""
    return _NEXT.get(cur)


def prev_stage(cur: PipelineStage) -> Optional[PipelineStage]:
    """Return the previous stage in the FSM rail (or None if at the beginning)."""
    return _PREV.get(cur)


def validate_stage_set() -> None:
    """
    Ensure the rail includes **every** PipelineStage exactly once.

    This guards against drift when adding/removing stages or updating enums.
    """
    assert set(PIPELINE_STAGE_SEQUENCE) == set(
        PipelineStage
    ), "FSM rail mismatch: PIPELINE_STAGE_SEQUENCE != PipelineStage"
