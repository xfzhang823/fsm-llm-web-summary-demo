"""
db_io/db_utils.py

Utility functions for querying DuckDB pipeline control metadata.

These helpers power:
- Worklist operations (claim/lease/release) for the machine lifecycle
- URL discovery/filtering (stage/status/task_state gating)
- Single-URL lookups
- Reporting/summaries
- DataFrame/schema hygiene for DuckDB I/O
- DDL generation from Pydantic models

Design:
- Public API grouped by usage frequency (worklist & discovery first)
- Private/internal helpers at the bottom
"""

# === Module header & imports ===================================================

# Standard
import logging
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    List,
    Union,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    get_origin,
    get_args,
    Sequence,
)
from datetime import datetime
import json
import uuid
from enum import Enum

# Third-party
from pydantic import BaseModel, HttpUrl
from pydantic_core import PydanticUndefined
import pandas as pd

# Project level
from job_bot.db_io.get_db_connection import get_db_connection
from job_bot.db_io.pipeline_enums import (
    PipelineStage,
    PipelineStatus,
    PipelineTaskState,
    TableName,
)

logger = logging.getLogger(__name__)
E = TypeVar("E")  # enum type


# === Worklist API: claim / lease / release (machine lifecycle) =================


def generate_worker_id(prefix: Optional[str] = "runner") -> str:
    """
    Generate a short worker_id for lease claims.

    Args:
        prefix: Optional prefix label. If falsy, defaults to 'runner'.

    Returns:
        str: e.g., 'runner-1a2b3c4d'
    """
    pfx = prefix or "runner"
    return f"{pfx}-{uuid.uuid4().hex[:8]}"


def get_claimable_worklist(
    *,
    stage: PipelineStage,
    status: Union[PipelineStatus, Sequence[PipelineStatus]] = PipelineStatus.NEW,
    max_rows: Optional[int] = 1000,
) -> List[Tuple[str, int]]:
    """
    Return (url, iteration) pairs eligible for claiming by the machine process.

    Conditions:
      - Matches stage & status (supports one or many statuses)
      - Human gate allows it: task_state = 'READY'
      - Not claimed OR lease expired

    Args:
        stage: The pipeline stage to filter on.
        status: One or more machine lifecycle statuses to match (default NEW).
        max_rows: Optional limit on returned rows.

    Returns:
        List[(url, iteration)]

            Workflow:
        - Filters by FSM lifecycle: stage = ? AND status IN (..)
        - Enforces human gate: task_state = 'READY'
        - Enforces lease rules:
            (is_claimed = FALSE OR lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP)
        - Optional LIMIT to keep batches small

    Example:
        items = get_claimable_worklist(
            stage=PipelineStage.JOB_URLS,
            status=(PipelineStatus.NEW, PipelineStatus.ERROR),
            max_rows=500
        )

    >>> Exmple output:
    [
        ("https://example.com/job1", 1),
        ("https://example.com/job2", 1),
    ]
    """
    # Normalize statuses to a tuple of enum values (strings)
    if isinstance(status, PipelineStatus):
        statuses = (status.value,)
    else:
        statuses = tuple(
            s.value if isinstance(s, PipelineStatus) else str(s) for s in status
        )

    if not statuses:
        return []

    in_placeholders = ", ".join(["?"] * len(statuses))
    lim_sql = f"LIMIT {int(max_rows)}" if max_rows else ""

    sql = f"""
        SELECT url, iteration
        FROM pipeline_control
        WHERE stage = ?
          AND status IN ({in_placeholders})
          AND task_state = 'READY'
          AND (
                is_claimed = FALSE
             OR lease_until IS NULL
             OR lease_until < CURRENT_TIMESTAMP
          )
        ORDER BY created_at, url
        {lim_sql}
    """

    # todo: debug; delete later
    logger.debug("CLAIM SQL:\n%s", sql)
    logger.debug("CLAIM PARAMS=%r", (stage.value, *statuses))

    con = get_db_connection()
    try:
        params = (stage.value, *statuses)
        df = con.execute(sql, params).df()

        # todo: debug; delete later
        logger.debug("CLAIM DF ROWS=%d", 0 if df is None else len(df))
        logger.debug(
            "CLAIM DF HEAD=%s", None if df is None else df.head().to_dict("records")
        )

        return list(df.itertuples(index=False, name=None))
    finally:
        con.close()


def try_claim_one(
    *, url: str, iteration: int, worker_id: str, lease_minutes: int = 15
) -> Optional[Dict[str, Any]]:
    """
    Atomically claim a single (url, iteration) row for processing.

    Marks the row as claimed (`is_claimed=TRUE`), assigns a `worker_id`,
    sets a lease expiration (`lease_until = CURRENT_TIMESTAMP + N minutes`),
    and updates `status` to `IN_PROGRESS`—but only if the row is currently
    READY and not actively leased.

    Returns:
        dict: The updated pipeline_control row on success.
        None: If the row was not claimable (e.g., already claimed or not READY).

    Notes:
        - Used by FSM runners to enforce one-worker-per-row exclusivity.
        - Lease duration is configurable via `lease_minutes`.
    """
    con = get_db_connection()
    try:
        sql = """
            UPDATE pipeline_control
            SET is_claimed  = TRUE,
                worker_id   = ?,
                lease_until = CURRENT_TIMESTAMP + (? * INTERVAL 1 MINUTE),
                status      = ?,
                updated_at  = CURRENT_TIMESTAMP
            WHERE url = ?
              AND iteration = ?
              AND task_state = 'READY'
              AND (
                    is_claimed = FALSE
                 OR lease_until IS NULL
                 OR lease_until < CURRENT_TIMESTAMP
              )
            RETURNING *
        """
        params = (
            worker_id,
            lease_minutes,
            PipelineStatus.IN_PROGRESS.value,
            url,
            iteration,
        )
        res = con.execute(sql, params)
        if res is None:
            logger.warning(
                "⚠️ try_claim_one(): no result returned from DuckDB execute()"
            )
            return None

        row = res.fetchone()
        if not row:
            return None

        cols = [c[0] for c in res.description]  # type: ignore[reportOptionalIterable]

        return dict(zip(cols, row))
    finally:
        con.close()


def renew_lease(
    *, url: str, iteration: int, worker_id: str, lease_minutes: int = 15
) -> bool:
    """
    Extend a lease mid-task to avoid expiry.

    Args:
        url: Row URL key.
        iteration: Iteration number for the row.
        worker_id: Must match the current holder.
        lease_minutes: Lease extension.

    Returns:
        True if updated; False otherwise.
    """
    con = get_db_connection()
    try:
        row = con.execute(
            """
            UPDATE pipeline_control
            SET lease_until = CURRENT_TIMESTAMP + INTERVAL ? MINUTE,
                updated_at = CURRENT_TIMESTAMP
            WHERE url = ?
              AND iteration = ?
              AND worker_id = ?
              AND is_claimed = TRUE
            RETURNING url
            """,
            (lease_minutes, url, iteration, worker_id),
        ).fetchone()
        return bool(row)
    finally:
        con.close()


def release_one(
    *, url: str, iteration: int, worker_id: str, final_status: PipelineStatus
) -> bool:
    """
    Release the lease and set the final machine status (e.g., COMPLETED or ERROR).
    Only the current holder may release.

    Args:
        url: Row URL key.
        iteration: Iteration number.
        worker_id: Must match current holder.
        final_status: Final machine lifecycle status.

    Returns:
        True if released; False otherwise.
    """
    con = get_db_connection()
    try:
        row = con.execute(
            """
            UPDATE pipeline_control
            SET is_claimed = FALSE,
                worker_id = NULL,
                lease_until = NULL,
                status = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE url = ?
              AND iteration = ?
              AND worker_id = ?
            RETURNING url
            """,
            (final_status.value, url, iteration, worker_id),
        ).fetchone()
        return bool(row)
    finally:
        con.close()


# === URL Discovery & Filtering =================================================


def get_urls_from_pipeline_control(
    *,
    status: Optional[
        Union[PipelineStatus, str, Sequence[Union[PipelineStatus, str]]]
    ] = None,
    stage: Optional[
        Union[PipelineStage, str, Sequence[Union[PipelineStage, str]]]
    ] = None,
    active_urls_only: bool = True,
    limit: Optional[int] = None,
    table: TableName = TableName.PIPELINE_CONTROL,
    con=None,
) -> List[str]:
    """
    Return DISTINCT URLs from `pipeline_control` filtered by status/stage.

    Notes on lifecycle gating:
      - When `active_urls_only=True` (default), excludes only terminal human-gate
        states {'skipped','completed'}. This is a *broad* notion of "active".
      - For strict "ready-only" gating, use `get_urls_by_stage_and_status` with
        `active_urls_only=True` (see its docstring).

    Args:
        status: Single or list of machine lifecycle statuses (enum or strings).
        stage: Single or list of pipeline stages (enum or strings).
        active_urls_only: Exclude human-gate terminal states (skipped/completed).
        limit: Optional max rows.
        table: TableName, defaults to PIPELINE_CONTROL.
        con: Optional existing connection (will not be closed if provided).

    Returns:
        List[str]: DISTINCT URLs ordered by updated_at DESC.
    """
    owns_con = con is None
    if owns_con:
        con = get_db_connection()

    try:
        where: List[str] = []
        params: List[Any] = []

        status_vals = _to_values(status, PipelineStatus, lower=True)
        stage_vals = _to_values(stage, PipelineStage, lower=False)

        if status_vals:
            placeholders = ",".join(["?"] * len(status_vals))
            where.append(f"status IN ({placeholders})")
            params.extend(status_vals)

        if stage_vals:
            placeholders = ",".join(["?"] * len(stage_vals))
            where.append(f"stage IN ({placeholders})")
            params.extend(stage_vals)

        if active_urls_only:
            # Broadly "active": not in terminal human-gate states.
            where.append(
                "(task_state IS NULL OR task_state NOT IN ('skipped','completed'))"
            )

        where_sql = " AND ".join(where) if where else "1=1"

        sql = f"""
            SELECT DISTINCT url
            FROM {table.value}
            WHERE {where_sql}
            ORDER BY updated_at DESC
            { 'LIMIT ?' if limit is not None else '' }
        """
        if limit is not None:
            params.append(limit)

        rows = con.execute(sql, params).fetchall()
        return [r[0] for r in rows]
    finally:
        if owns_con:
            con.close()


def get_urls_by_stage_and_status(
    stage: PipelineStage,
    *,
    status: Union[PipelineStatus, Sequence[PipelineStatus]] = PipelineStatus.NEW,
    version: Optional[str] = None,
    iteration: Optional[int] = None,
    active_urls_only: bool = True,
) -> List[str]:
    """
    Return DISTINCT URLs matching a specific stage and one or more machine statuses,
    with optional filtering by version/iteration.

    Lifecycle gating (strict):
        When active_urls_only=True (default), ONLY rows with task_state='READY'
        are included. This is the *strict* gating used by claimers.

    Args:
        stage: Pipeline stage to match.
        status: Single status or a sequence of statuses (default NEW).
        version: Optional version filter.
        iteration: Optional iteration filter.
        active_urls_only: If True, gate to task_state='READY'.

    Returns:
        List[str]: DISTINCT URLs ordered by updated_at DESC then created_at DESC.
    """
    # Normalize to a non-empty list of PipelineStatus
    if isinstance(status, PipelineStatus):
        status_list: List[PipelineStatus] = [status]
    else:
        status_list = list(status)
        if not status_list:
            return []

    filters = ["stage = ?"]
    params: List[object] = [stage.value]

    filters.append(f"status IN ({', '.join(['?'] * len(status_list))})")
    params.extend(s.value for s in status_list)

    if active_urls_only:
        allowed = (PipelineTaskState.READY.value,)
        filters.append(f"task_state IN ({', '.join(['?'] * len(allowed))})")
        params.extend(allowed)

    if version is not None:
        filters.append("version = ?")
        params.append(version)
    if iteration is not None:
        filters.append("iteration = ?")
        params.append(iteration)

    sql = f"""
        SELECT DISTINCT url
        FROM pipeline_control
        WHERE {' AND '.join(filters)}
        ORDER BY updated_at DESC, created_at DESC
    """

    con = get_db_connection()
    try:
        df: pd.DataFrame = con.execute(sql, params).df()
        return df["url"].tolist()
    finally:
        con.close()


def get_urls_by_status(
    status: PipelineStatus, *, limit: Optional[int] = None
) -> List[str]:
    """
    Return DISTINCT URLs for rows matching the given machine lifecycle status.

    Args:
        status: Machine lifecycle status to filter (e.g., NEW, IN_PROGRESS).
        limit: Optional max rows.

    Returns:
        List[str]
    """
    con = get_db_connection()
    try:
        lim = f"LIMIT {int(limit)}" if limit else ""
        df = con.execute(
            f"""
            SELECT DISTINCT url
            FROM pipeline_control
            WHERE status = ?
            {lim}
            """,
            (status.value,),
        ).df()
        return df["url"].tolist()
    finally:
        con.close()


def get_urls_by_stage(
    stage: PipelineStage, *, limit: Optional[int] = None
) -> List[str]:
    """
    Return DISTINCT URLs for rows currently in the specified pipeline stage.

    Args:
        stage: PipelineStage enum value (e.g., JOB_POSTINGS).
        limit: Optional max rows.

    Returns:
        List[str]
    """
    con = get_db_connection()
    try:
        lim = f"LIMIT {int(limit)}" if limit else ""
        df = con.execute(
            f"""
            SELECT DISTINCT url
            FROM pipeline_control
            WHERE stage = ?
            {lim}
            """,
            (stage.value,),
        ).df()
        return df["url"].tolist()
    finally:
        con.close()


# todo: to be deprecated entirely; delete later
def get_urls_ready_for_transition(
    stage: PipelineStage, limit: Optional[int] = None
) -> List[str]:
    """
    Specialized picker: rows marked for next-stage transition (decision_flag=1).

    Args:
        stage: Current stage to check.
        limit: Optional max rows.

    Returns:
        List[str]: URLs flagged for transition.
    """
    con = get_db_connection()
    try:
        sql = f"""
            SELECT url
            FROM {TableName.PIPELINE_CONTROL.value}
            WHERE stage = ? AND decision_flag = 1
        """
        params: List[object] = [stage.value]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        df = con.execute(sql, params).df()
        return df["url"].tolist()
    finally:
        con.close()


# === Lookups (single URL state) ===============================================


def get_pipeline_state(url: str) -> pd.DataFrame:
    """
    Return the full `pipeline_control` row for a given URL.

    Args:
        url: The job posting URL.

    Returns:
        pd.DataFrame: Single-row DataFrame (or empty if not found).
    """
    con = get_db_connection()
    try:
        return con.execute(
            """
            SELECT *
            FROM pipeline_control
            WHERE url = ?
            """,
            (url,),
        ).df()
    finally:
        con.close()


def get_current_stage_for_url(url: str) -> Optional[str]:
    """
    Return the current pipeline stage (string) for a given URL, or None if missing.

    Args:
        url: The job posting URL.

    Returns:
        Optional[str]
    """
    df = get_pipeline_state(url)
    if df.empty:
        return None
    return df.iloc[0]["stage"]


# === Reporting / Summaries =====================================================


def get_stage_progress_counts() -> pd.DataFrame:
    """
    Return counts grouped by (stage, status).

    Returns:
        pd.DataFrame with columns (stage, status, count).
    """
    con = get_db_connection()
    try:
        return con.execute(
            """
            SELECT stage, status, COUNT(*) AS count
            FROM pipeline_control
            GROUP BY stage, status
            ORDER BY stage, status
            """
        ).df()
    finally:
        con.close()


def get_recent_urls(limit: int = 10) -> pd.DataFrame:
    """
    Return the most recently updated job URLs.

    Args:
        limit: Number of recent URLs to return (default 10).

    Returns:
        pd.DataFrame: (url, stage, status, updated_at), ordered by updated_at DESC.
    """
    con = get_db_connection()
    try:
        return con.execute(
            """
            SELECT url, stage, status, updated_at
            FROM pipeline_control
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).df()
    finally:
        con.close()


# === DataFrame / Schema Utilities =============================================


def align_df_with_schema(
    df: pd.DataFrame, schema_columns: List[str], strict: bool = False
) -> pd.DataFrame:
    """
    Final validation and alignment step before inserting into DuckDB.

    This function:
      - Ensures all schema columns are present
      - Reorders columns to match DuckDB column order
      - Deduplicates fully identical rows (before timestamps are added)
      - Cleans and coerces problematic types (nested dicts/lists → JSON; BaseModel/HttpUrl → str)
      - Replaces pandas NA/NaN with Python None

    Args:
        df: Input DataFrame to validate and clean.
        schema_columns: Expected column names in final DuckDB order.
        strict:
            If True: missing columns error out.
            If False: missing columns are created with None; extras are dropped with a warning.

    Returns:
        pd.DataFrame: Cleaned, schema-aligned DataFrame.
    """
    df = df.copy()

    # 1) Drop exact duplicates early (pre-metadata)
    original_len = len(df)
    df.drop_duplicates(inplace=True)
    deduped_len = len(df)
    if deduped_len < original_len:
        logger.info(f"Dropped {original_len - deduped_len} duplicate rows")

    # 2) Detect mismatches before adding/removing columns
    missing_before = [c for c in schema_columns if c not in df.columns]
    extra_in_df = [c for c in df.columns if c not in schema_columns]

    # 3) Strict handling for missing columns
    if strict and missing_before:
        raise ValueError(f"Missing columns in DataFrame: {missing_before}")

    # 4) Drop unexpected columns (explicit + observable)
    if extra_in_df:
        logger.warning(f"Dropping unexpected columns: {extra_in_df}")
        df = df.drop(columns=extra_in_df)

    # 5) Add still-missing schema columns as None
    for col in schema_columns:
        if col not in df.columns:
            df[col] = None
            logger.debug(f"Added missing column: {col}")

    # 6) Reorder to schema
    df = df[[col for col in schema_columns]]

    # 7) Coerce problematic types in a single pass per column
    for col in df.columns:
        df[col] = df[col].map(_coerce_cell)
        if df[col].isna().any():
            df[col] = df[col].where(pd.notna(df[col]), None)

    logger.info(f"Aligned DataFrame with schema: {schema_columns}")
    return df


# === DDL Helpers (Pydantic -> DuckDB) =========================================


def duckdb_type_from_annotation(annotation) -> str:
    """
    Map a Pydantic annotation or standard Python type to a DuckDB-compatible SQL type.

    Args:
        annotation: The type annotation from a Pydantic field.

    Returns:
        str: A valid DuckDB SQL type (e.g., TEXT, INTEGER, DOUBLE, TIMESTAMP).
    """
    base = get_origin(annotation) or annotation

    # Optional[X] / Union[X, None]
    if base is Union:
        args = get_args(annotation)
        base = next((arg for arg in args if arg is not type(None)), str)

    # Enum → TEXT
    if isinstance(base, type) and issubclass(base, Enum):
        return "TEXT"

    # Pydantic-specific
    if base in [HttpUrl]:
        return "TEXT"

    # Date/time
    if base in [datetime]:
        return "TIMESTAMP"

    # Primitives
    if base in [str]:
        return "TEXT"
    if base in [int]:
        return "INTEGER"
    if base in [float]:
        return "DOUBLE"
    if base in [bool]:
        return "BOOLEAN"

    # Fallback
    return "TEXT"


def generate_table_schema_from_model(
    model: type[BaseModel],
    table_name: str,
    primary_keys: Optional[List[str]] = None,
) -> str:
    """
    Auto-generate a DuckDB CREATE TABLE DDL statement from a Pydantic model.

    Args:
        model: A Pydantic model class (not instance).
        table_name: Desired DuckDB table name.
        primary_keys: Optional list of primary key column names.

    Returns:
        str: SQL CREATE TABLE statement.
    """
    lines: List[str] = []

    for name, field in model.model_fields.items():
        annotation = field.annotation
        duckdb_type = duckdb_type_from_annotation(annotation)

        # Static default support (including Enums)
        default_clause = ""
        if field.default is not PydanticUndefined:
            val = field.default
            if val is None:
                default_clause = "DEFAULT NULL"
            elif isinstance(val, Enum):
                default_clause = f"DEFAULT '{val.value}'"
            elif isinstance(val, str):
                default_clause = f"DEFAULT '{val}'"
            elif isinstance(val, bool):
                default_clause = f"DEFAULT {'TRUE' if val else 'FALSE'}"
            else:
                default_clause = f"DEFAULT {val}"

        lines.append(f"{name} {duckdb_type} {default_clause}".strip())

    pk_clause = f", PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    field_block = ",\n    ".join(lines)

    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        f"    {field_block}"
        f"{pk_clause}\n);"
    )


# === Internals (private helpers) ===============================================


def _to_values(
    item: Optional[Union[str, E, Sequence[Union[str, E]]]],
    enum_cls: Optional[Type[E]] = None,
    *,
    lower: bool = True,
) -> List[str]:
    """
    Normalize an enum/string or a sequence of them into a list of string values.
    If enum_cls is provided, enum members become their .value; strings are optionally lowered.
    """
    if item is None:
        return []
    seq: Iterable[Union[str, E]]
    if isinstance(item, str) or (enum_cls and isinstance(item, enum_cls)):
        seq = [item]
    else:
        seq = item  # assume sequence

    out: List[str] = []
    for x in seq:
        if enum_cls and isinstance(x, enum_cls):
            out.append(getattr(x, "value"))
        else:
            s = str(x)
            out.append(s.lower() if lower else s)
    return out


def _coerce_cell(x: Any) -> Any:
    """
    Coerce nested / special Python objects to DuckDB-friendly scalars.

    - dict/list → JSON string
    - BaseModel/HttpUrl → str
    - passthrough otherwise
    """
    if isinstance(x, (dict, list)):
        return json.dumps(x, ensure_ascii=False)
    if isinstance(x, (BaseModel, HttpUrl)):
        return str(x)
    return x


def finalize_one_row_in_pipeline_control(
    url: str,
    iteration: int,
    *,
    worker_id: str,
    ok: bool,
    notes: str | None = None,
) -> bool:
    """
    Lease-aware finalize for a single (url, iteration).

    - Only the current holder (matching worker_id) can finalize.
    - Clears the lease fields.
    - Sets status to COMPLETED or ERROR.
    - Optionally appends notes.

    Returns:
        True if the row was finalized (worker_id matched and UPDATE succeeded), else False.
    """
    if notes:
        con = get_db_connection()
        try:
            con.execute(
                "UPDATE pipeline_control SET notes = COALESCE(?, notes), updated_at = CURRENT_TIMESTAMP "
                "WHERE url = ? AND iteration = ?",
                [notes, url, iteration],
            )
        finally:
            con.close()

    final_status = PipelineStatus.COMPLETED if ok else PipelineStatus.ERROR
    return release_one(
        url=url,
        iteration=iteration,
        worker_id=worker_id,
        final_status=final_status,
    )


# === Public export surface (kept in section order) =============================

__all__ = [
    # Worklist
    "generate_worker_id",
    "get_claimable_worklist",
    "try_claim_one",
    "renew_lease",
    "release_one",
    # Discovery
    "get_urls_from_pipeline_control",
    "get_urls_by_stage_and_status",
    "get_urls_by_status",
    "get_urls_by_stage",
    "get_urls_ready_for_transition",
    # Lookups
    "get_pipeline_state",
    "get_current_stage_for_url",
    # Reporting
    "get_stage_progress_counts",
    "get_recent_urls",
    # Schema utils
    "align_df_with_schema",
    # DDL
    "duckdb_type_from_annotation",
    "generate_table_schema_from_model",
]
