"""
db_io/db_loaders.py

Schema- and config-driven loaders for pulling structured content from DuckDB.

This module replaces static, per-table functions with a unified mechanism that
combines three sources of truth:

1. DUCKDB_SCHEMA_REGISTRY  →  authoritative table schemas (columns, PKs, metadata)
2. db_loaders.yaml         →  behavioral overlay (rehydrator, filters, grouping)
3. Pydantic row/file models →  validation and structured return types

Together these let the pipeline fetch, filter, and rehydrate rows without
hard-coding SQL or table-specific logic.

Key features:
- ✅ Config-driven: loader behavior (filters, grouping, rehydrator) lives in YAML
- ✅ Schema-aware: predicates, ordering, and projections derived from registry
- ✅ Pydantic-validated: all results round-trip cleanly through typed models
- ✅ Flexible: supports per-URL (FSM granularity), batch (QA/dashboards),
  or raw DataFrame access for analytics
- ✅ Extensible: add new tables by editing YAML + registry, no code changes

-------------------------------------------------------------
 Loader Entry (from YAML)                          | Behavior
-------------------------------------------------------------
 job_postings                                      | rehydrate → JobPostingsBatch
 extracted_requirements                            | rehydrate → ExtractedRequirementsBatch
 flattened_requirements                            | group_by_url → dict[url, Requirements]
 flattened_responsibilities                        | group_by_url → dict[url, Responsibilities]
 edited_responsibilities                           | prefer_latest_only → NestedResponsibilities
 similarity_metrics                                | raw DataFrame (validated separately)
-------------------------------------------------------------

>>> Example Usage:

    # Load flattened requirements for a specific URL
    reqs = load_table(TableName.FLATTENED_REQUIREMENTS, url="https://job/123")

    # Load edited responsibilities, prefer latest per-URL record
    edited = load_table(TableName.EDITED_RESPONSIBILITIES, url="https://job/123")

    # Load entire job_urls table as a validated batch model
    urls_batch = load_table(TableName.JOB_URLS)

    # Load similarity metrics as raw DataFrame
    df = load_table(TableName.SIMILARITY_METRICS, limit=100)
    print(df.head())

Usage is consistent across tables: the config + registry determine whether
results come back as a dict of models, a file model, or a DataFrame.
"""

# Std and 3rd party imports
from __future__ import annotations
import re
from typing import Any, Optional, Sequence, List, Dict, Union
import logging
import pandas as pd
from pydantic import BaseModel
import yaml

# User defined
from db_io.get_db_connection import get_db_connection
from db_io.db_schema_registry import DUCKDB_SCHEMA_REGISTRY
from fsm.pipeline_enums import TableName, PipelineStatus
from models.db_loaders_config_models import (
    LoaderConfig,
    TableLoaderConfig,
    import_callable,
)
from config.project_config import DB_LOADERS_YAML


logger = logging.getLogger(__name__)

_ORDER_HINTS = ["updated_at", "created_at", "iteration"]

# load config yaml once (or cache)
with open(DB_LOADERS_YAML, "r") as f:
    LOADER_CFG = LoaderConfig.model_validate(yaml.safe_load(f))
    logger.info(f"Loaded yaml config ({DB_LOADERS_YAML}) into config model.")

    # todo: debug; delete later
    logger.debug("LOADER tables: %s", list(LOADER_CFG.tables.keys()))
    logger.debug(
        "pipeline_control.filters: %s", LOADER_CFG.tables["pipeline_control"].filters
    )
    logger.debug(
        "pipeline_control.order_by: %s", LOADER_CFG.tables["pipeline_control"].order_by
    )


def load_table(
    table: TableName,
    *,
    order_by: Optional[Sequence[str]] = None,
    columns: str = "*",
    **filters: Any,
) -> Union[
    pd.DataFrame,
    BaseModel,
    Dict[str, BaseModel],
]:
    """
    Load and rehydrate rows from a DuckDB table using schema + config.

    This function builds a parameterized SQL query from the given filters,
    executes it against DuckDB, and returns either a DataFrame or a
    Pydantic model, depending on the table's loader configuration.

    Args:
        table: Target DuckDB table (from the TableName enum).
        order_by: Optional list of ORDER BY expressions. Falls back to
            per-table or global defaults if not provided.
        columns: Columns to select (default '*').
        **filters: Arbitrary keyword filters (url, iteration, version, etc.).
            Only columns allowed by the table config/registry are applied.

    Returns:
        - pandas.DataFrame if no rehydrator is configured
        - A Pydantic file model or dict of models if `rehydrate` is defined

    Notes:
        • Unknown filter keys are ignored with a warning.
        • `group_by_url` and `prefer_latest_only` behaviors are controlled
          by the YAML loader config for each table.
    """

    name = table.value
    tbl_loader_config: TableLoaderConfig = LOADER_CFG.tables.get(
        name, TableLoaderConfig()
    )
    dcfg = LOADER_CFG.defaults

    ob = list(order_by or tbl_loader_config.order_by or dcfg.order_by)

    raw = dict(filters)
    if "status" in raw and isinstance(raw["status"], PipelineStatus):
        raw["status"] = raw["status"].value

    allowed = _allowed_predicates(table, tbl_loader_config.filters)
    preds = {k: v for k, v in raw.items() if k in allowed and v is not None}
    unknown = set(raw) - allowed
    if unknown:
        logger.warning("Ignoring unknown predicates for %s: %s", name, sorted(unknown))

    df = _select_df(
        table,
        predicates=preds,
        order_by=ob,
        columns=columns,
    )

    # 1) No modeling configured → DataFrame
    if not tbl_loader_config.rehydrate:
        return df

    rehydrate_fn = import_callable(tbl_loader_config.rehydrate)
    # model_or_file = rehydrate_fn(df)

    # 2) If a single URL is requested → return one Model
    # (pick newest rows automatically via order_by)
    if "url" in preds and preds["url"]:
        # df already filtered to that URL; just rehydrate that slice
        return rehydrate_fn(df)

    # 3) Multi-URL call: group into dict[url, Model] if requested
    if tbl_loader_config.group_by_url:
        if df.empty:
            return {}
        key = dcfg.group_by_url_key
        return {str(u): rehydrate_fn(g) for u, g in df.groupby(key)}

    # 4) Otherwise return a DataFrame (keeps things simple; avoids *Batch*)
    return df


def _allowed_predicates(table: TableName, cfg_filters: Sequence[str]) -> set[str]:
    schema = DUCKDB_SCHEMA_REGISTRY[table]
    # union of PK + metadata from registry; intersect with config whitelist
    allowed = set(schema.primary_keys) | set(schema.metadata_fields)

    # todo: debug; delete later
    logger.debug(f"schema pks: {set(schema.primary_keys)}")
    logger.debug(f"schema meta fields: {set(schema.metadata_fields)}")
    logger.debug(f"allowed_fields: {allowed}")

    return allowed & set(cfg_filters)


def _clean_order_by(order_by: Optional[Sequence[str]], cols: set[str]) -> List[str]:
    """
    Keep only order terms whose base column exists in this table.
    Supports items like: 'updated_at DESC', 'iteration', 'col DESC NULLS LAST'.
    """
    cleaned: List[str] = []
    if not order_by:
        return cleaned
    for term in order_by:
        base = re.split(r"[ (]", term.strip(), maxsplit=1)[0]  # first token
        if base in cols:
            cleaned.append(term.strip())
    return cleaned


def _fallback_order_for(table: TableName, cols: set[str]) -> List[str]:
    schema = DUCKDB_SCHEMA_REGISTRY[table]
    # Prefer common recency hints, then PKs (minus url), then any *_key
    for pref in [
        [c for c in _ORDER_HINTS if c in cols],
        [c for c in getattr(schema, "primary_keys", []) if c in cols and c != "url"],
        [c for c in schema.column_order if c in cols and c.endswith("_key")],
    ]:
        if pref:
            # default to DESC NULLS LAST for recency-ish sorts
            return [f"{c} DESC NULLS LAST" for c in pref]
    return []


def _select_df(
    table: TableName,
    *,
    predicates: Dict[str, Any],
    order_by: Optional[Sequence[str]] = None,
    columns: str = "*",
) -> pd.DataFrame:
    """
    Execute a SQL script to query a table in DuckDB.

    Args:
        table: Target DuckDB table (from TableName enum).
        predicates: Dict of column filters (validated upstream).
        order_by: Sequence of ORDER BY clauses.
        columns: Projection (default '*').

    Returns:
        pandas.DataFrame with query results.

    Note: In SQL, predicates are the conditions that evaluate to
    TRUE / FALSE (or UNKNOWN with NULLs).
    """
    schema = DUCKDB_SCHEMA_REGISTRY[table]
    cols = set(schema.column_order)

    where_sql, params = [], []
    for col, val in predicates.items():
        if val is None:
            continue

        # Type guard: treat val as a multi-value container only if it’s a list/tuple/set
        if isinstance(val, (list, tuple, set)) and not isinstance(val, (str, bytes)):
            vals = list(val)
            if not vals:  # empty IN: make it false
                where_sql.append("1=0")
                continue
            placeholders = ", ".join(["?"] * len(vals))
            where_sql.append(f"{col} IN ({placeholders})")
            params.extend(vals)
        else:
            where_sql.append(f"{col} = ?")
            params.append(val)

    where = f"WHERE {' AND '.join(where_sql)}" if where_sql else ""

    chosen = _clean_order_by(order_by, cols) or _fallback_order_for(table, cols)
    order_sql = f" ORDER BY {', '.join(chosen)}" if chosen else ""

    sql = f"SELECT {columns} FROM {schema.table_name} {where}{order_sql}".strip()
    con = get_db_connection()
    try:
        return con.execute(sql, params).df()
    finally:
        con.close()
