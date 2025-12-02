# db_io/db_table_registry.py

"""
Authoritative registry for all DuckDB tables used in the *web summary demo*.

This module centralizes:
- ✅ Table names (`TableName` enum) for structured reference
- ✅ Pydantic models (`BaseModel` subclasses) defining schema/validation
- ✅ Primary key definitions per table
- ✅ Explicit per-table column order (authoritative; no auto-derivation)
- ✅ Helper methods to align, deduplicate, and check DataFrame content

By using this registry, the pipeline keeps database shape and validation code in sync,
and guarantees stable INSERT/SELECT ordering across all stages.

---
🧩 COMPONENT RELATIONSHIPS

    +------------------+         +------------------------+        +-------------------+
    |  pipeline_enums  |  --->   |  DUCKDB_SCHEMA_REGISTRY|  --->  |  TableSchema      |
    |   (TableName)    |         |  (central mapping)     |        |  (per table info) |
    +------------------+         +------------------------+        +-------------------+

Each entry in `DUCKDB_SCHEMA_REGISTRY` maps a `TableName` enum to a `TableSchema`.
`TableSchema` encapsulates the Pydantic model, the primary keys, and the explicit
column order to use for IO.

---
✅ USAGE EXAMPLES

>>> from db_io.db_table_registry import DUCKDB_SCHEMA_REGISTRY, TableName
>>> schema = DUCKDB_SCHEMA_REGISTRY[TableName.WEB_SUMMARY]
>>> schema_columns = DUCKDB_SCHEMA_REGISTRY[TableName.WEB_PAGE].column_order

# Align a DataFrame to the canonical order:
>>> df = schema.align_df(df)
>>> df = schema.drop_dupes(df)
>>> schema.log_pk_dupes(df, logger)

# Query by URL with a stable projection:
>>> sql = schema.select_by_url_sql(["url", "summary_text", "created_at"],
...                                order_by="created_at DESC")
>>> con.execute(sql, ("https://example.com",)).df()

This file is a **single source of truth** (SST) for DuckDB table behavior.
"""

from __future__ import annotations

import logging
from typing import Optional, Type

from pydantic import BaseModel

from fsm.pipeline_enums import TableName
from db_io.db_utils import align_df_with_schema
from models.db_table_models import (
    PipelineState,
    UrlRow,
    WebPageRow,
    WebSummaryRow,
)

logger = logging.getLogger(__name__)

# Which columns, if present, we treat as "standard metadata" in summaries / logs
STANDARD_METADATA_FIELDS = {
    "source_file",
    "iteration",
    "created_at",
    "updated_at",
    "llm_provider",
    "model_id",
    # FSM control
    "stage",
    "status",
    "task_state",
}

# --- 🔒 Explicit column order per table (authoritative) ---
# Keep these lists in the exact order you want them SELECT/INSERTed.
DUCKDB_COLUMN_ORDER = {
    TableName.PIPELINE_CONTROL.value: [
        "url",
        "iteration",
        "stage",
        "status",
        "task_state",
        "is_claimed",
        "worker_id",
        "lease_until",
        "source_file",
        "notes",
        "created_at",
        "updated_at",
    ],
    TableName.URL.value: [
        "url",
        "source",
        "tags",
        "created_at",
        "updated_at",
    ],
    TableName.WEB_PAGE.value: [
        "url",
        "iteration",
        "status_code",
        "html",
        "clean_text",
        "fetched_at",
        "fetch_error",
        "created_at",
        "updated_at",
    ],
    TableName.WEB_SUMMARY.value: [
        "url",
        "iteration",
        "llm_provider",
        "model_id",
        "summary_text",
        "summary_json",
        "tokens_prompt",
        "tokens_completion",
        "tokens_total",
        "created_at",
        "updated_at",
    ],
}


class TableSchema:
    """
    Encapsulates DuckDB table schema metadata and helper utilities.

    This class includes:
    - the Pydantic model (for validation elsewhere)
    - primary key definitions
    - explicit column order

    It also provides utilities for DataFrame alignment, deduplication,
    and logging of duplicate primary-key rows.
    """

    def __init__(
        self,
        *,
        model: Type[BaseModel],
        table_name: str,
        primary_keys: list[str],
        column_order: Optional[list[str]] = None,
    ):
        self.model = model
        self.table_name = table_name
        self.primary_keys = primary_keys

        # Enforce explicit order (no fallbacks to model). Either provided or from registry.
        if column_order is not None:
            final_cols = column_order
        else:
            # KeyError if missing is intentional: registry must define order.
            final_cols = DUCKDB_COLUMN_ORDER[table_name]
        self.column_order: list[str] = final_cols

    @property
    def metadata_fields(self) -> list[str]:
        return [c for c in self.column_order if c in STANDARD_METADATA_FIELDS]

    def align_df(self, df):
        """
        Align a DataFrame to this table's canonical column order.

        Raises:
            ValueError: If required columns are missing or extra columns exist (strict mode).
        """
        try:
            return align_df_with_schema(df, self.column_order, strict=True)
        except Exception as e:
            logger.error("❌ Align failed for '%s': %s", self.table_name, e)
            raise

    def drop_dupes(self, df):
        """Drop duplicate rows based on this table's primary key."""
        return df.drop_duplicates(subset=self.primary_keys)

    def log_pk_dupes(self, df, logger_):
        """
        Log any rows in `df` that violate the primary key uniqueness.
        """
        if not all(col in df.columns for col in self.primary_keys):
            logger_.warning(
                "Cannot check PK dupes — missing df fields: %s", self.primary_keys
            )
            return

        dupes = df[df.duplicated(subset=self.primary_keys, keep=False)]
        if not dupes.empty:
            logger_.warning(
                "❗ %d duplicate rows by PK %s", len(dupes), self.primary_keys
            )
            try:
                logger_.warning(
                    "Sample duplicate keys:\n%s",
                    dupes[self.primary_keys].drop_duplicates().head(),
                )
            except Exception:
                pass

    def select_by_url_sql(
        self,
        cols: list[str],
        order_by: str | list[str] | None = None,
    ) -> str:
        """
        Build a parameterized SELECT to retrieve all rows for a given URL.

        Args:
            cols: Columns to project (must exist in this table).
            order_by: Optional ordering; can be:
                - a single column name: "col"
                - comma-separated columns: "col1, col2 DESC"
                - a list of columns: ["col1", "col2 DESC"]
            Each column must exist in this table; direction must be ASC or DESC if given.

        Returns:
            SQL string using `?` placeholder for the URL parameter.
        """
        # Validate projection columns
        unknown = [c for c in cols if c not in self.column_order]
        if unknown:
            raise ValueError(f"{self.table_name}: unknown columns requested: {unknown}")

        # Normalize and validate ORDER BY
        order_clause = ""
        if order_by:
            if isinstance(order_by, (list, tuple)):
                raw_items = list(order_by)
            else:
                # Split comma-separated string into items
                raw_items = [
                    tok.strip() for tok in str(order_by).split(",") if tok.strip()
                ]

            normalized_items: list[str] = []
            for item in raw_items:
                parts = item.split()
                col = parts[0]
                if col not in self.column_order:
                    raise ValueError(
                        f"{self.table_name}: unknown order_by column: {col}"
                    )
                # Optional direction
                direction = ""
                if len(parts) > 1:
                    dir_upper = parts[1].upper()
                    if dir_upper not in ("ASC", "DESC"):
                        raise ValueError(
                            f"{self.table_name}: invalid sort direction for {col}: {parts[1]}"
                        )
                    direction = f" {dir_upper}"

                normalized_items.append(f"{col}{direction}")

            if normalized_items:
                order_clause = " ORDER BY " + ", ".join(normalized_items)

        select_list = ", ".join(cols)
        return (
            f"SELECT {select_list} FROM {self.table_name} WHERE url = ?{order_clause}"
        )

    def summary(self) -> str:
        """
        Return a human-readable summary of this table's schema configuration.
        """
        return (
            f"📊 Table: {self.table_name}\n"
            f"🔑 Primary Keys: {', '.join(self.primary_keys)}\n"
            f"📋 Columns: {', '.join(self.column_order)}\n"
            f"🧩 Metadata Fields: {', '.join(self.metadata_fields)}"
        )

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.table_name} with PK {self.primary_keys}>"

    def __str__(self):
        return f"{self.table_name} table\nPrimary keys: {', '.join(self.primary_keys)}"


# ✅ Centralized registry (4 tables: URL, PIPELINE_CONTROL, WEB_PAGE, WEB_SUMMARY)
DUCKDB_SCHEMA_REGISTRY = {
    # FSM snapshot ONLY: one row per (url, iteration)
    TableName.PIPELINE_CONTROL: TableSchema(
        model=PipelineState,
        table_name=TableName.PIPELINE_CONTROL.value,
        primary_keys=["url", "iteration"],
        column_order=DUCKDB_COLUMN_ORDER[TableName.PIPELINE_CONTROL.value],
    ),
    # Seed list: NO iteration
    TableName.URL: TableSchema(
        model=UrlRow,
        table_name=TableName.URL.value,
        primary_keys=["url"],
        column_order=DUCKDB_COLUMN_ORDER[TableName.URL.value],
    ),
    # Raw + cleaned content (per pass)
    TableName.WEB_PAGE: TableSchema(
        model=WebPageRow,
        table_name=TableName.WEB_PAGE.value,
        primary_keys=["url", "iteration"],
        column_order=DUCKDB_COLUMN_ORDER[TableName.WEB_PAGE.value],
    ),
    # LLM-generated summaries (per pass; provider/model stamped in PK)
    TableName.WEB_SUMMARY: TableSchema(
        model=WebSummaryRow,
        table_name=TableName.WEB_SUMMARY.value,
        primary_keys=["url", "iteration", "llm_provider", "model_id"],
        column_order=DUCKDB_COLUMN_ORDER[TableName.WEB_SUMMARY.value],
    ),
}
