"""
db_io/create_db_tables.py

Create DuckDB tables from DUCKDB_SCHEMA_REGISTRY using Pydantic models.
"""

from __future__ import annotations

import logging
from typing import Optional

# Project imports
from db_io.get_db_connection import get_db_connection
from db_io.db_schema_registry import DUCKDB_SCHEMA_REGISTRY
from db_io.db_utils import generate_table_schema_from_model
from fsm.pipeline_enums import TableName

logger = logging.getLogger(__name__)


def create_single_db_table(table_name: TableName | str) -> bool:
    """
    Create a single DuckDB table defined in DUCKDB_SCHEMA_REGISTRY.
    Returns True if successful, False otherwise.
    """
    table = _to_tablename(table_name)
    if table is None:
        logger.error("❌ %r is not a valid TableName.", table_name)
        return False

    schema = DUCKDB_SCHEMA_REGISTRY.get(table)
    if not schema:
        logger.error("⚠️ Table '%s' not found in registry.", table.value)
        return False

    ddl = generate_table_schema_from_model(
        model=schema.model,
        table_name=schema.table_name,
        primary_keys=schema.primary_keys,
    )

    con = get_db_connection()
    try:
        con.execute(ddl)
        logger.info("✅ Table '%s' created or confirmed.", table.value)
        return True
    except Exception as e:  # noqa: BLE001
        logger.error("❌ Error creating table '%s': %s", table.value, e, exc_info=True)
        return False
    finally:
        try:
            con.close()
        except Exception:
            pass


def create_all_db_tables() -> None:
    """
    Create all DuckDB tables listed in DUCKDB_SCHEMA_REGISTRY.
    """
    con = get_db_connection()
    try:
        for table, schema in DUCKDB_SCHEMA_REGISTRY.items():
            ddl = generate_table_schema_from_model(
                model=schema.model,
                table_name=schema.table_name,
                primary_keys=schema.primary_keys,
            )
            try:
                con.execute(ddl)
                logger.info("✅ Created or confirmed table '%s'.", table.value)
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "❌ Failed to create table '%s': %s", table.value, e, exc_info=True
                )
    finally:
        try:
            con.close()
        except Exception:
            pass


def _to_tablename(name: TableName | str) -> Optional[TableName]:
    if isinstance(name, TableName):
        return name
    try:
        return TableName(name)
    except ValueError:
        for t in TableName:
            if t.value == name:
                return t
        return None


def _ensure_decision_bits_and_views(
    tablename: TableName = TableName.PIPELINE_CONTROL,
    decision_col: str = "decision_flag",
    transition_col: str = "transition_flag",
) -> None:
    """
    Ensures decision tracking columns and associated views exist in the target table.

    This function performs a idempotent setup of decision tracking infrastructure:
    - Adds decision and transition flag columns if they don't exist
    - Applies constraint validation to ensure data integrity
    - Creates or replaces views for decision state monitoring and pipeline progress

    The decision system follows a state machine pattern:
    - 'undecided': No decision made yet (decision_flag IS NULL)
    - 'ready': Decision made but not finalized (decision_flag = 1, transition_flag = 0)
    - 'final': Step completed and finalized (transition_flag = 1)

    Args:
        table_name: Name of the table to modify. Defaults to "pipeline_control".
        decision_col:
            Name of the decision flag column. Defaults to "decision_flag".
            Represents whether a decision has been made (0=no, 1=yes, NULL=undecided).
        transition_col:
            Name of the transition flag column. Defaults to "transition_flag".
            Represents whether the step is finalized (0=active, 1=finalized).

    Raises:
        DatabaseError: If any SQL execution fails.
        ValueError: If the table doesn't exist.

    Notes:
        - Idempotent: Safe to run multiple times (uses IF NOT EXISTS, OR REPLACE)
        - Constraints: Ensures valid flag values and mutually exclusive states
        - Views created:
            * v_decision_state: Current decision state for each row
            * v_pipeline_progress: Aggregated progress metrics per URL

    Example:
        >>> _ensure_decision_bits_and_views()
        # Sets up default decision tracking on 'pipeline_control' table

        >>> _ensure_decision_bits_and_views(
        ...     table_name="custom_pipeline",
        ...     decision_col="approved_flag",
        ...     transition_col="completed_flag"
        ... )
        # Sets up decision tracking with custom column names
    """
    table_name: str = tablename.value

    con = get_db_connection()
    try:
        # Simple validation: check if table exists
        result = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            (table_name,),
        ).fetchone()

        if not result or result[0] == 0:
            raise ValueError(f"Table '{table_name}' does not exist")

        # --- Columns (safe to run multiple times) ---
        con.execute(
            f"""
            ALTER TABLE {table_name}
              ADD COLUMN IF NOT EXISTS {decision_col} TINYINT;
        """
        )
        con.execute(
            f"""
            ALTER TABLE {table_name}
              ADD COLUMN IF NOT EXISTS {transition_col} TINYINT DEFAULT 0;
        """
        )

        # --- Guardrails (DuckDB supports ADD CHECK) ---
        con.execute(
            f"""
            ALTER TABLE {table_name}
              ADD CHECK ({decision_col} IS NULL OR {decision_col} IN (0,1));
        """
        )
        con.execute(
            f"""
            ALTER TABLE {table_name}
              ADD CHECK ({transition_col} IN (0,1));
        """
        )
        con.execute(
            f"""
            ALTER TABLE {table_name}
              ADD CHECK (NOT ({decision_col} = 1 AND {transition_col} = 1));
        """
        )

        # --- Views (CREATE OR REPLACE) ---
        con.execute(
            f"""
            CREATE OR REPLACE VIEW v_decision_state AS
            SELECT
              url, stage, {decision_col}, {transition_col},
              CASE
                WHEN {decision_col} = 1 AND {transition_col} = 0 THEN 'ready'
                WHEN {transition_col} = 1 THEN 'final'
                ELSE 'undecided'
              END AS decision_state
            FROM {table_name};
        """
        )

        con.execute(
            f"""
            CREATE OR REPLACE VIEW v_pipeline_progress AS
            SELECT
              url,
              SUM({transition_col}) AS finalized_steps,
              SUM(CASE WHEN {decision_col}=1 THEN 1 ELSE 0 END) AS ready_now,
              COUNT(*) AS total_stages,
              (SUM({transition_col}) = COUNT(*)) AS is_final
            FROM {table_name}
            GROUP BY url;
        """
        )
    finally:
        con.close()
