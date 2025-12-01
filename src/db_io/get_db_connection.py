"""db_io/get_db_connection.py"""

from pathlib import Path
import logging
from typing import Optional
import duckdb
from job_bot.utils.find_project_root import find_project_root


logger = logging.getLogger(__name__)


def get_db_connection(
    db_path: Optional[Path | str] = None,
) -> duckdb.DuckDBPyConnection:
    """
    Create a connection to DuckDB.

    Args:
    db_path: Optional path to the DuckDB database. If not provided, a default path will be used.

    Returns:
    A DuckDBPyConnection object.
    """
    root_dir = find_project_root()
    if root_dir is None:
        raise FileNotFoundError("Could not locate the project root")

    if db_path is None:
        db_path = root_dir / "pipeline_data/db/pipeline_data.duckdb"
    elif isinstance(db_path, str):
        db_path = Path(db_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    return duckdb.connect(str(db_path))
