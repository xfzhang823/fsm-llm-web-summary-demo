import logging
import duckdb
from utils.find_project_root import find_project_root

logger = logging.getLogger(__name__)


# Define your persistent DB path
root_dir = find_project_root()
if root_dir is None:
    raise FileExistsError()

db_dir = root_dir / "pipeline_data/db"
db_dir.mkdir(parents=True, exist_ok=True)  # ensure directory exists

db_path = db_dir / "pipeline_data.duckdb"
con = duckdb.connect(str(db_path))  # this creates the file if not present

logger.info(f"✅ DuckDB connected at: {db_path}")
