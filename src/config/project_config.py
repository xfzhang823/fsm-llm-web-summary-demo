"""
config/project_config.py

Minimal centralized config for this demo:

- Project root detection
- DuckDB / data directory locations
- LLM provider + model IDs
- YAML config paths for DB loaders / inserters
"""

from __future__ import annotations

from pathlib import Path
import logging

logger = logging.getLogger(__name__)

DEFAULT_MARKER = ".git"

# -----------------------------------------------------------------------------
# Project root detection
# -----------------------------------------------------------------------------


def find_project_root(
    starting_path: str | Path | None = None,
    marker: str = DEFAULT_MARKER,
) -> Path | None:
    """
    Find the project root by searching upward for a marker (e.g. '.git').

    Args:
        starting_path: Optional path to start from. Defaults to this file's dir.
        marker: File or directory name that marks the project root.

    Returns:
        Path to the project root, or None if not found.
    """
    if starting_path is None:
        starting_path = Path(__file__).resolve().parent
    else:
        starting_path = Path(starting_path).resolve()

    # Check starting directory and then walk up parents
    for candidate in (starting_path, *starting_path.parents):
        if (candidate / marker).exists():
            return candidate

    logger.error("Could not locate project root (marker %r not found).", marker)
    return None


# -----------------------------------------------------------------------------
# Base / data directories
# -----------------------------------------------------------------------------

BASE_DIR = find_project_root()
if not BASE_DIR or not BASE_DIR.exists():
    raise ValueError(
        f"Invalid project root directory (marker '{DEFAULT_MARKER}' not found)"
    )

# All structured pipeline data (DuckDB + exports) lives under here
PIPELINE_DATA_DIR = BASE_DIR / "pipeline_data"

# DuckDB location
DUCKDB_FILE = PIPELINE_DATA_DIR / "pipeline_data.duckdb"
URL_JSON_FILE = PIPELINE_DATA_DIR / "urls.json"


# -----------------------------------------------------------------------------
# LLM providers + model IDs (used by async LLM utilities)
# -----------------------------------------------------------------------------

# Provider identifiers
OPENAI = "openai"
ANTHROPIC = "anthropic"

# OpenAI chat models
GPT_35_TURBO = "gpt-3.5-turbo"  # legacy, kept for fallback / compat
GPT_4_1 = "gpt-4.1"  # main strong model
GPT_4_1_NANO = "gpt-4.1-nano"  # cheaper/faster default for many calls

# (Reasoning models if/when needed)
GPT_O3 = "o3"
GPT_O3_MINI = "o3-mini"
GPT_O3_PRO = "o3-pro"

# Embeddings (if you add RAG later)
EMBEDDING_3_SMALL = "text-embedding-3-small"
EMBEDDING_3_LARGE = "text-embedding-3-large"

# Anthropic (Claude) models
CLAUDE_HAIKU = "claude-3-haiku-20240307"
CLAUDE_SONNET_3_5 = "claude-3-5-sonnet-20241022"
CLAUDE_OPUS = "claude-3-opus-20240229"


# -----------------------------------------------------------------------------
# YAML config locations for DB loaders / inserters
# -----------------------------------------------------------------------------

# For this demo we still assume the Python package is `job_bot`,
# and config YAMLs live in job_bot/config/.
SRC_ROOT_DIR = BASE_DIR / "src"
DB_IO_DIR = SRC_ROOT_DIR / "db_io"
CONFIG_DIR = SRC_ROOT_DIR / "config"

DB_LOADERS_YAML = CONFIG_DIR / "db_loaders.yaml"
DB_INSERTERS_YAML = CONFIG_DIR / "db_inserters.yaml"
