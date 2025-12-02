"""
db_io/db_loaders_config_models.py

Pydantic models and helpers for table loader configuration.
Defines schema for YAML-based loader settings (rehydrators, filters,
ordering, grouping) and provides a utility to import rehydration
functions by dotted path.
"""

from typing import Optional, List, Dict
import importlib
from pydantic import BaseModel, Field


class TableLoaderConfig(BaseModel):
    """Per-table loader settings (rehydrator, filters, ordering, grouping)."""

    rehydrate: Optional[str] = None
    filters: List[str] = Field(default_factory=list)
    order_by: List[str] = Field(default_factory=list)
    group_by_url: bool = False
    prefer_latest_only: bool = False


class LoaderDefaults(BaseModel):
    """Global defaults applied across all table loaders."""

    order_by: List[str] = Field(default_factory=list)
    group_by_url_key: str = "url"


class LoaderConfig(BaseModel):
    """Root configuration object combining defaults and per-table settings."""

    defaults: LoaderDefaults
    tables: Dict[str, TableLoaderConfig]


def import_callable(path: str):
    """Import a function from a dotted 'module:function' path string."""
    mod, fn = path.split(":")
    return getattr(importlib.import_module(mod), fn)
