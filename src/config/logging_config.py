"""
Centralized logging setup for job_bot.

Usage:
    from job_bot.logger import init_logging, log_and_flush
    init_logging()  # call once at app startup

This module:
- Finds the project root by walking up to a marker (default: ".git").
- Creates a per-run log file: logs/<username>_<YYYY-MM-DD_HH-MM-SS>_app.log
- Installs a RotatingFileHandler (100MB x 5 backups) + a console StreamHandler.
- Prevents duplicate handlers if init_logging() is called multiple times.
- Keeps module loggers handler-free and propagating to the root logger.
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path
import getpass
from datetime import datetime
from typing import Optional


DEFAULT_MARKER = ".git"
DEFAULT_LOG_SUBDIR = "logs"


def find_project_root(
    starting_path: Optional[Path] = None, marker: str = DEFAULT_MARKER
) -> Optional[Path]:
    """
    Recursively search upward from starting_path (or this file) for a directory
    containing `marker` (e.g., ".git"). Returns the found directory or None.
    """
    if starting_path is None:
        starting_path = Path(__file__).resolve().parent
    p = Path(starting_path)
    for candidate in (p, *p.parents):
        if (candidate / marker).exists():
            return candidate
    return None


def get_log_file_path(logs_dir: Path) -> Path:
    """
    Construct a log file path with username and timestamp under `logs_dir`.
    """
    username = getpass.getuser()
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return logs_dir / f"{username}_{timestamp}_app.log"


def _has_stream_handler(logger: logging.Logger) -> bool:
    return any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


def _has_rotating_file_handler_for(logger: logging.Logger, path: Path) -> bool:
    target = str(path)
    for h in logger.handlers:
        if isinstance(h, logging.handlers.RotatingFileHandler):
            try:
                # RotatingFileHandler has .baseFilename (str)
                if getattr(h, "baseFilename", None) == target:
                    return True
            except Exception:
                # Be conservative: do not treat as a match
                pass
    return False


def init_logging(
    *,
    root_level: int = logging.DEBUG,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
    marker: str = DEFAULT_MARKER,
    logs_subdir: str = DEFAULT_LOG_SUBDIR,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: Optional[str] = None,
) -> None:
    """
    Initialize application-wide logging ONCE (idempotent).

    - Finds project root using `marker`.
    - Ensures `<root>/<logs_subdir>` exists.
    - Adds a RotatingFileHandler (UTF-8, 100MB x 5) and a console StreamHandler.
    - Avoids duplicate handlers if called multiple times.
    - Sets `job_bot` logger to propagate to root and not attach its own handlers.
    """
    root_logger = logging.getLogger()
    if getattr(root_logger, "_job_bot_inited", False):
        # Already initialized; nothing to do.
        return

    # Resolve project root
    project_root = find_project_root(marker=marker)
    if project_root is None:
        raise RuntimeError(
            f"Project root not found. Ensure marker '{marker}' exists in a parent directory."
        )

    # Prepare logs directory and log file path
    logs_dir = project_root / logs_subdir
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = get_log_file_path(logs_dir)

    # Configure root logger level
    root_logger.setLevel(root_level)

    # Create handlers
    file_handler = logging.handlers.RotatingFileHandler(
        str(log_file_path),
        maxBytes=100 * 1024 * 1024,  # 100 MB
        backupCount=5,
        encoding="utf-8",
        delay=True,  # file is opened on first emit
    )
    file_handler.setLevel(file_level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)

    # Formatters
    formatter = logging.Formatter(log_format, date_format)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Attach handlers if not already present
    if not _has_rotating_file_handler_for(root_logger, log_file_path):
        root_logger.addHandler(file_handler)
    if not _has_stream_handler(root_logger):
        root_logger.addHandler(console_handler)

    # Ensure child loggers rely on root handlers only
    jb = logging.getLogger("job_bot")
    jb.handlers = []  # no per-module handlers
    jb.propagate = True

    # Mark as initialized and announce destination
    root_logger._job_bot_inited = True  # type: ignore[attr-defined]
    logging.getLogger(__name__).info("Logs → %s", log_file_path)


def log_and_flush(message: str, level: str = "info") -> None:
    """
    Log a message and immediately flush all root handlers.

    Args:
        message: The log message.
        level: 'debug' | 'info' | 'warning' | 'error' | 'critical'
    """
    log_func = getattr(logging, level.lower(), logging.info)
    log_func(message)
    for handler in logging.getLogger().handlers:
        try:
            handler.flush()
        except Exception:
            # Don't crash logging; move on to the next handler
            pass


__all__ = [
    "init_logging",
    "find_project_root",
    "get_log_file_path",
    "log_and_flush",
]

# --- auto-init ---
try:
    init_logging()
except Exception as e:
    import logging

    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__).warning(
        "⚠️ Logging auto-init failed, using basicConfig: %s", e
    )
