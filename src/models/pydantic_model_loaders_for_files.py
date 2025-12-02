"""models/pydantic_model_loaders_for_files.py"""

# utils/pydantic_model_loaders_for_files.py

from __future__ import annotations

from pathlib import Path
import json
import logging
from typing import Type, TypeVar, Union

# from project
from pydantic import BaseModel, ValidationError
from models.url_file_models import UrlFile  # your URL seed file model

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


def _load_raw_json(file_path: Union[str, Path]) -> object:
    """
    Internal helper: read a JSON file and return the raw Python object.

    Raises:
        OSError, json.JSONDecodeError
    """
    path = Path(file_path)
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def load_model_from_json(
    file_path: Union[str, Path],
    model_cls: Type[T],
) -> T:
    """
    Generic helper to load & validate a Pydantic model from a JSON file.

    This is reusable for any future models you add.

    Args:
        file_path: Path to the JSON file.
        model_cls: Pydantic BaseModel subclass to validate against.

    Returns:
        An instance of `model_cls`.

    Raises:
        OSError, json.JSONDecodeError, ValidationError
    """
    raw = _load_raw_json(file_path)
    return model_cls.model_validate(raw)


def load_url_file_model(file_path: Union[str, Path]) -> UrlFile:
    """
    Load and validate the URL seed file as a UrlFile model.

    This is the one the URL ingestion pipeline uses.
    """
    try:
        model = load_model_from_json(file_path, UrlFile)
        logger.info(
            "✅ Loaded URL seed file from %s with %d entries",
            file_path,
            len(model.urls),
        )
        return model
    except (OSError, json.JSONDecodeError) as e:
        logger.error(
            "❌ Failed to read URL seed file %s: %s", file_path, e, exc_info=True
        )
        raise
    except ValidationError as e:
        logger.error(
            "❌ Validation error in URL seed file %s: %s", file_path, e, exc_info=True
        )
        raise
