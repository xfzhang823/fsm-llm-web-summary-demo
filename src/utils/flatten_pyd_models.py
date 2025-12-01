import logging
from pathlib import Path
from typing import Dict, List, Union
from pydantic import ValidationError
from job_bot.models.resume_job_description_io_models import (
    NestedResponsibilities,
    Requirements,
)

# Set up logger for this module
logger = logging.getLogger(__name__)


def flatten_requirements_model(model: Requirements) -> List[Dict[str, str]]:
    """
    Flattens a validated Requirements model into a list of row-based dictionaries.

    Each row represents a single job requirement and includes:
        - url: The URL of the job posting
        - requirement_key: The hierarchical key (e.g. '1.down_to_earth.0')
        - requirement: The full text of the requirement

    Args:
        model (Requirements): A validated Pydantic Requirements model.

    Returns:
        List[Dict[str, str]]: A list of flattened job requirement rows.
    """
    try:
        flattened = [
            {"url": model.url, "requirement_key": key, "requirement": value}
            for key, value in model.requirements.items()
        ]
        logger.info(f"✅ Flattened {len(flattened)} requirements from: {model.url}")
        return flattened

    except ValidationError as e:
        logger.error(f"❌ Validation error while flattening requirements: {e}")
        raise

    except Exception as e:
        logger.exception(f"❌ Unexpected error flattening requirements: {e}")
        raise


def flatten_nested_responsibilities_model(
    model: NestedResponsibilities,
) -> List[Dict[str, str]]:
    """
    Flattens a validated NestedResponsibilities model into a list of row-based dictionaries.

    Each row includes:
        - url: The URL of the job posting
        - responsibility_key: The unique responsibility path
        - requirement_key: The requirement ID it is aligned with
        - optimized_text: The rewritten responsibility for that requirement

    Args:
        model (NestedResponsibilities): A validated NestedResponsibilities model.

    Returns:
        List[Dict[str, str]]: A list of flattened responsibility → requirement alignments.
    """
    try:
        rows = []
        for responsibility_key, match_obj in model.responsibilities.items():
            for (
                requirement_key,
                opt_text_obj,
            ) in match_obj.optimized_by_requirements.items():
                rows.append(
                    {
                        "url": str(model.url),
                        "responsibility_key": responsibility_key,
                        "requirement_key": requirement_key,
                        "optimized_text": opt_text_obj.optimized_text,
                    }
                )

        logger.info(
            f"✅ Flattened {len(rows)} responsibility-requirement rows from: {model.url}"
        )
        return rows

    except ValidationError as e:
        logger.error(f"❌ Validation error while flattening responsibilities: {e}")
        raise

    except Exception as e:
        logger.exception(f"❌ Unexpected error flattening responsibilities: {e}")
        raise
