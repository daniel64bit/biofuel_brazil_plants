"""Project pipelines."""
from __future__ import annotations

from kedro.pipeline import Pipeline
from .pipelines.get_renovabio_database.pipeline import get_renovabio_database
from .pipelines.refined_renovabio_database.pipeline import \
    generate_refined_renovabio_plants


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines."""

    return {
        "__default__":
        get_renovabio_database() +
        generate_refined_renovabio_plants()
    }
