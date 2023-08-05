"""Project pipelines."""
from __future__ import annotations

from kedro.pipeline import Pipeline
from .pipelines.get_renovabio_database.pipeline import get_renovabio_database


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines."""

    return {
        "__default__":
        get_renovabio_database()
    }
