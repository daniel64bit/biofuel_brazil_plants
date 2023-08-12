"""Project pipelines."""
from __future__ import annotations

from kedro.pipeline import Pipeline
from .pipelines.get_renovabio_database.pipeline import get_renovabio_database
from .pipelines.refined_renovabio_database.pipeline import \
    generate_refined_renovabio_plants
from .pipelines.geocode_renovabio_plants.pipeline import geocode_renovabio_plants_pipe


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines."""

    return {
        "__default__":
        get_renovabio_database() +
        generate_refined_renovabio_plants() +
        geocode_renovabio_plants_pipe(),
        'get_renovabio_database': get_renovabio_database(),
        'generate_refined_renovabio_plants': generate_refined_renovabio_plants(),
        'geocode_renovabio_plants': geocode_renovabio_plants_pipe()
    }
