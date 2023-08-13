"""Project pipelines."""
from __future__ import annotations

from kedro.pipeline import Pipeline
from .pipelines.get_renovabio_database.pipeline import get_renovabio_database
from .pipelines.refined_renovabio_database.pipeline import (
    generate_refined_renovabio_plants,
)
from .pipelines.geocode_renovabio_plants.pipeline import (
    geocode_renovabio_plants_pipe,
)
from .pipelines.biofuel_plants_map.pipeline import biofuel_plants_map_pipe


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines."""

    return {
        "__default__": get_renovabio_database()
        + generate_refined_renovabio_plants()
        + geocode_renovabio_plants_pipe()
        + biofuel_plants_map_pipe(),
        "get_renovabio_database": get_renovabio_database(),
        "generate_refined_renovabio_plants": generate_refined_renovabio_plants(),
        "geocode_renovabio_plants": geocode_renovabio_plants_pipe(),
        "biofuel_plants_map": biofuel_plants_map_pipe(),
        "get_and_refine_renovabio_database": get_renovabio_database()
        + generate_refined_renovabio_plants(),
    }
