"""
Pipeline 'geocode_renovabio_plants'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline
from .nodes import rf_renovabio_plants_geocoded


def geocode_renovabio_plants(**kwargs) -> Pipeline:
    return pipeline([rf_renovabio_plants_geocoded])
