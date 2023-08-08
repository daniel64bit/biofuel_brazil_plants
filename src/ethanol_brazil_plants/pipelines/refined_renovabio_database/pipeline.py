"""
Pipeline 'refined_renovabio_database'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline
from .nodes import refined_renovabio_plants


def generate_refined_renovabio_plants(**kwargs) -> Pipeline:
    return pipeline([refined_renovabio_plants])
