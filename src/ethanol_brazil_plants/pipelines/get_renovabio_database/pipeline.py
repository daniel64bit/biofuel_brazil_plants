"""
Pipeline 'get_renovabio_database'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline
from .nodes import raw_renovabio_database


def get_renovabio_database(**kwargs) -> Pipeline:
    return pipeline([raw_renovabio_database])
