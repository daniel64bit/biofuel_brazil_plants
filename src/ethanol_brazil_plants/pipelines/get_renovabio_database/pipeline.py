"""
Pipeline 'get_renovabio_database'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import download_renovabio_database


def get_renovabio_database(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=download_renovabio_database,
                inputs={
                    "renovabio_url": "params:renovabio_url",
                    "save_path": "params:raw_renovabio_plants_path"},
                outputs=None,
                name="download_renovabio_database",
            )
        ]
    )
