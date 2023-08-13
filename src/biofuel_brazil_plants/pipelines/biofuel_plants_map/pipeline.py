"""
This is a boilerplate pipeline 'biofuel_plants_map'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import generate_biofuel_plants_map


def biofuel_plants_map_pipe(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=generate_biofuel_plants_map,
                inputs={
                    "rf_renovabio_plants_geocoded":
                    "refined_renovabio_plants_geocoded",
                    "biofuel_plants_map_path":
                    "params:biofuel_plants_map_path"
                },
                outputs=None,
                name="generate_biofuel_plants_map",
            ),
        ]
    )
