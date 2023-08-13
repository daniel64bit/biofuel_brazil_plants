"""
This is a boilerplate pipeline 'biofuel_plants_map'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import generate_biofuel_plants_map
from utils import utils


def biofuel_plants_map_pipe(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=generate_biofuel_plants_map,
                inputs={
                    "rf_renovabio_plants_geocoded":
                    "refined_renovabio_plants_geocoded"
                },
                outputs="biofuel_plants_map",
                name="generate_biofuel_plants_map",
            ),
            node(
                func=utils.save_html,
                inputs={
                    "html_content": "biofuel_plants_map",
                    "save_path": "params:biofuel_plants_map_path"
                },
                outputs=None,
                name="save_biofuel_plants_map",
            )
        ]
    )
