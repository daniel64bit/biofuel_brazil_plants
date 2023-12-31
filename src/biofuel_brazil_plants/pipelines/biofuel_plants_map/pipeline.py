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
                    "rf_renovabio_plants": "refined_renovabio_plants",
                    "rf_dm_plant_address": "refined_dm_plant_address",
                    "icon_path": "params:biomass_energy",
                },
                outputs='biofuel_plants_map',
                name="generate_biofuel_plants_map",
            ),
        ]
    )
