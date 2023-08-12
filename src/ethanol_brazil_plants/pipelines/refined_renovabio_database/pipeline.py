"""
Pipeline 'refined_renovabio_database'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import generate_refined_renovabio_database


def generate_refined_renovabio_plants(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=generate_refined_renovabio_database,
                inputs={
                    "raw_renovabio_database": "raw_renovabio_plants_validos",
                    "object_cols": "params:object_cols",
                    "int_cols": "params:int_cols",
                    "float_cols": "params:float_cols",
                    "date_cols": "params:date_cols",
                    "ds_rota": "params:ds_rota",
                    "cd_rota": "params:cd_rota",
                    "dict_rename_cols": "params:dict_rename_cols",
                    "ordered_cols": "params:ordered_cols",
                },
                outputs="refined_renovabio_plants",
                name="generate_refined_renovabio_database",
            )
        ]
    )
