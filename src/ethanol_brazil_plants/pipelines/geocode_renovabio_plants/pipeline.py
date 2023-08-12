"""
Pipeline 'geocode_renovabio_plants'
generated using Kedro 0.18.12
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import geocode_renovabio_plants


def geocode_renovabio_plants_pipe(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=geocode_renovabio_plants,
                inputs={
                    'rf_renovabio_plants': 'refined_renovabio_plants',
                    'user_agent': 'params:user_agent',
                    'geckodriver_path': 'params:geckodriver_path',
                    'log_path': 'params:log_path',
                    'first_iter_sleep_time': 'params:first_iter_sleep_time',
                    'sleep_time': 'params:bing_sleep_time',
                    'gis': 'params:bing_gis',
                },
                outputs='refined_renovabio_plants_geocoded',
                name='bing_geocode_renovabio_plants',
            ),
        ]
    )
