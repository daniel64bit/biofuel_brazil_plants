"""
Test file for pipeline 'biofuel_plants_map'
generated using Kedro 0.18.12.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""

import pandas as pd
import pytest
from biofuel_brazil_plants.pipelines.biofuel_plants_map.nodes import (
    merge_plants_with_adress,
)


@pytest.fixture
def renovabio_plants() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "CNPJ": ["00738822000255", "00738822000255"],
            "RAZAO_SOCIAL": [
                "SANTA CRUZ ACUCAR E ALCOOL LTDA. ",
                "SANTA CRUZ ACUCAR E ALCOOL LTDA. ",
            ],
            "BIOCOMBUSTIVEL": ["ETANOL HIDRATADO", "ETANOL ANIDRO"],
        }
    )


@pytest.fixture
def dm_plant_address() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "CNPJ": ["00738822000255", "28620879000193"],
            "LATITUDE_google": [-16.1830272, -9.9138851],
            "LONGITUDE_google": [-39.3347587, -36.3326883],
        }
    )


def test_merge_with_address(renovabio_plants, dm_plant_address):
    expected = pd.DataFrame(
        {
            "CNPJ": ["00738822000255", "00738822000255"],
            "RAZAO_SOCIAL": [
                "SANTA CRUZ ACUCAR E ALCOOL LTDA. ",
                "SANTA CRUZ ACUCAR E ALCOOL LTDA. ",
            ],
            "BIOCOMBUSTIVEL": ["ETANOL HIDRATADO", "ETANOL ANIDRO"],
            "LATITUDE_google": [-16.1830272, -16.1830272],
            "LONGITUDE_google": [-39.3347587, -39.3347587],
        }
    )

    actual = merge_plants_with_adress(renovabio_plants, dm_plant_address)

    assert expected.equals(actual)
