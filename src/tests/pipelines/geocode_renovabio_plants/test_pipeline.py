"""
Test file for pipeline 'geocode_renovabio_plants'
generated using Kedro 0.18.12.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""

import pytest
import pandas as pd
from biofuel_brazil_plants.pipelines.geocode_renovabio_plants.nodes import (
    generate_plants_adress_dimension,
    generate_address_lists,
    get_latlong_from_url,
    selenium_setup,
    selenium_geocode,
)


@pytest.fixture
def renovabio_plants() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "CNPJ": [
                "00738822000255",
                "00738822000255",
                "28620879000193",
                "28620879000193",
            ],
            "RAZAO_SOCIAL": [
                "SANTA CRUZ ACUCAR E ALCOOL LTDA. ",
                "SANTA CRUZ ACUCAR E ALCOOL LTDA. ",
                "IMPACTO BIOENERGIA ALAGOAS S.A. ",
                "IMPACTO BIOENERGIA ALAGOAS S.A. ",
            ],
            "DS_END": [
                "FAZENDA SANTA CLARA ",
                "FAZENDA SANTA CLARA ",
                "FAZENDA SAO MATHEUS",
                "FAZENDA SAO MATHEUS",
            ],
            "NO_END": ["SN", "SN", "SN", "SN"],
            "CEP": ["45807000", "45807000", "57265000", "57265000"],
            "CIDADE": [
                " SANTA CRUZ CABRALIA ",
                " SANTA CRUZ CABRALIA ",
                " TEOTONIO VILELA ",
                " TEOTONIO VILELA ",
            ],
            "UF": [" BA", " BA", " AL", " AL"],
        }
    )


@pytest.fixture
def user_agent():
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0"  # noqa


@pytest.fixture
def geckodriver_path():
    return "conf/local/geckodriver.exe"


@pytest.fixture
def log_path():
    return "logs/geckodriver.log"


def test_generate_address_dimension(renovabio_plants):
    expected = pd.DataFrame(
        {
            "CNPJ": ["00738822000255", "28620879000193"],
            "RAZAO_SOCIAL": [
                "SANTA CRUZ ACUCAR E ALCOOL LTDA. ",
                "IMPACTO BIOENERGIA ALAGOAS S.A. ",
            ],
            "DS_END": ["FAZENDA SANTA CLARA ", "FAZENDA SAO MATHEUS"],
            "NO_END": ["SN", "SN"],
            "CEP": ["45807000", "57265000"],
            "CIDADE": [" SANTA CRUZ CABRALIA ", " TEOTONIO VILELA "],
            "UF": [" BA", " AL"],
        }
    )
    actual = generate_plants_adress_dimension(renovabio_plants)

    assert expected.equals(actual)


def test_generate_address_lists(renovabio_plants):

    expected = (
        [
            "SANTA%20CRUZ%20ACUCAR%20E%20ALCOOL%20LTDA.%20SANTA%20CRUZ%20CABRALIA%2045807000%20BA",  # noqa
            "IMPACTO%20BIOENERGIA%20ALAGOAS%20S.A.%20TEOTONIO%20VILELA%2057265000%20AL",
        ],
        [
            "USINA%20SANTA%20CLARA%20SN%20SANTA%20CRUZ%20CABRALIA%20BA%2045807000",
            "USINA%20SAO%20MATHEUS%20SN%20TEOTONIO%20VILELA%20AL%2057265000",
        ],
    )

    dm_plant_address = generate_plants_adress_dimension(renovabio_plants)
    actual = generate_address_lists(dm_plant_address)

    assert expected == actual


def test_latlong_from_url():
    url_list = [
        "https://www.google.com.br/maps/place/@-22.970451,-43.2094055,14.78",
        "https://www.google.com.br/maps/place/@-2.970451,-43.202,14.78",
        "https://www.google.com.br/maps/place/@-22.970451,-3.202,14.78",
        "https://www.google.com.br/maps/place/@-2.970,-3.202,14.78",
    ]

    expected = [
        (-22.970451, -43.2094055),
        (-2.970451, -43.202),
        (-22.970451, -3.202),
        (-2.97, -3.202),
    ]

    actual = [get_latlong_from_url(url) for url in url_list]

    assert expected == actual


@pytest.mark.skip(reason="Test requires firefox and geckodriver to run")
def test_selenium_geocode(renovabio_plants, user_agent, geckodriver_path, log_path):

    expected = pd.DataFrame(
        {
            "LATITUDE_google": [-16.1830272, -9.9138851],
            "LONGITUDE_google": [-39.3347587, -36.3326883],
        }
    )

    dm_plant_adress = generate_plants_adress_dimension(renovabio_plants)
    address_list_v1, address_list_v2 = generate_address_lists(dm_plant_adress)
    mozilla_service, mozilla_options = selenium_setup(
        user_agent, geckodriver_path, log_path
    )

    actual = selenium_geocode(
        mozilla_service,
        mozilla_options,
        address_list_v1,
        address_list_v2,
        6,
        5.5,
        gis="google",
    )

    assert expected.equals(actual)
