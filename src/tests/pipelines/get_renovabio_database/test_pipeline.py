"""
Test file for pipeline 'get_renovabio_database'
generated using Kedro 0.18.12.
Please add your pipeline tests here.

Kedro recommends using `pytest` framework, more info about it can be found
in the official documentation:
https://docs.pytest.org/en/latest/getting-started.html
"""

import pytest
from biofuel_brazil_plants.pipelines.get_renovabio_database.nodes import (
    extract_renovabio_database_link,
    download_file,
)


@pytest.fixture
def renovabio_url() -> str:
    return "https://www.gov.br/anp/pt-br/assuntos/renovabio/certificados-producao-importacao-eficiente-biocombustiveis"  # noqa


def test_extract_link(renovabio_url):
    database_link = extract_renovabio_database_link(renovabio_url)
    assert "xlsx" in database_link


def test_download_file(renovabio_url):
    database_link = extract_renovabio_database_link(renovabio_url)
    file = download_file(database_link)
    assert "sheet" in file.decode("latin-1")
