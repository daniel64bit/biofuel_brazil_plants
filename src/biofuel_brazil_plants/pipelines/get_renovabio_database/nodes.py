"""
Pipeline 'get_renovabio_database'
generated using Kedro 0.18.12
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup


def download_file(url: str) -> None:
    """
    Downloads a file from a given url.
    """

    try:
        response = requests.get(url)
        return response.content

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def extract_renovabio_database_link(renovabio_url: str) -> str:
    """
    Extracts renovabio database link.
    """

    try:
        response = requests.get(renovabio_url)
        soup = BeautifulSoup(response.text, "html.parser")

        found_link = soup.find_all("a", attrs={"class": "internal-link"})
        found_link = found_link[0]["href"]
        return found_link

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")
        return None


def download_renovabio_database(renovabio_url) -> None:
    """
    Download and save Renovabio plants database.
    """

    database_url = extract_renovabio_database_link(renovabio_url)
    binary_file = download_file(database_url)

    raw_validos = pd.read_excel(
        binary_file, sheet_name="Válidos", skiprows=1, skipfooter=10
    )
    raw_canc_susp = pd.read_excel(
        binary_file, sheet_name="Cancelados ou Suspensos", skiprows=1, skipfooter=4
    )
    raw_anulados = pd.read_excel(
        binary_file, sheet_name="Anulados", skiprows=1, skipfooter=9)

    return raw_validos, raw_canc_susp, raw_anulados
