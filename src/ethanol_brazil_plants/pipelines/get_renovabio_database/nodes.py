"""
Pipeline 'get_renovabio_database'
generated using Kedro 0.18.12
"""
import requests
from bs4 import BeautifulSoup
from kedro.pipeline import node


def download_file(url: str, save_path: str) -> None:
    """
    Download file file from url and save it to save_path.
    """

    try:
        response = requests.get(url)

        with open(save_path, 'wb') as file:
            file.write(response.content)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def extract_renovabio_database_link(renovabio_url: str) -> str:
    """
    Extracts renovabio database link.
    """

    try:
        response = requests.get(renovabio_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        found_link = soup.find_all('a', attrs={'class': 'internal-link'})
        found_link = found_link[0]['href']
        return found_link

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")
        return None


def download_renovabio_database(renovabio_url, save_path) -> None:
    """
    Download and save Renovabio plants database.
    """

    database_url = extract_renovabio_database_link(renovabio_url)
    download_file(database_url, save_path)
    return None


raw_renovabio_database = node(
    func=download_renovabio_database,
    inputs={
        "renovabio_url": "params:renovabio_url",
        "save_path": "params:raw_renovabio_plants_path"},
    outputs=None
)
