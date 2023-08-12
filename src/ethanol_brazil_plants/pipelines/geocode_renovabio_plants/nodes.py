"""
Pipeline 'geocode_renovabio_plants'
generated using Kedro 0.18.12
"""

import re
import time
import pandas as pd
import selenium.webdriver as webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from random import random


def selenium_setup(
    user_agent: str,
    geckodriver_path: str,
    log_path: str,
) -> tuple:
    """
    Setup selenium webdriver for Firefox
    """

    mozilla_service = Service(geckodriver_path, log_output=log_path)
    mozilla_options = Options()
    mozilla_options.set_preference('general.useragent.override', user_agent)
    return mozilla_service, mozilla_options


def generate_address_lists(
    rf_renovabio_plants: pd.DataFrame,
) -> tuple:
    """
    Generate a list of addresses to be geocoded.
    """

    rf_renovabio_plants = rf_renovabio_plants.fillna('')

    address_list_v1 = (
        rf_renovabio_plants['DS_END'].str.replace('FAZENDA', 'USINA')
        .str.cat([
            rf_renovabio_plants['NO_END'],
            rf_renovabio_plants['CIDADE'],
            rf_renovabio_plants['UF'],
            rf_renovabio_plants['CEP']],
            sep=' '
        )
    ).tolist()

    address_list_v2 = (
        rf_renovabio_plants['CIDADE']
        .str.cat([
            rf_renovabio_plants['UF'],
            rf_renovabio_plants['CEP']],
            sep=' '
        )
    ).tolist()

    return address_list_v1, address_list_v2


def get_latlong_from_url(url: str) -> tuple:
    """
    Returns latitude and longitude from a Bing Maps URL
    """
    latlong = re.findall(r'-?\d{1,2}\.\d{3,}', url)
    latitude = float(latlong[0])
    longitude = float(latlong[1])
    return latitude, longitude


def selenium_geocode(
    mozilla_service,
    mozilla_options,
    address_list_v1: list,
    address_list_v2: list,
    first_iter_sleep_time: float,
    sleep_time: float,
    gis: str,
) -> pd.DataFrame:
    """
    Geocode addresses using Selenium and Bing or Google Maps.
    Args:
        mozilla_service: Selenium service for Firefox
        mozilla_options: Selenium options for Firefox
        address_list_v1: List of addresses to be geocoded
        address_list_v2: List of addresses to be geocoded in case version 1 fails
        first_iter_sleep_time: Sleep time for first iteration
        sleep_time: Sleep time for subsequent iterations
        gis: GIS (Maps) to be used. Choose between 'bing' or 'google'
    """
    # Launch browser
    browser = webdriver.Firefox(service=mozilla_service, options=mozilla_options)

    # Define GIS URL
    if gis == 'bing':
        gis_url = 'https://www.bing.com/maps?q={}'
        not_found_text = 'Não há resultados para:'  # PT-BR
    elif gis == 'google':
        gis_url = 'https://www.google.com.br/maps/search/{}'
        not_found_text = 'O Google Maps não encontrou'  # PT-BR
    else:
        raise ValueError('Invalid GIS. Please choose between "bing" or "google".')

    # Geocoding addresses
    latitude = []
    longitude = []

    first_iter = True
    for i in range(len(address_list_v1)):
        browser.get(gis_url.format(address_list_v1[i]))

        # Longer sleep time for first iteration (browser opens)
        if first_iter is True:
            time.sleep(first_iter_sleep_time)
            first_iter = False
        else:
            time.sleep(sleep_time + random())

        lat, long = get_latlong_from_url(browser.current_url)

        # If address is not found, try to geocode only the last 3 address columns
        ckeck_results = re.findall(not_found_text, browser.page_source)
        if len(ckeck_results) > 0:
            browser.get(gis_url.format(address_list_v2[i]))
            time.sleep(sleep_time)

            lat, long = get_latlong_from_url(browser.current_url)

        latitude.append(lat)
        longitude.append(long)

    browser.quit()

    df_latlong = pd.DataFrame({'LATITUDE': latitude, 'LONGITUDE': longitude})
    return df_latlong


def geocode_renovabio_plants(
    rf_renovabio_plants: pd.DataFrame,
    user_agent: str,
    geckodriver_path: str,
    log_path: str,
    first_iter_sleep_time: float,
    sleep_time: float,
    gis: str,
) -> pd.DataFrame:
    """
    Geocodes addresses using Selenium and Firefox
    """
    mozilla_service, mozilla_options = selenium_setup(
        user_agent, geckodriver_path, log_path
    )
    address_list_v1, address_list_v2 = generate_address_lists(rf_renovabio_plants)

    df_latlong = selenium_geocode(
        mozilla_service, mozilla_options,
        address_list_v1, address_list_v2,
        first_iter_sleep_time, sleep_time,
        gis
    )

    rf_renovabio_plants_geocoded = pd.concat([
        rf_renovabio_plants, df_latlong
    ], axis=1, ignore_index=False)

    return rf_renovabio_plants_geocoded
