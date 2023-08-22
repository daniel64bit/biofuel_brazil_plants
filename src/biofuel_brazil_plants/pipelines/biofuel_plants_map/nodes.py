"""
Pipeline 'biofuel_plants_map'
generated using Kedro 0.18.12
"""

import folium
import pandas as pd


def create_marker(
    lat: float,
    lon: float,
    content: str,
    icon_path: str
) -> folium.Marker:
    """
    Create a marker with a popup.
    """
    biofuelicon = folium.features.CustomIcon(
        icon_path, icon_size=(45, 45)
    )
    return folium.Marker(
        location=[lat, lon],
        popup=folium.Popup(content, max_width=1500),
        icon=biofuelicon,
    )


def generate_marker_content(
    business_name: str,
    address: str,
    address_no: str,
    city: str,
    state: str,
    zip_code: int,
    biofuel: str,
    route: str,
    volume: int,
    lat: float,
    long: float,
) -> folium.Html:
    return folium.Html(
        f"""
        <h6><strong>{business_name}</strong></h6>
        <strong>Adress: </strong>{address}, {address_no}, {city}, {state} - {zip_code}
        <br>
        <strong>Biofuel: </strong>{biofuel}<br>
        <strong>Volume: </strong>{volume}<br>
        <strong>Route: </strong>{route}<br>
        <a href="https://www.google.com/maps/search/?api=1&query={lat,long}">
        Open with Google </a>
        """,
        script=True,
    )


def merge_plants_with_adress(
    rf_renovabio_plants: pd.DataFrame,
    rf_dm_plant_address: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges the plants dataframe with addresses dimension
    """

    rf_dm_plant_address = rf_dm_plant_address[
        ['CNPJ', 'LATITUDE_google', 'LONGITUDE_google']
    ].copy()
    rf_renovabio_plants_geocoded = rf_renovabio_plants.merge(
        rf_dm_plant_address, on='CNPJ', how='left')

    return rf_renovabio_plants_geocoded


def create_biofuel_plants_map(
    rf_renovabio_plants_geocoded: pd.DataFrame,
    icon_path: str
) -> folium.Map:
    """
    Creates a map with all the biofuel plants in Brazil.
    """

    biofuel_plants_map = folium.Map(
        location=[-14.2350, -51.9253], zoom_start=4, tiles="OpenStreetMap"
    )

    for index, row in rf_renovabio_plants_geocoded.iterrows():
        marker_content = generate_marker_content(
            row["RAZAO_SOCIAL"],
            row["DS_END"],
            row["NO_END"],
            row["CIDADE"],
            row["UF"],
            row["CEP"],
            row["BIOCOMBUSTIVEL"],
            row["DS_ROTA"],
            int(row["LITROS_CBIO"]),
            row["LATITUDE_google"],
            row["LONGITUDE_google"],
        )
        marker = create_marker(
            row["LATITUDE_google"],
            row["LONGITUDE_google"],
            marker_content,
            icon_path
        )

        marker.add_to(biofuel_plants_map)

    return biofuel_plants_map


def add_satellite_tile_layer(map: folium.Map) -> folium.Map:
    """
    Add a satellite tile layer to the map.
    """
    tile = folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',  # noqa
        attr='Esri',
        name='Esri Satellite',
        overlay=False,
        control=True
    )
    tile.add_to(map)
    return map


def generate_biofuel_plants_map(
    rf_renovabio_plants: pd.DataFrame,
    rf_dm_plant_address: pd.DataFrame,
    icon_path: str,
    biofuel_plants_map_path: str
) -> None:
    """
    Generate a map with all the biofuel plants in Brazil.
    """

    rf_renovabio_plants_geocoded = merge_plants_with_adress(
        rf_renovabio_plants, rf_dm_plant_address
    )

    biofuel_plants_map = create_biofuel_plants_map(
        rf_renovabio_plants_geocoded, icon_path
    )

    biofuel_plants_map = add_satellite_tile_layer(biofuel_plants_map)

    biofuel_plants_map.save(biofuel_plants_map_path)

    return None
