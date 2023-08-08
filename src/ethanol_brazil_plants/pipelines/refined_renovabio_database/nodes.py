"""
Pipeline 'refined_renovabio_database'
generated using Kedro 0.18.12
"""

import pandas as pd
import numpy as np
from ethanol_brazil_plants.utils import utils
from kedro.pipeline import node


def normalize_renovabio_database(
    raw_df: pd.DataFrame,
    object_cols: list[str],
    int_cols: list[str],
    float_cols: list[str],
    date_cols: list[str],
) -> pd.DataFrame:
    """
    Normalize renovabio database columns
    """

    normalized_df = utils.normalize_header(raw_df)
    normalized_df = normalized_df.fillna(method="ffill")

    normalized_df = utils.normalize_object_columns(normalized_df, object_cols)
    normalized_df = normalized_df[normalized_df["CNPJ"] != ""]

    normalized_df = utils.normalize_int_columns(normalized_df, int_cols)
    normalized_df = utils.normalize_float_columns(normalized_df, float_cols)

    normalized_df = utils.normalize_date_columns(normalized_df, date_cols)

    return normalized_df


def split_razao_cidade_uf_column(normalized_df: pd.DataFrame) -> pd.DataFrame:
    """
    Split RAZAO_SOCIAL-CIDADE-UF column into RAZAO_SOCIAL, CIDADE and UF columns.
    """

    normalized_df["RAZAO_SOCIAL"] = (
        normalized_df["RAZAO_SOCIAL-CIDADE-UF"].str.split("-").str[0]
    )

    normalized_df[["CIDADE", "UF", "UF_2"]] = (
        normalized_df["RAZAO_SOCIAL-CIDADE-UF"]
        .str.split("-")
        .str[-2:]
        .str.join(",")
        .str.split(",", expand=True)
    )

    normalized_df["UF"] = np.where(
        normalized_df["UF"].isnull(),
        normalized_df["UF_2"],
        normalized_df["UF"],
    )

    normalized_df["CIDADE"] = np.where(
        normalized_df["RAZAO_SOCIAL"] != normalized_df["CIDADE"],
        normalized_df["CIDADE"],
        None,
    )

    normalized_df = normalized_df.drop(
        ["RAZAO_SOCIAL-CIDADE-UF", "UF_2"], axis=1
    )
    return normalized_df


def abbreviate_production_route(
    normalized_df: pd.DataFrame, ds_rota: dict, cd_rota: dict
) -> pd.DataFrame:
    """
    Abbreviate routes names and define codes for each one of them.
    Args:
        normalized_df: Normalized dataframe.
        ds_rota: Dictionary with routes names as keys and abbreviations as values.
        cd_rota: Dictionary with routes abbreviations as keys and codes as values.
    """

    normalized_df["DS_ROTA"] = normalized_df["ROTA"].replace(ds_rota)
    normalized_df["CD_ROTA"] = normalized_df["DS_ROTA"].replace(cd_rota)
    return normalized_df


def generate_zip_code(
    normalized_df: pd.DataFrame,
    address_column: str = "ENDERECO_EMISSOR_PRIMARIO",
) -> pd.Series:
    """
    Extract zip code (CEP) from address.
    Args:
        normalized_df: DataFrame with normalized data;
        address_column: Column with address.
    """

    return (
        normalized_df[address_column]
        .str.replace(".", "", regex=False)
        .str.replace(r"-(\s+)", "-", regex=True)
        .str.findall(r"\d{5}-\d{3}|\d{5}-\d{2}")
        .str[0]
        .str.replace("-", "", regex=False)
        .str.pad(8, side="right", fillchar="0")
    )


def generate_refined_address(
    normalized_df: pd.DataFrame,
    address_column: str = "ENDERECO_EMISSOR_PRIMARIO",
) -> pd.Series:
    """
    Generate a refined address column from address column.
    Args:
        normalized_df: Normalized DataFrame;
        address_column: Address column name.
    """
    return (
        normalized_df[address_column]
        .str.replace(r"S/N[\w\W]| NO ", "", regex=True)
        .str.replace(r"CEP[\W]", "", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(", ,", ",", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )


def extract_city_and_state(
    normalized_df: pd.DataFrame,
    refined_address_col: str = "DS_END",
) -> pd.DataFrame:
    """
    Extracts city (CIDADE) and state (UF) from a normalized address column.

    Args:
        normalized_df: DataFrame with a refined address column.
        refined_address_col: Name of refined address column.
    """

    normalized_df[["re_CIDADE", "re_UF"]] = normalized_df[
        refined_address_col
    ].str.extract(r"([A-Z\s]+)(?: - |/| / )([A-Z]{2})\b")

    for col in ["CIDADE", "UF"]:
        normalized_df[col] = normalized_df[col].fillna(
            normalized_df[f"re_{col}"]
        )
        normalized_df = normalized_df.drop(f"re_{col}", axis=1)

    return normalized_df


def split_address_column(
    normalized_df: pd.DataFrame,
    refined_address_column: str = "DS_END",
) -> pd.DataFrame:
    """
    Split address column into main address and number columns.
    Args:
        normalized_df: Normalized dataframe.
        refined_address_column: Column name of refined address column.
    """

    # Pattern 1 - Highway and Roadway
    normalized_df["p1_DS_END"] = normalized_df[
        refined_address_column
    ].str.extract(
        r"(^RO[\w\s]+|[\s\W][A-Z]{2}(?<!KM|LT|CP)[\s\W]\d{1,3}\s|^EST[\w\s]+|ESTRADA[\w\s]+)"
    )

    normalized_df["p1_NO_END"] = normalized_df[
        refined_address_column
    ].str.extract(r"(KM[\s\W]+\d+)")

    # Pattern 2 - Farm
    normalized_df[["p2_DS_END"]] = normalized_df[
        refined_address_column
    ].str.extract(r"(^FAZ[\w|\s]+)")

    normalized_df[["p2_NO_END"]] = "SN"

    # Pattern 3 - Street
    normalized_df[["p3_DS_END", "p3_NO_END"]] = normalized_df[
        refined_address_column
    ].str.extract(r"(R[\s][A-Z\s]+|RUA[\w\s]+)(?:, |[\W]|,)([\d]+)")

    # Pattern 4 - Avenue
    normalized_df["p4_DS_END"] = normalized_df[
        refined_address_column
    ].str.extract(r"(^AV[\w\s]+|^AV[\s\w]+)")

    normalized_df["p4_NO_END"] = normalized_df[
        refined_address_column
    ].str.extract(r"(?:^AV[\w\s]+|^AV[\s\w]+)(?:, |[\W]|,)([\d]+)")

    # Pattern 5 - Others
    normalized_df["p5_DS_END"] = (
        normalized_df[refined_address_column].str.split(",").str[0]
    )

    normalized_df["p5_NO_END"] = (
        normalized_df[refined_address_column].str.split(",").str[1]
    )

    # Addresses
    address_cols = [refined_address_column, "NO_END"]
    for col in address_cols:
        normalized_df[f"{col}"] = (
            normalized_df[f"p1_{col}"]
            .fillna(normalized_df[f"p2_{col}"])
            .fillna(normalized_df[f"p3_{col}"])
            .fillna(normalized_df[f"p4_{col}"])
            .fillna(normalized_df[f"p5_{col}"])
        )
        normalized_df = normalized_df.drop(
            [f"p1_{col}", f"p2_{col}", f"p3_{col}", f"p4_{col}", f"p5_{col}"],
            axis=1,
        )

    return normalized_df


def rename_and_reorder_column(
    df: pd.DataFrame,
    dict_rename_cols: dict,
    ordered_cols: list,
) -> pd.DataFrame:
    """
    Rename and reorder columns of DataFrame.
    Args:
        df: DataFrame to be renamed and reordered.
        dict_rename_cols: Dictionary with old and new column names.
        ordered_cols: List with ordered columns.
    """

    df = df.rename(dict_rename_cols, axis=1)
    df = df[ordered_cols].copy()
    return df


def generate_refined_renovabio_database(
    raw_renovabio_database: pd.DataFrame,
    object_cols: list,
    int_cols: list,
    float_cols: list,
    date_cols: list,
    ds_rota: dict,
    cd_rota: dict,
    dict_rename_cols: dict,
    ordered_cols: list,
) -> pd.DataFrame:
    """
    Generate a refined Renovabio database.
    """

    # Normalize
    normalized_database = normalize_renovabio_database(
        raw_renovabio_database,
        object_cols,
        int_cols,
        float_cols,
        date_cols,
    )

    # Business name, City and State
    normalized_database = split_razao_cidade_uf_column(normalized_database)

    # Route
    normalized_database = abbreviate_production_route(
        normalized_database, ds_rota, cd_rota
    )

    # Zip code
    normalized_database["CEP"] = generate_zip_code(normalized_database)
    # City and State
    normalized_database["DS_END"] = generate_refined_address(
        normalized_database
    )
    normalized_database = extract_city_and_state(normalized_database)
    # Address
    normalized_database = split_address_column(normalized_database)

    # Rename and reorder columns
    normalized_database = rename_and_reorder_column(
        normalized_database, dict_rename_cols, ordered_cols
    )

    return normalized_database


refined_renovabio_plants = node(
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
)
