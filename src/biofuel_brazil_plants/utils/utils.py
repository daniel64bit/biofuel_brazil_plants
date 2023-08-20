import pandas as pd
import numpy as np
from xlrd.xldate import xldate_as_datetime


def normalize_header(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove unnamed columns and
    from normalize header of a pandas DataFrame.
    """

    unnamed_cols = [c for c in df.columns if "Unnamed" in c]
    df = df.drop(unnamed_cols, axis=1)

    df.columns = (
        df.columns.str.replace("[*()]", "", regex=True)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(" - ", "-")
        .str.replace(" ", "_")
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    return df


def normalize_object_columns(
    df: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    """
    Normalize columns of a dataframe.
    """
    for col in columns:
        df[col] = (
            df[col]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.upper()
        )

    return df


def normalize_int_columns(
    df: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    """
    Normalize columns to int type
    """
    for col in columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("[.,/-]", "", regex=True)
            .astype(np.int64)
        )
        if col == "CNPJ":
            df[col] = df[col].astype(str).str.zfill(14)

    return df


def normalize_float_columns(
    df: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    """
    Normalize columns to int type.
    Not working with negative numbers.
    """
    for col in columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.encode("ascii", "ignore")
            .str.decode("ascii")
            .str.replace("[/-]", "", regex=True)
            .str.replace("[,]", ".", regex=True)
            .astype(np.float64)
        )
    return df


def xldate_as_datetime_coerce(xldate, datemode):
    """
    Convert Excel date into Python datetime.datetime object
    ignoring errors.
    """
    try:
        return xldate_as_datetime(xldate, datemode)
    except Exception:
        return np.datetime64("NAT")


def normalize_date_columns(
    df: pd.DataFrame, date_cols: list[str]
) -> pd.DataFrame:
    """
    Normalize columns to datetime type,
    dealing with different formats.
    """
    for col in date_cols:
        # Excel format (1900 based)
        if df[col].dtype != "datetime64[ns]":
            is_xldt = df[col].apply(lambda x: np.issubdtype(type(x), int))
            df[f"xldt_{col}"] = np.where(is_xldt, df[col], np.nan)
            df[f"xldt_{col}"] = df[f"xldt_{col}"].apply(
                lambda x: xldate_as_datetime_coerce(x, 0)
            )
        else:
            df[f"xldt_{col}"] = np.datetime64("NAT")

        # Format 1
        df[f"f1_{col}"] = pd.to_datetime(
            df[col], format="%Y-%m-%d %H:%M:%S", errors="coerce"
        )

        # Format 2
        df[f"f2_{col}"] = pd.to_datetime(
            df[col].astype(str).str.replace("[/*]", "", regex=True),
            format="%d%m%Y",
            errors="coerce",
        )

        df[col] = (
            df[f"f1_{col}"].fillna(df[f"f2_{col}"]).fillna(df[f"xldt_{col}"])
        )

        df = df.drop([f"xldt_{col}", f"f1_{col}", f"f2_{col}"], axis=1)
    return df


def save_html(
    html_content: str,
    save_path: str
) -> None:
    """
    Save html content to a file.
    """

    with open(save_path, "w") as f:
        f.write(html_content)

    return None
