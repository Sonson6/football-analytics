"""
Created on Sat Feb 18 13:54:51 2023

@author: nelso
"""

from datetime import date, datetime
from typing import Any, Union

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


def check_date_format(wk_date: str) -> None:
    """Check format of working dates.

    Args:
        wk_date (str): working date to check.

    Raises:
        ValueError
    """

    if len(wk_date) < 8:
        raise ValueError(f"{wk_date} should be in YYYYMMDD format. Please correct")


def treat_cli_params(start_date: str, end_date: Union[str, None]) -> Any:
    """Convert date as string to datetime format.

    Args:
        start_date (str): start date to scrap.
        end_date (str): end date to scrap.

    Raises:
        ValueError

    Returns:
        _type_: start_date and end_date as datetime.
    """

    check_date_format(start_date)
    start_date_dt = datetime.strptime(start_date, "%Y%m%d")

    if end_date:
        check_date_format(end_date)
        end_date_dt = datetime.strptime(end_date, "%Y%m%d")
    else:
        end_date_dt = start_date_dt

    if end_date_dt < start_date_dt:
        raise ValueError(f"{start_date_dt} should be earlier than {end_date_dt}, please correct.")

    return start_date_dt, end_date_dt


def create_metadata(wk_date: str, root_url: str, db_config: Union[None, dict[str, Any]] = None) -> dict[str, Any]:
    """Create the required dictionary to prepare scraping settings.

    Args:
        wk_date (str): Game on which the football game statistics scraping will occur.
        root_url (str): Link to the FBRef website (to disappear soon)
        db_config (Union[None, dict[str, Any]]): database configurations. Defaults to None.

    Returns:
        dict[str, Any]: Date based metadata with key configurations, among which PostGreSQL database
            information if requested.

    For LEAGUES_INTEREST, key is the name of the league on the FBRef and the value is the code related to
    each competition. Helps to ignore Women leagues that shares the exact same name that men.
    """

    if db_config:
        return {
            "LEAGUES_INTEREST": {"Premier-League": 9, "La-Liga": 12, "Bundesliga": 20, "Serie-A": 11, "Ligue-1": 13},
            "TO_DROP_COLS": ["ID_GAME", "#", "NATION", "POS", "AGE", "MIN"],
            "URL_WEB": f"{root_url}/en/matches/{wk_date}",
        } | db_config
    else:
        return {
            "LEAGUES_INTEREST": {"Premier-League": 9, "La-Liga": 12, "Bundesliga": 20, "Serie-A": 11, "Ligue-1": 13},
            "TO_DROP_COLS": ["ID_GAME", "#", "NATION", "POS", "AGE", "MIN"],
            "URL_WEB": f"{root_url}/en/matches/{wk_date}",
        }


def get_dates_list() -> list[str]:
    """_summary_

    Returns:
        list[str]: _description_
    """

    current_date = date.today()
    all_days_mth = np.arange("2023-02", "2023-03", dtype="datetime64[D]")

    return [str(date) for date in all_days_mth if date < current_date]


def get_table_name(query: str) -> str:
    """Extract the table name from a PSQL query.

    Args:
        query (str): PSQL Table creation query.
    """
    return query.split("(", 1)[0].split("EXISTS ")[1]


def table_creation_query(list_cols: list[str], table_name: str) -> str:
    start_table_query = f"CREATE TABLE IF NOT EXISTS {table_name}(\n"
    var_declaration = "\n".join([f'"{var}"' + " VARCHAR(200)" for var in ["PLAYER", "ID_GAME"] + list_cols])
    end_table_query = '\nPRIMARY KEY ("ID_GAME"));'

    full_query = start_table_query + var_declaration + end_table_query

    return full_query


def read_data_from_postgres(table_name: str, metadata: Union[dict[str, Any], None], **kwargs) -> pd.DataFrame:  # type: ignore
    """Read dataframe from PostGreSQL.

    Args:
        table_name (str): Name of PostGreSQL table.
        metadata (dict[str, Any]): Database configuration.

    Returns:
        pd.DataFrame: Dataset read from PostGreSQL.
    """

    if not metadata:
        return

    conn_string = f'postgresql://{metadata["user"]}:{metadata["password"]}@{metadata["host"]}/{metadata["database"]}'

    db = create_engine(conn_string)
    conn = db.connect()

    data = pd.read_sql(table_name, con=conn, **kwargs)

    conn.close()

    return data
