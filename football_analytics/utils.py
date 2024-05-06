"""
Created on Sat Feb 18 13:54:51 2023

@author: nelso
"""


from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from config import TableMapping
from sqlalchemy import create_engine


def create_metadata(wk_date: str, root_url: str) -> dict[str, Any]:
    """Create the required dictionary to prepare scraping settings.

    Args:
        wk_date (str): Game on which the football game statistics scraping will occur.
        root_url (str): Link to the FBRef website (to disappear soon)

    Returns:
        dict[str, Any]: Date based metadata with league of interest and related URLs.

    For LEAGUES_INTEREST, key is the name of the league on the FBRef and the value is the code related to
    each competition. Helps to ignore Women leagues that shares the exact same name that men.
    """
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


def read_data_from_postgres(table_name: str, **kwargs) -> pd.DataFrame:  # type: ignore
    """Read dataframe from PostGreSQL.

    Args:
        table_name (str): Name of PostGreSQL table.

    Returns:
        pd.DataFrame: Dataset read from PostGreSQL.
    """

    config = TableMapping.config
    conn_string = f'postgresql://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}'

    db = create_engine(conn_string)
    conn = db.connect()

    data = pd.read_sql(table_name, con=conn, **kwargs)

    conn.close()

    return data
