import logging
from typing import Any

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from football_analytics.config import TableDefinition, Tables
from football_analytics.utils import get_table_name, read_data_from_postgres

logging.basicConfig(format="[%(asctime)s] | [%(levelname)s]  %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_and_update_date_working_table(date: str, metadata: dict[str, Any]) -> None:
    """ """

    sql = """INSERT INTO date_working("DATE")
             VALUES(%s);"""

    db_config = {k: metadata[k] for k in list(metadata)[-4:]}

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql, (date,))

                # commit the changes to the database
                conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_tables(query: str, metadata: dict[str, Any], drop_before: bool = False) -> None:
    """Create tables in the PostgreSQL database.

    Args:
        query (str): Query to create the table.
        metadata (dict[str, Any]): Date based metadata with key configurations, among which PostGreSQL database
            information if requested.
        drop_before (bool, optional): Wether or not the table should be dropped if already existing. Defaults to False.
    """

    config_dict = {k: metadata[k] for k in list(metadata)[-4:]}

    try:
        with psycopg2.connect(**config_dict) as conn:
            with conn.cursor() as cur:
                if drop_before:
                    table_name = get_table_name(query)
                    cur.execute(f"drop table {table_name};")
                # execute the CREATE TABLE statement
                cur.execute(query)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def data_to_postgres(
    metadata: dict[str, Any], date: str, df: pd.DataFrame, my_table_object: TableDefinition, table: Tables
) -> None:
    """_summary_

    Args:
        metadata (dict[str, Any]): Date based metadata with key configurations, among which PostGreSQL database
            information if requested.
        date (str): _description_
        df (pd.DataFrame): _description_
        wk_columns (list[str]): _description_
    """

    logger.info("Exporting results into PSQL...")
    db_config = {k: metadata[k] for k in list(metadata)[-4:]}
    conn_string = (
        f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}/{db_config["database"]}'
    )
    table_name = table.value
    logger.info("Connecting to the PSQL database.")
    db = create_engine(conn_string)
    conn = db.connect()

    logger.info("Writing data to PSQL.")
    # Create DataFrame
    df_filtered = df[my_table_object.get_all_columns()].copy()

    try:
        df_filtered.to_sql(table_name, con=conn, if_exists="append", index=False)
        logger.info("No duplicated primary key, data was written directly to %s", table_name)
    except Exception:
        primary_key_list = my_table_object.primary_key

        original_table = read_data_from_postgres(table_name, db_config)
        append_table = pd.concat([original_table, df_filtered])
        append_table = append_table.drop_duplicates(subset=primary_key_list, keep="last")

        create_tables(my_table_object.creation_query, metadata, drop_before=True)
        append_table.to_sql(table_name, con=conn, if_exists="append", index=False)
        logger.warning(
            "Due to duplicated primary key, original table was read, concatenated with new data, "
            "deduplicated and then the output was written directly to %s",
            table_name,
        )

    conn.close()

    logger.info("Export done.")
