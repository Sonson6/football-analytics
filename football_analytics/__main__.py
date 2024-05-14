import logging
from typing import Union

import pandas as pd
import typer

from football_analytics.config import load_postgres_config
from football_analytics.scrapper import scraping_data_games
from football_analytics.utils import read_data_from_postgres, treat_cli_params

app = typer.Typer()


@app.callback(invoke_without_command=True)  # type: ignore
def scrap(
    start_date: str = typer.Option(
        "20240101",
        "--start-date",
        help="first date to be scrapped, should be YYYYMMDD.",
    ),
    end_date: Union[str, None] = typer.Option(
        None,
        "--end-date",
        help="last date to be scrapped, should be YYYYMMDD.",
    ),
    db_config_path: Union[str, None] = typer.Option(
        None,
        "--db-config-path",
        help=(
            "Path to the config file, should be a .ini file with a [postgresql] header."
            "If filed, we consider you want to save the output to the related database."
        ),
    ),
) -> None:
    """
    Typer command running the main function, called scraping_data_games.

    Args:
        start_date (str, optional): first date to be scrapped, should be YYYYMMDD.
                    Defaults to "20240101".
        end_date (Union[str, None], optional): last date to be scrapped, should be YYYYMMDD.
                    Defaults to None.
        db_config_path (Union[str, None], optional): Path to the config file.
                It should be a .ini file with a [postgresql] header.
                If filed,  we consider you want to save the output to the related database.
                        Defaults to None.
    """

    start_date, end_date = treat_cli_params(start_date, end_date)

    date_list = pd.date_range(start_date, end_date, freq="d").tolist()
    date_list = [date.strftime(format="%Y-%m-%d") for date in date_list]

    db_config = load_postgres_config(filename=db_config_path)

    try:
        date_df = read_data_from_postgres("date_working", db_config)
        existing_dates = date_df["DATE"].tolist()
    except Exception:
        existing_dates = []

    for date in date_list:
        logging.info(f"Processing games for {date}")

        if date in existing_dates:
            logging.warning(f"{date} was already scraped, this attempt will be skipped.")
            pass
        else:
            scraping_data_games(date, db_config=db_config)
