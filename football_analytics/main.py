import datetime
import logging
import time

import pandas as pd
from config import TableCreations, TableMapping, Tables

from football_analytics.tasks.preprocess_data import preprocess_game_data
from football_analytics.tasks.scraping_extraction import get_games_url
from football_analytics.tasks.write_postgres_table import (
    create_and_update_date_working_table,
    create_tables,
    data_to_postgres,
)
from football_analytics.utils import create_metadata, read_data_from_postgres

logging.basicConfig(format="[%(asctime)s] | [%(levelname)s]  %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def scraping_data_games(date: str) -> None:
    """Scraping FBref to get games statistics, and even players specific statistics.

    Args:
        date (str): Game on which the football game statistics scraping will oc
    """

    logger.info("Initiates scraping...")
    logger.info("Generating metadata...")
    scraping_metadata = create_metadata(date, ROOT_URL)

    logger.info("Scraping all game urls matching metadata perimeter...")
    games_url = get_games_url(scraping_metadata, ROOT_URL)

    if not games_url:
        logger.info(f"No URL to scrap. Program will end here for {date}.")
        time.sleep(5)
        return

    logger.info("Preprocessing each game data...")
    """game_and_player_dict ="""
    game_and_player_dict = preprocess_game_data(date, games_url, scraping_metadata)
    logger.info("Scraping done.")

    logger.info("Initiates PSQL writing...")
    for table in Tables:
        logger.info("Creating or appending %s table.", table.value)
        table_config = TableMapping(enhanced=True).get_table_info(table)

        create_tables(table_config.creation_query)
        data_to_postgres(date, game_and_player_dict[table_config.perimeter.value], table_config, table)

    # Tag the current working date in a specific table, to avoid future overwritting attempts
    create_tables(TableCreations.DATE_DATA_TABLE)
    create_and_update_date_working_table(date)

    logger.info("PSQL writing done.\n")


if __name__ == "__main__":
    ROOT_URL = "https://fbref.com"

    start_date = datetime.date(2024, 5, 4)  # start date
    end_date = datetime.date(2024, 5, 6)  # end date

    date_list = pd.date_range(start_date, end_date - datetime.timedelta(days=1), freq="d").tolist()
    date_list = [date.strftime(format="%Y-%m-%d") for date in date_list]

    try:
        date_df = read_data_from_postgres("date_working")
        existing_dates = date_df["DATE"].tolist()
    except Exception:
        existing_dates = []

    for date in date_list:
        logging.info(f"Processing games for {date}")

        if date in existing_dates:
            logging.warning(f"{date} was already scraped, this attempt will be skipped.")
            pass
        else:
            scraping_data_games(date)
