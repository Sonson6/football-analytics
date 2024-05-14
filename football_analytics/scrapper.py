import logging
import time
from typing import Any, Union

from football_analytics.config import TableCreations, TableMapping, Tables
from football_analytics.tasks.preprocess_data import preprocess_game_data
from football_analytics.tasks.scraping_extraction import get_games_url
from football_analytics.tasks.write_postgres_table import (
    create_and_update_date_working_table,
    create_tables,
    data_to_postgres,
)
from football_analytics.utils import create_metadata

logging.basicConfig(format="[%(asctime)s] | [%(levelname)s]  %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def scraping_data_games(date: str, db_config: Union[None, dict[str, Any]] = None) -> None:
    """Scraping FBref to get games statistics, and even players specific statistics.

    Args:
        date (str): Game on which the football game statistics scraping will occur.
        db_config (Union[None, dict[str, Any]]): database configurations. Defaults to None.
        save_to_psql (bool, optional): Save to psql or not.
    """

    ROOT_URL = "https://fbref.com"

    logger.info("Initiates scraping...")
    logger.info("Generating metadata...")
    scraping_metadata = create_metadata(date, ROOT_URL, db_config)

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

    if db_config:
        logger.info("Initiates PSQL writing...")
        for table in Tables:
            logger.info("Creating or appending %s table.", table.value)
            table_config = TableMapping(enhanced=True).get_table_info(table)

            create_tables(table_config.creation_query, scraping_metadata)
            data_to_postgres(
                scraping_metadata, date, game_and_player_dict[table_config.perimeter.value], table_config, table
            )

        # Tag the current working date in a specific table, to avoid future overwritting attempts
        create_tables(TableCreations.DATE_DATA_TABLE, scraping_metadata)
        create_and_update_date_working_table(date, scraping_metadata)

        logger.info("PSQL writing done.\n")
