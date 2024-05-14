"""
Created on Sat Feb 18 13:43:41 2023

@author: nelso
"""

import itertools
import logging
import time
from typing import Any

import pandas as pd
import requests  # type: ignore
from bs4 import BeautifulSoup

from football_analytics.tasks.scraping_utils import (
    drop_dup_cols,
    get_comps_id,
    get_match_report,
    get_missing_saves_stat,
    preprocess_match_tables,
    return_nb_attempted,
)

logging.basicConfig(format="[%(asctime)s] | [%(levelname)s]  %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_games_url(metadata: dict[str, Any], root_url: str) -> list[str]:
    """Filter FBref main match page to extract football games links (only the ones from
        the leagues of interest in metadata)

    Args:
        metadata (dict[str, Any]): Date based metadata with key configurations, among which PostGreSQL database
            information if requested.
        root_url (str): Link to the FBRef website (to disappear soon)

    Returns:
        games_url (list[str]): Links to football games statistics.
    """

    url_web, leagues_interest = (
        metadata["URL_WEB"],
        metadata["LEAGUES_INTEREST"],
    )

    response = requests.get(url_web)
    soup = BeautifulSoup(response.text, "html.parser")

    # Récupération des informations
    all_tbody = soup.find_all("tbody")

    # Filtering unwanted competitions, see get_comps_id for more details
    all_tbody = [tbody for tbody in all_tbody if get_comps_id(tbody) in leagues_interest.values()]

    # Filtrage pour garder les balises contenant les "match reports"
    # uniquement puis flatten méthode via itertools
    match_reports = [get_match_report(table) for table in all_tbody]
    match_reports = list(itertools.chain(*match_reports))

    # Build the game link from the fbref root url
    return [f"{root_url}{game}" for game in match_reports]


# from tasks.scraping_extraction import get_game_data
def get_game_data(
    game_url: str, date: str, to_drop_cols: list[str]
) -> tuple[list[list[Any]], dict[str, list[pd.DataFrame]]]:  # game_url = games_url[4]
    """Extract all game and player statistics.

    Args:
        game_url (str): URL to specific game
        date (str): Game on which the football game statistics scraping will occur.
        to_drop_cols (list[str]): Useless statistics columns because duplicated

    Returns:
        tuple[list[Any], dict[str, list[pd.DataFrame]]]: _description_
    """

    # Request Game URL and convert to BS4 object
    response = requests.get(game_url)
    soup = BeautifulSoup(response.text, "html.parser")

    # GET PLAYERS STATS
    tables_game, key_games = preprocess_match_tables(game_url)

    val_insert = f"{key_games}_{date.replace('-', '')}"

    [table.insert(1, "ID_GAME", val_insert) for table in tables_game]

    # Sort players statistics by Home and Away
    # This division Home/Away mandatory to avoid droping to_drop_cols variables from
    # tables_game of reference which are indexed as 0 and 7
    tables_game_home = [tables_game[0]] + [drop_dup_cols(table, to_drop_cols) for table in tables_game[1:7]]
    tables_game_away = [tables_game[7]] + [drop_dup_cols(table, to_drop_cols) for table in tables_game[8:]]

    # Aggregating player statistics to a dictionary
    dict_tables = {}
    dict_tables[key_games] = tables_game_home + tables_game_away

    # GET TEAM STATS
    team_home, team_away = key_games.split("_")

    team_stats = []

    # Extract game result (score and score_xg)
    scores = soup.find_all("div", {"class": "scores"})

    game_score = "-".join([score.find(class_="score").get_text() for score in scores])
    game_score_xg = "-".join([score.find(class_="score_xg").get_text() for score in scores])

    # Extract game statistics (posession, passing, cards...)
    save = soup.select('div[id*="team_stats"]')[0].find_all("td")

    stat_list = [stat.find("strong").string for stat in save[:-2]]
    stat_list += [len(stat.select("span")) for stat in save[-2:]]
    stat_list += [return_nb_attempted(pass_nb) for pass_nb in save[2:-2]]

    team_stats.append([val_insert, team_home, "HOME", game_score, game_score_xg] + stat_list[::2])
    team_stats.append([val_insert, team_away, "AWAY", game_score, game_score_xg] + stat_list[1::2])

    # Add artificially missing "Saves" in output if the tag
    # (and therefore the statistic) is missing
    if get_missing_saves_stat(soup):
        for team_stat in team_stats:
            team_stat.insert(6, "0%")
            team_stat.append("0 of 0")

    logging.info("TREATING %s", key_games)
    time.sleep(10)

    return team_stats, dict_tables
