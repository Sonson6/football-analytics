"""
Created on Sat Feb 18 13:43:42 2023

@author: nelso
"""

import itertools
from functools import reduce
from typing import Any

import numpy as np
import pandas as pd

from football_analytics.config import PlayerExtendCols, PlayerGeneralCols, TableMapping, Tables
from football_analytics.tasks.scraping_extraction import get_game_data


def enhance_match_df(match_df: pd.DataFrame) -> pd.DataFrame:
    """Creates new variables based to simplify the reading of the game result and _XG metrics.

    Args:
        match_df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """

    # GET GOAL PER TEAM
    match_df[["HOME_GOAL", "AWAY_GOAL"]] = match_df["SCORE"].str.split("-", expand=True).astype(int)

    # GET XG PER TEAM
    match_df[["HOME_GOAL_XG", "AWAY_GOAL_XG"]] = match_df["SCORE_XG"].str.split("-", expand=True).astype(float)

    # Remove the HOME/AWAY logic, SCORED/CONCEIDED are easier to read
    match_df["SCORED"] = np.where(match_df["STATUS"] == "HOME", match_df["HOME_GOAL"], match_df["AWAY_GOAL"])
    match_df["CONCEIDED"] = np.where(match_df["STATUS"] == "HOME", match_df["AWAY_GOAL"], match_df["HOME_GOAL"])

    match_df["SCORED_XG"] = np.where(match_df["STATUS"] == "HOME", match_df["HOME_GOAL_XG"], match_df["AWAY_GOAL_XG"])
    match_df["CONCEIDED_XG"] = np.where(
        match_df["STATUS"] == "HOME", match_df["AWAY_GOAL_XG"], match_df["HOME_GOAL_XG"]
    )

    # BUILDS FINAL RESULT OF THE GAME
    conditions = [match_df["SCORED"] > match_df["CONCEIDED"], match_df["SCORED"] < match_df["CONCEIDED"]]
    choices = ["WIN", "LOSS"]
    match_df["FINAL_RESULT"] = np.select(conditions, choices, default="DRAW")

    # BUILDS FINAL RESULT with HOME/AWAY additional information
    conditions = [match_df["HOME_GOAL"] > match_df["AWAY_GOAL"], match_df["HOME_GOAL"] < match_df["AWAY_GOAL"]]
    choices = ["HOME_WIN", "AWAY_WIN"]
    match_df["FINAL_RESULT_STATUS"] = np.select(conditions, choices, default="DRAW")

    return match_df.drop(["SCORE", "SCORE_XG"], axis=1)


def preprocess_game_data(date: str, games_url: list[str], metadata: dict[str, Any]) -> dict[str, pd.DataFrame]:
    """Aggregates individual game data to a Dataframes before exports.

    Args:
        date (str): Game on which the football game statistics scraping will occur.
        games_url (list[str]): Links to football games statistics.
        metadata (dict[str, Any]): Date based metadata with key configurations, among which PostGreSQL database
            information if requested.
    """

    # to_drop_cols = scraping_metadata["TO_DROP_COLS"]
    to_drop_cols = metadata["TO_DROP_COLS"]

    # List all game data
    functions_output = [get_game_data(game_url, date, to_drop_cols) for game_url in games_url]
    functions_output = [game for game in functions_output if game]

    # Extract main team statistics only
    team_stats = [team_stat[0] for team_stat in functions_output]
    team_stats = list(itertools.chain(*team_stats))

    # Extract player statistics only
    players_dict = [dict_table[1] for dict_table in functions_output]
    players_dict = {k: v for element in players_dict for k, v in element.items()}  # type: ignore

    # Build dataframe with main team statistics
    match_df = pd.DataFrame(
        team_stats,
        columns=TableMapping().get_table_info(Tables.GAME_DATA).get_all_columns(),
    )

    date_datetime = pd.to_datetime(date)
    match_df["SEASON"] = np.where(
        date_datetime.month <= 6,
        f"{date_datetime.year - 1}/{date_datetime.year}",
        f"{date_datetime.year}/{date_datetime.year + 1}",
    )

    match_df = enhance_match_df(match_df)

    # Dictionary is helping to assign a player to a team
    # Slicing on ID_GAME removes the game date, useless for mapping
    team_status_map = dict(zip(match_df["ID_GAME"].str[:-9] + "___" + match_df["STATUS"], match_df["TEAM"]))

    # Build dataframe with player statistics
    data_players_df = concat_data_players_df(players_dict, team_status_map)  # type: ignore
    data_players_df = data_players_df[~data_players_df[PlayerExtendCols.PLAYER].str.contains(" Players")]
    data_players_df = data_players_df.rename(columns={"#": PlayerExtendCols.NUMBER})
    data_players_df[PlayerGeneralCols.AGE] = data_players_df[PlayerGeneralCols.AGE].str[:-3]
    data_players_df[PlayerGeneralCols.NATION] = data_players_df[PlayerGeneralCols.NATION].str[-3:]

    return {"GAME": match_df, "PLAYER": data_players_df}


def concat_data_players_df(
    players_dict: dict[str, list[pd.DataFrame]], team_status_map: dict[str, str]
) -> pd.DataFrame:
    """Concatenate all games players statistics to a unique DataFrame

    Args:
        players_dict (dict[str, list[pd.DataFrame]]): Dictionary of players statistics, game names as key.
        team_status_map (dict[str, str]): Dictionary of teams playing and their status (Home or Away)

    Returns:
        pd.DataFrame: Aggregated players statistics.
    """

    game_list = [val for val in list(players_dict.keys()) for _ in (0, 1)]
    bool_list = [True if i % 2 == 0 else False for i in range(len(game_list))]

    game_tuple: tuple[tuple[str, bool], ...] = tuple(zip(game_list, bool_list))

    data_players_list = [
        build_data_players_list(players_dict, game, h_or_a, team_status_map) for game, h_or_a in game_tuple
    ]

    return pd.concat(data_players_list)


def build_data_players_list(
    players_dict: dict[str, list[pd.DataFrame]], game: str, home: bool, team_status_map: dict[str, str]
) -> pd.DataFrame:
    """Merge all statistics dataframe for a single game into a unique dataframe.

    Args:
        players_dict (dict[str, list[pd.DataFrame]]): Dictionary of players statistics, game names as key.
        game (str): Game key name.
        bool (bool): Home game or not (it affects the tables to select for the merge). Defaults to True.
        team_status_map (dict[str, str]): Dictionary of teams playing and their status (Home or Away)

    Returns:
        pd.DataFrame: Contains home or away team players complete statistics
    """

    if home:
        data = players_dict[game][:7]
        TEAM = team_status_map[game + "___" + "HOME"]
    else:
        data = players_dict[game][7:]
        TEAM = team_status_map[game + "___" + "AWAY"]

    data_merge = reduce(
        lambda left, right: pd.merge(  # Merge DataFrames in list
            left, right, on=["PLAYER"], how="left"
        ),
        data,
    )
    data_merge["TEAM"] = TEAM

    to_drop = [col for col in data_merge.columns if col.endswith("_y")]
    to_rename = {col: col[:-2] for col in data_merge.columns if col.endswith("_x")}

    data_merge = data_merge.drop(to_drop, axis=1)

    return data_merge.rename(columns=to_rename)
