"""
Created on Sat Feb 18 13:46:26 2023

@author: nelso
"""

from typing import Any

import pandas as pd
from bs4.element import Tag


def preprocess_match_tables(game_url: str) -> tuple[list[pd.DataFrame], str]:
    """Preprocess players statistics tables.

    Args:
        game_url (str): URL to specific game

    Returns:
        tuple[list[pd.DataFrame], str]: Players statistics tables and game key names
    """

    tables_game = pd.read_html(game_url)[2:]

    for table in tables_game:
        renamed_cols = [
            (group, f"{group}_{kpi}") if (not group.startswith("Unnamed")) else (group, kpi)
            for (group, kpi) in table.columns
        ]

        dict_rename = dict(zip(table.columns, renamed_cols))

        table.columns = table.columns.values
        table.columns = pd.MultiIndex.from_tuples(table.rename(columns=dict_rename))
        table.columns = table.columns.droplevel().str.upper()

    return tables_game[1:-3], "_".join(tables_game[0].columns.str[:-11])


def drop_dup_cols(table: pd.DataFrame, to_drop_cols: list[str]) -> pd.DataFrame:
    """All tables have some common columns, so we decide to drop them to avoid duplication

    Args:
        table (pd.DataFrame): Players statistics dataframe
        to_drop_cols (list[str]): Duplicated columns to drop

    Returns:
        pd.DataFrame: Players statistics dataframe without duplicated columns
    """

    to_drop = [col for col in to_drop_cols if col in table.columns]
    return table.drop(to_drop, axis=1)


def get_match_report(tbody: Tag) -> list[Tag]:
    """Filter .find_all("tbody") href tag element to keep only the ones representing match reports

    Args:
        tbody (Tag): Unique output of .find_all("tbody")

    Returns:
        list[Tag]: All Href representing match reports
    """
    return [a["href"] for a in tbody.find_all("a") if a.get("href") and "Match Report" in a]


def get_comps_id(tbody: Tag) -> int:
    """Filter .find_all("tbody") to keep only
        the one related to one of our competition of interest.

    Args:
        tbody (Tag): Unique output of .find_all("tbody")

    Returns:
        int: ID of the competition.
    """

    # We select only the first href of tbody because it's always the
    # link page for the competition representing the tbody
    # With the ID of this competition (which is located in the href)
    # we are then able to filter all the unwanted competitions
    try:
        return int(tbody.find_all("a", href=True)[0]["href"].split("/")[-2])
    except Exception:
        return 9999


def return_nb_attempted(elem: Tag) -> Any:
    """Extract game statistics in BS4 object.

    Args:
        elem (Tag): BS4 object containing games statistics (either possesion, successful passes % etc...)

    Returns:
        str: Extract the statistics itself (usually wrote as 100 of 200
            for 100 passes succeeded on 200 attempted)
    """

    nb_attempted = elem.find_all("div")[1].text.split("—")
    nb_attempted = [nb for nb in nb_attempted if "of" in nb][0]

    return nb_attempted.replace("\xa0", "")


def get_missing_saves_stat(soup: Tag) -> bool:
    """Search whether or not there is a "Saves" statistic in the game
    FBref page. This statistic is missing when there was no
    Shots on Target.

    Args:
        soup (Tag): Game FBref page on BS4 object.

    Returns:
        bool: Whether or not the "Saves" statistic is missing.
    """

    stat_searcher = soup.select('div[id*="team_stats"]')[0].find_all("th")
    stat_searcher = [
        stat_searcher_ind for stat_searcher_ind in stat_searcher if "colspan" in stat_searcher_ind.attrs.keys()
    ]
    stat_searcher = [stat_searcher.string for stat_searcher in stat_searcher]

    return True if "Saves" not in stat_searcher else False
