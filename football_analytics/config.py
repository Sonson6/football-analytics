from configparser import ConfigParser
from enum import Enum
from typing import Union


def load_postgres_config(filename: Union[str, None], section: str = "postgresql") -> Union[dict[str, str], None]:
    """Read configuration file with credentials.

    Args:
        filename (str, optional): Name of the file containing PostGreSQL credentials. Defaults to "database.ini".
        section (str, optional): Defaults to "postgresql".

    Raises:
        Exception: Raised when no config found.

    Returns:
        dict[str, str]: Configuration information.
    """
    parser = ConfigParser()

    if filename:
        my_reader = parser.read(filename)
        if not my_reader:
            my_reader = parser.read(f"../{filename}")

        # get section, default to postgresql
        config = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                config[param[0]] = param[1]
        else:
            raise Exception(f"Section {section} not found in the {filename} file")
    else:
        config = None

    return config


class Perimeter(Enum):
    """Table perimeter."""

    GAME = "GAME"
    PLAYER = "PLAYER"


class Tables(Enum):
    """SQL tables."""

    GAME_DATA = "game_data"
    PLAYER_GENERAL_INFO = "player_general_info"
    PLAYER_GAME_INFO_EXTEND = "player_game_info_extend"
    PLAYER_GAME_SUMMARY = "player_game_summary"
    PLAYER_GAME_PASSING_INFO = "passing_table"
    PLAYER_GAME_PASSING_TYPE_INFO = "passing_type_table"
    PLAYER_GAME_DEFENSIVE_INFO = "defensive_table"
    PLAYER_GAME_POSSESSION_INFO = "possession_table"
    PLAYER_GAME_MISSES_INFO = "misses_table"
    PLAYER_GAME_GOALKEEPER_INFO = "goalkeeper_table"


class TableCreations:
    """TableCreations."""

    DATE_DATA_TABLE = """
        CREATE TABLE IF NOT EXISTS date_working(
        "DATE" VARCHAR(200) PRIMARY KEY
        );"""

    GAME_DATA_TABLE = """
            CREATE TABLE IF NOT EXISTS game_data(
            "ID_GAME" VARCHAR(200),
            "SEASON" VARCHAR(200),
            "TEAM" VARCHAR(200),
            "STATUS" VARCHAR(200),
            "possession%" VARCHAR(200),
            "pass_acc%" VARCHAR(200),
            "SoT%" VARCHAR(200),
            "saves%" VARCHAR(200),
            yellow_or_red_card VARCHAR(200),
            pass_acc VARCHAR(200),
            "SoT" VARCHAR(200),
            saves VARCHAR(200),
            "HOME_GOAL" VARCHAR(200),
            "AWAY_GOAL" VARCHAR(200),
            "HOME_GOAL_XG" VARCHAR(200),
            "AWAY_GOAL_XG" VARCHAR(200),
            "SCORED" VARCHAR(200),
            "CONCEIDED" VARCHAR(200),
            "SCORED_XG" VARCHAR(200),
            "CONCEIDED_XG" VARCHAR(200),
            "FINAL_RESULT" VARCHAR(200),
            "FINAL_RESULT_STATUS" VARCHAR(200),
            PRIMARY KEY ("ID_GAME", "TEAM")
            );
            """
    PLAYER_GAME_SUMMARY_TABLE = """
            CREATE TABLE IF NOT EXISTS player_game_summary(
            "PLAYER" VARCHAR(200),
            "ID_GAME" VARCHAR(200),
            "PERFORMANCE_GLS" VARCHAR(200),
            "PERFORMANCE_AST" VARCHAR(200),
            "PERFORMANCE_PK" VARCHAR(200),
            "PERFORMANCE_PKATT" VARCHAR(200),
            "PERFORMANCE_SH" VARCHAR(200),
            "PERFORMANCE_SOT" VARCHAR(200),
            "PERFORMANCE_TOUCHES" VARCHAR(200),
            "PERFORMANCE_TKL" VARCHAR(200),
            "PERFORMANCE_INT" VARCHAR(200),
            "PERFORMANCE_BLOCKS" VARCHAR(200),
            "EXPECTED_XG" VARCHAR(200),
            "EXPECTED_NPXG" VARCHAR(200),
            "EXPECTED_XAG" VARCHAR(200),
            "SCA_SCA" VARCHAR(200),
            "SCA_GCA" VARCHAR(200),
            "PASSES_CMP" VARCHAR(200),
            "PASSES_ATT" VARCHAR(200),
            "PASSES_CMP%" VARCHAR(200),
            "PASSES_PRGP" VARCHAR(200),
            "CARRIES_CARRIES" VARCHAR(200),
            "CARRIES_PRGC" VARCHAR(200),
            "TAKE-ONS_ATT" VARCHAR(200),
            "TAKE-ONS_SUCC" VARCHAR(200),
            PRIMARY KEY ("PLAYER", "ID_GAME")
            );
            """
    PLAYER_GENERAL_INFO_TABLE = """
            CREATE TABLE IF NOT EXISTS player_general_info(
            "PLAYER" VARCHAR(200),
            "ID_GAME" VARCHAR(200),
            "NATION" VARCHAR(200),
            "AGE" VARCHAR(200),
            PRIMARY KEY ("PLAYER", "ID_GAME")
            );
            """
    PLAYER_GAME_INFO_EXTEND_TABLE = """
            CREATE TABLE IF NOT EXISTS player_game_info_extend(
            "PLAYER" VARCHAR(200),
            "TEAM" VARCHAR(200),
            "ID_GAME" VARCHAR(200),
            "NUMBER" VARCHAR(200),
            "POS" VARCHAR(200),
            "MIN" VARCHAR(200),
            PRIMARY KEY ("PLAYER", "TEAM", "ID_GAME")
            );
            """
    PLAYER_GAME_PASSING_INFO_TABLE = """
            CREATE TABLE IF NOT EXISTS passing_table(
            "PLAYER" VARCHAR(200),
            "ID_GAME" VARCHAR(200),
            "TOTAL_CMP" VARCHAR(200),
            "TOTAL_ATT" VARCHAR(200),
            "TOTAL_CMP%" VARCHAR(200),
            "TOTAL_TOTDIST" VARCHAR(200),
            "TOTAL_PRGDIST" VARCHAR(200),
            "SHORT_CMP" VARCHAR(200),
            "SHORT_ATT" VARCHAR(200),
            "SHORT_CMP%" VARCHAR(200),
            "MEDIUM_CMP" VARCHAR(200),
            "MEDIUM_ATT" VARCHAR(200),
            "MEDIUM_CMP%" VARCHAR(200),
            "LONG_CMP" VARCHAR(200),
            "LONG_ATT" VARCHAR(200),
            "LONG_CMP%" VARCHAR(200),
            "AST" VARCHAR(200),
            "XAG" VARCHAR(200),
            "XA" VARCHAR(200),
            "KP" VARCHAR(200),
            "1/3" VARCHAR(200),
            "PPA" VARCHAR(200),
            "CRSPA" VARCHAR(200),
            "PRGP" VARCHAR(200),
            PRIMARY KEY ("PLAYER", "ID_GAME")
            );
            """
    PLAYER_GAME_PASSING_TYPE_INFO_TABLE = """CREATE TABLE IF NOT EXISTS passing_type_table(
        "PLAYER" VARCHAR(200),
        "ID_GAME" VARCHAR(200),
        "ATT" VARCHAR(200),
        "PASS TYPES_LIVE" VARCHAR(200),
        "PASS TYPES_DEAD" VARCHAR(200),
        "PASS TYPES_FK" VARCHAR(200),
        "PASS TYPES_TB" VARCHAR(200),
        "PASS TYPES_SW" VARCHAR(200),
        "PASS TYPES_CRS" VARCHAR(200),
        "PASS TYPES_TI" VARCHAR(200),
        "PASS TYPES_CK" VARCHAR(200),
        "CORNER KICKS_IN" VARCHAR(200),
        "CORNER KICKS_OUT" VARCHAR(200),
        "CORNER KICKS_STR" VARCHAR(200),
        "OUTCOMES_CMP" VARCHAR(200),
        "OUTCOMES_OFF" VARCHAR(200),
        "OUTCOMES_BLOCKS" VARCHAR(200),
        PRIMARY KEY ("PLAYER", "ID_GAME")
        );
        """

    PLAYER_GAME_DEFENSIVE_INFO_TABLE = """CREATE TABLE IF NOT EXISTS defensive_table(
        "PLAYER" VARCHAR(200),
        "ID_GAME" VARCHAR(200),
        "TACKLES_TKL" VARCHAR(200),
        "TACKLES_TKLW" VARCHAR(200),
        "TACKLES_DEF 3RD" VARCHAR(200),
        "TACKLES_MID 3RD" VARCHAR(200),
        "TACKLES_ATT 3RD" VARCHAR(200),
        "CHALLENGES_TKL" VARCHAR(200),
        "CHALLENGES_ATT" VARCHAR(200),
        "CHALLENGES_TKL%" VARCHAR(200),
        "CHALLENGES_LOST" VARCHAR(200),
        "BLOCKS_BLOCKS" VARCHAR(200),
        "BLOCKS_SH" VARCHAR(200),
        "BLOCKS_PASS" VARCHAR(200),
        "INT" VARCHAR(200),
        "TKL+INT" VARCHAR(200),
        "CLR" VARCHAR(200),
        "ERR" VARCHAR(200),
        PRIMARY KEY ("PLAYER", "ID_GAME")
        );
        """

    PLAYER_GAME_POSSESSION_INFO_TABLE = """CREATE TABLE IF NOT EXISTS possession_table(
        "PLAYER" VARCHAR(200),
        "ID_GAME" VARCHAR(200),
        "TOUCHES_TOUCHES" VARCHAR(200),
        "TOUCHES_DEF PEN" VARCHAR(200),
        "TOUCHES_DEF 3RD" VARCHAR(200),
        "TOUCHES_MID 3RD" VARCHAR(200),
        "TOUCHES_ATT 3RD" VARCHAR(200),
        "TOUCHES_ATT PEN" VARCHAR(200),
        "TOUCHES_LIVE" VARCHAR(200),
        "TAKE-ONS_SUCC%" VARCHAR(200),
        "TAKE-ONS_TKLD" VARCHAR(200),
        "TAKE-ONS_TKLD%" VARCHAR(200),
        "CARRIES_TOTDIST" VARCHAR(200),
        "CARRIES_PRGDIST" VARCHAR(200),
        "CARRIES_1/3" VARCHAR(200),
        "CARRIES_CPA" VARCHAR(200),
        "CARRIES_MIS" VARCHAR(200),
        "CARRIES_DIS" VARCHAR(200),
        "RECEIVING_REC" VARCHAR(200),
        "RECEIVING_PRGR" VARCHAR(200),
        PRIMARY KEY ("PLAYER", "ID_GAME")
        );
        """

    PLAYER_GAME_MISSES_INFO_TABLE = """CREATE TABLE IF NOT EXISTS misses_table(
    "PLAYER" VARCHAR(200),
    "ID_GAME" VARCHAR(200),
    "PERFORMANCE_CRDY" VARCHAR(200),
    "PERFORMANCE_CRDR" VARCHAR(200),
    "PERFORMANCE_2CRDY" VARCHAR(200),
    "PERFORMANCE_FLS" VARCHAR(200),
    "PERFORMANCE_FLD" VARCHAR(200),
    "PERFORMANCE_OFF" VARCHAR(200),
    "PERFORMANCE_CRS" VARCHAR(200),
    "PERFORMANCE_TKLW" VARCHAR(200),
    "PERFORMANCE_PKWON" VARCHAR(200),
    "PERFORMANCE_PKCON" VARCHAR(200),
    "PERFORMANCE_OG" VARCHAR(200),
    "PERFORMANCE_RECOV" VARCHAR(200),
    "AERIAL DUELS_WON" VARCHAR(200),
    "AERIAL DUELS_LOST" VARCHAR(200),
    "AERIAL DUELS_WON%" VARCHAR(200),
    PRIMARY KEY ("PLAYER", "ID_GAME")
    );
    """

    PLAYER_GAME_GOALKEEPER_INFO_TABLE = """CREATE TABLE IF NOT EXISTS goalkeeper_table(
        "PLAYER" VARCHAR(200),
        "ID_GAME" VARCHAR(200),
        "SHOT STOPPING_SOTA" VARCHAR(200),
        "SHOT STOPPING_GA" VARCHAR(200),
        "SHOT STOPPING_SAVES" VARCHAR(200),
        "SHOT STOPPING_SAVE%" VARCHAR(200),
        "SHOT STOPPING_PSXG" VARCHAR(200),
        "LAUNCHED_CMP" VARCHAR(200),
        "LAUNCHED_ATT" VARCHAR(200),
        "LAUNCHED_CMP%" VARCHAR(200),
        "PASSES_THR" VARCHAR(200),
        "PASSES_LAUNCH%" VARCHAR(200),
        "PASSES_AVGLEN" VARCHAR(200),
        "GOAL KICKS_ATT" VARCHAR(200),
        "GOAL KICKS_LAUNCH%" VARCHAR(200),
        "GOAL KICKS_AVGLEN" VARCHAR(200),
        "CROSSES_OPP" VARCHAR(200),
        "CROSSES_STP" VARCHAR(200),
        "CROSSES_STP%" VARCHAR(200),
        "SWEEPER_#OPA" VARCHAR(200),
        "SWEEPER_AVGDIST" VARCHAR(200),
        PRIMARY KEY ("PLAYER", "ID_GAME")
        );
        """


class MatchCols:
    """MatchCols"""

    ID_GAME = "ID_GAME"
    TEAM = "TEAM"
    STATUS = "STATUS"
    SCORE = "SCORE"
    SCORE_XG = "SCORE_XG"
    POSSESSION_PERC = "possession%"
    PASS_ACC_PERC = "pass_acc%"
    SOT_PERC = "SoT%"
    SAVES_PERC = "saves%"
    Y_R_CARDS = "yellow_or_red_card"
    PASS_ACC = "pass_acc"
    SOT = "SoT"
    SAVES = "saves"


class MatchColsEnhanced:
    """MatchColsEnhanced"""

    ID_GAME = "ID_GAME"
    SEASON = "SEASON"
    TEAM = "TEAM"
    STATUS = "STATUS"
    POSSESSION_PERC = "possession%"
    PASS_ACC_PERC = "pass_acc%"
    SOT_PERC = "SoT%"
    SAVES_PERC = "saves%"
    Y_R_CARDS = "yellow_or_red_card"
    PASS_ACC = "pass_acc"
    SOT = "SoT"
    SAVES = "saves"
    HOME_GOAL = "HOME_GOAL"
    AWAY_GOAL = "AWAY_GOAL"
    HOME_GOAL_XG = "HOME_GOAL_XG"
    AWAY_GOAL_XG = "AWAY_GOAL_XG"
    SCORED = "SCORED"
    CONCEIDED = "CONCEIDED"
    SCORED_XG = "SCORED_XG"
    CONCEIDED_XG = "CONCEIDED_XG"
    FINAL_RESULT = "FINAL_RESULT"
    FINAL_RESULT_STATUS = "FINAL_RESULT_STATUS"


class PlayerGeneralCols:
    """PlayerGeneralCols"""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    NATION = "NATION"
    AGE = "AGE"


class PlayerExtendCols:
    """PlayerExtendCols."""

    PLAYER = "PLAYER"
    TEAM = "TEAM"
    ID_GAME = "ID_GAME"
    NUMBER = "NUMBER"
    POS = "POS"
    MIN = "MIN"


class PlayerGameSummaryCols:
    """PlayerGameSummaryCols."""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    PERFORMANCE_GLS = "PERFORMANCE_GLS"
    PERFORMANCE_AST = "PERFORMANCE_AST"
    PERFORMANCE_PK = "PERFORMANCE_PK"
    PERFORMANCE_PKATT = "PERFORMANCE_PKATT"
    PERFORMANCE_SH = "PERFORMANCE_SH"
    PERFORMANCE_SOT = "PERFORMANCE_SOT"
    PERFORMANCE_TOUCHES = "PERFORMANCE_TOUCHES"
    PERFORMANCE_TKL = "PERFORMANCE_TKL"
    PERFORMANCE_INT = "PERFORMANCE_INT"
    PERFORMANCE_BLOCKS = "PERFORMANCE_BLOCKS"
    EXPECTED_XG = "EXPECTED_XG"
    EXPECTED_NPXG = "EXPECTED_NPXG"
    EXPECTED_XAG = "EXPECTED_XAG"
    SCA_SCA = "SCA_SCA"
    SCA_GCA = "SCA_GCA"
    PASSES_CMP = "PASSES_CMP"
    PASSES_ATT = "PASSES_ATT"
    PASSES_CMP_PERC = "PASSES_CMP%"
    PASSES_PRGP = "PASSES_PRGP"
    CARRIES_CARRIES = "CARRIES_CARRIES"
    CARRIES_PRGC = "CARRIES_PRGC"
    TAKE_ONS_ATT = "TAKE-ONS_ATT"
    TAKE_ONS_SUCC = "TAKE-ONS_SUCC"


class PlayerGamePassingCols:
    """PlayerGamePassingCols."""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    TOTAL_CMP = "TOTAL_CMP"
    TOTAL_ATT = "TOTAL_ATT"
    TOTAL_CMP_PERC = "TOTAL_CMP%"
    TOTAL_TOTDIST = "TOTAL_TOTDIST"
    TOTAL_PRGDIST = "TOTAL_PRGDIST"
    SHORT_CMP = "SHORT_CMP"
    SHORT_ATT = "SHORT_ATT"
    SHORT_CMP_PERC = "SHORT_CMP%"
    MEDIUM_CMP = "MEDIUM_CMP"
    MEDIUM_ATT = "MEDIUM_ATT"
    MEDIUM_CMP_PERC = "MEDIUM_CMP%"
    LONG_CMP = "LONG_CMP"
    LONG_ATT = "LONG_ATT"
    LONG_CMP_PERC = "LONG_CMP%"
    AST = "AST"
    XAG = "XAG"
    XA = "XA"
    KP = "KP"
    ONE_THIRD = "1/3"
    PPA = "PPA"
    CRSPA = "CRSPA"
    PRGP = "PRGP"


class PlayerGamePassingTypeCols:
    """PlayerGamePassingTypeCols"""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    ATT = "ATT"
    PASS_TYPES_LIVE = "PASS TYPES_LIVE"
    PASS_TYPES_DEAD = "PASS TYPES_DEAD"
    PASS_TYPES_FK = "PASS TYPES_FK"
    PASS_TYPES_TB = "PASS TYPES_TB"
    PASS_TYPES_SW = "PASS TYPES_SW"
    PASS_TYPES_CRS = "PASS TYPES_CRS"
    PASS_TYPES_TI = "PASS TYPES_TI"
    PASS_TYPES_CK = "PASS TYPES_CK"
    CORNER_KICKS_IN = "CORNER KICKS_IN"
    CORNER_KICKS_OUT = "CORNER KICKS_OUT"
    CORNER_KICKS_STR = "CORNER KICKS_STR"
    OUTCOMES_CMP = "OUTCOMES_CMP"
    OUTCOMES_OFF = "OUTCOMES_OFF"
    OUTCOMES_BLOCKS = "OUTCOMES_BLOCKS"


class PlayerGameDefensiveCols:
    """PlayerGameDefensiveCols."""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    TACKLES_TKL = "TACKLES_TKL"
    TACKLES_TKLW = "TACKLES_TKLW"
    TACKLES_DEF_THIRD = "TACKLES_DEF 3RD"
    TACKLES_MID_THIRD = "TACKLES_MID 3RD"
    TACKLES_ATT_THIRD = "TACKLES_ATT 3RD"
    CHALLENGES_TKL = "CHALLENGES_TKL"
    CHALLENGES_ATT = "CHALLENGES_ATT"
    CHALLENGES_TKL_PERC = "CHALLENGES_TKL%"
    CHALLENGES_LOST = "CHALLENGES_LOST"
    BLOCKS_BLOCKS = "BLOCKS_BLOCKS"
    BLOCKS_SH = "BLOCKS_SH"
    BLOCKS_PASS = "BLOCKS_PASS"
    INT = "INT"
    TKL_AND_INT = "TKL+INT"
    CLR = "CLR"
    ERR = "ERR"


class PlayerGamePossessionCols:
    """PlayerGamePossessionCols."""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    TOUCHES_TOUCHES = "TOUCHES_TOUCHES"
    TOUCHES_DEF_PEN = "TOUCHES_DEF PEN"
    TOUCHES_DEF_THIRD = "TOUCHES_DEF 3RD"
    TOUCHES_MID_THIRD = "TOUCHES_MID 3RD"
    TOUCHES_ATT_THIRD = "TOUCHES_ATT 3RD"
    TOUCHES_ATT_PEN = "TOUCHES_ATT PEN"
    TOUCHES_LIVE = "TOUCHES_LIVE"
    TAKE_ONS_SUCC_PERC = "TAKE-ONS_SUCC%"
    TAKE_ONS_TKLD = "TAKE-ONS_TKLD"
    TAKE_ONS_TKLD_PERC = "TAKE-ONS_TKLD%"
    CARRIES_TOTDIST = "CARRIES_TOTDIST"
    CARRIES_PRGDIST = "CARRIES_PRGDIST"
    CARRIES_ONE_THIRD = "CARRIES_1/3"
    CARRIES_CPA = "CARRIES_CPA"
    CARRIES_MIS = "CARRIES_MIS"
    CARRIES_DIS = "CARRIES_DIS"
    RECEIVING_REC = "RECEIVING_REC"
    RECEIVING_PRGR = "RECEIVING_PRGR"


class PlayerGameMissesCols:
    """PlayerGameMissesCols."""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    PERFORMANCE_CRDY = "PERFORMANCE_CRDY"
    PERFORMANCE_CRDR = "PERFORMANCE_CRDR"
    PERFORMANCE_2CRDY = "PERFORMANCE_2CRDY"
    PERFORMANCE_FLS = "PERFORMANCE_FLS"
    PERFORMANCE_FLD = "PERFORMANCE_FLD"
    PERFORMANCE_OFF = "PERFORMANCE_OFF"
    PERFORMANCE_CRS = "PERFORMANCE_CRS"
    PERFORMANCE_TKLW = "PERFORMANCE_TKLW"
    PERFORMANCE_PKWON = "PERFORMANCE_PKWON"
    PERFORMANCE_PKCON = "PERFORMANCE_PKCON"
    PERFORMANCE_OG = "PERFORMANCE_OG"
    PERFORMANCE_RECOV = "PERFORMANCE_RECOV"
    AERIAL_DUELS_WON = "AERIAL DUELS_WON"
    AERIAL_DUELS_LOST = "AERIAL DUELS_LOST"
    AERIAL_DUELS_WON_PERC = "AERIAL DUELS_WON%"


class PlayerGameGoalkeeperCols:
    """PlayerGameGoalkeeperCols."""

    PLAYER = "PLAYER"
    ID_GAME = "ID_GAME"
    SHOT_STOPPING_SOTA = "SHOT STOPPING_SOTA"
    SHOT_STOPPING_GA = "SHOT STOPPING_GA"
    SHOT_STOPPING_SAVES = "SHOT STOPPING_SAVES"
    SHOT_STOPPING_SAVE_PERC = "SHOT STOPPING_SAVE%"
    SHOT_STOPPING_PSXG = "SHOT STOPPING_PSXG"
    LAUNCHED_CMP = "LAUNCHED_CMP"
    LAUNCHED_ATT = "LAUNCHED_ATT"
    LAUNCHED_CMP_PERC = "LAUNCHED_CMP%"
    PASSES_THR = "PASSES_THR"
    PASSES_LAUNCH_PERC = "PASSES_LAUNCH%"
    PASSES_AVGLEN = "PASSES_AVGLEN"
    GOAL_KICKS_ATT = "GOAL KICKS_ATT"
    GOAL_KICKS_LAUNCH_PERC = "GOAL KICKS_LAUNCH%"
    GOAL_KICKS_AVGLEN = "GOAL KICKS_AVGLEN"
    CROSSES_OPP = "CROSSES_OPP"
    CROSSES_STP = "CROSSES_STP"
    CROSSES_STP_PERC = "CROSSES_STP%"
    SWEEPER_OPA = "SWEEPER_#OPA"
    SWEEPER_AVGDIST = "SWEEPER_AVGDIST"


class TableDefinition:
    """Aggregate main SQL tables informations, among which the perimeter of the table,
    the query to create it if needed and of course the variables.
    """

    def __init__(
        self,
        perimeter: Perimeter,
        creation_query: str,
        wk_columns: Union[
            type[MatchCols],
            type[MatchColsEnhanced],
            type[PlayerGeneralCols],
            type[PlayerExtendCols],
            type[PlayerGameSummaryCols],
            type[PlayerGamePassingCols],
            type[PlayerGamePassingTypeCols],
            type[PlayerGameDefensiveCols],
            type[PlayerGamePossessionCols],
            type[PlayerGameMissesCols],
            type[PlayerGameGoalkeeperCols],
        ],
        primary_key: list[str],
    ):
        """Class instantiation.

        Args:
            perimeter (Perimeter): Says if table is related to the game itself or player statistics.
            creation_query (str): SQL request to create the table if not existing already.
            wk_columns (): Columns related to the SQL table.
            primary_key (list[str]): primary key(s) for a table.
        """
        self.perimeter = perimeter
        self.creation_query = creation_query
        self.wk_columns = wk_columns
        self.primary_key = primary_key

    def get_all_columns(self) -> list[str]:
        """As columns are declared in class attributes,  we use this function to list them.

        Returns:
            list[str]: Columns from the SQL table in the format used in the Python script.
        """
        return [
            getattr(self.wk_columns, attr)
            for attr in self.wk_columns.__dict__.keys()
            if not callable(getattr(self.wk_columns, attr)) and not attr.startswith("__")
        ]


# TableDefinition("GAME", TableCreations.GAME_DATA_TABLE, MatchCols).get_all_columns()


class TableMapping:
    """Class to map main SQL table informations in a dictionary that can
    be used anywhere on the script.
    """

    def __init__(self, enhanced: bool = False) -> None:
        """Class Instantiation."""
        self.TableConfig = {}

        if enhanced:
            self.TableConfig[Tables.GAME_DATA] = TableDefinition(
                Perimeter.GAME, TableCreations.GAME_DATA_TABLE, MatchColsEnhanced, ["ID_GAME", "TEAM"]
            )

        else:
            self.TableConfig[Tables.GAME_DATA] = TableDefinition(
                Perimeter.GAME, TableCreations.GAME_DATA_TABLE, MatchCols, ["ID_GAME", "TEAM"]
            )

        self.TableConfig[Tables.PLAYER_GAME_SUMMARY] = TableDefinition(
            Perimeter.PLAYER, TableCreations.PLAYER_GAME_SUMMARY_TABLE, PlayerGameSummaryCols, ["PLAYER", "ID_GAME"]
        )
        self.TableConfig[Tables.PLAYER_GENERAL_INFO] = TableDefinition(
            Perimeter.PLAYER, TableCreations.PLAYER_GENERAL_INFO_TABLE, PlayerGeneralCols, ["PLAYER"]
        )
        self.TableConfig[Tables.PLAYER_GAME_INFO_EXTEND] = TableDefinition(
            Perimeter.PLAYER,
            TableCreations.PLAYER_GAME_INFO_EXTEND_TABLE,
            PlayerExtendCols,
            ["PLAYER", "TEAM", "ID_GAME"],
        )
        self.TableConfig[Tables.PLAYER_GAME_PASSING_INFO] = TableDefinition(
            Perimeter.PLAYER,
            TableCreations.PLAYER_GAME_PASSING_INFO_TABLE,
            PlayerGamePassingCols,
            ["PLAYER", "ID_GAME"],
        )

        self.TableConfig[Tables.PLAYER_GAME_PASSING_TYPE_INFO] = TableDefinition(
            Perimeter.PLAYER,
            TableCreations.PLAYER_GAME_PASSING_TYPE_INFO_TABLE,
            PlayerGamePassingTypeCols,
            ["PLAYER", "ID_GAME"],
        )

        self.TableConfig[Tables.PLAYER_GAME_DEFENSIVE_INFO] = TableDefinition(
            Perimeter.PLAYER,
            TableCreations.PLAYER_GAME_DEFENSIVE_INFO_TABLE,
            PlayerGameDefensiveCols,
            ["PLAYER", "ID_GAME"],
        )

        self.TableConfig[Tables.PLAYER_GAME_POSSESSION_INFO] = TableDefinition(
            Perimeter.PLAYER,
            TableCreations.PLAYER_GAME_POSSESSION_INFO_TABLE,
            PlayerGamePossessionCols,
            ["PLAYER", "ID_GAME"],
        )

        self.TableConfig[Tables.PLAYER_GAME_MISSES_INFO] = TableDefinition(
            Perimeter.PLAYER, TableCreations.PLAYER_GAME_MISSES_INFO_TABLE, PlayerGameMissesCols, ["PLAYER", "ID_GAME"]
        )

        self.TableConfig[Tables.PLAYER_GAME_GOALKEEPER_INFO] = TableDefinition(
            Perimeter.PLAYER,
            TableCreations.PLAYER_GAME_GOALKEEPER_INFO_TABLE,
            PlayerGameGoalkeeperCols,
            ["PLAYER", "ID_GAME"],
        )

    def get_table_info(self, table: Tables) -> TableDefinition:
        """Get the TableDefinition object related to the SQL table we want.

        Args:
            table (Tables): SQL table

        Returns:
            TableDefinition: Dcitionary with main information (perimeter, creation query and columns)
        """
        return self.TableConfig[table]


# TableMapping().get_table_info(Tables.GAME_DATA).perimeter
# TableMapping().get_table_info(Tables.GAME_DATA).creation_query.value
# TableMapping().TableConfig[Tables.GAME_DATA].creation_query
