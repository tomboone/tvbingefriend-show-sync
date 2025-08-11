"""Configuration for tvbingefriend_show_sync"""
import os


def _get_required_env(var_name: str) -> str:
    """Gets a required environment variable or raises a ValueError."""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Missing required environment variable: '{var_name}'")
    return value


# App Setting Keys (for use in function binding decorators)
STORAGE_CONNECTION_SETTING_NAME = "AzureWebJobsStorage"

# Connection Strings
SQLALCHEMY_CONNECTION_STRING = _get_required_env("SQLALCHEMY_CONNECTION_STRING")
STORAGE_CONNECTION_STRING = _get_required_env(STORAGE_CONNECTION_SETTING_NAME)

# Show storage
TVMAZE_SHOWS_QUEUE = os.getenv("TVMAZE_SHOWS_QUEUE", "tvshowsqueue")
SHOW_STAGE_CONTAINER = os.getenv("SHOW_STAGE_CONTAINER", "showstagecontainer")
SHOW_UPSERT_CONTAINER = os.getenv("SHOW_UPSERT_CONTAINER", "showupsertcontainer")

# Show seasons & episodes storage
TVMAZE_SHOW_IDS_CONTAINER = os.getenv("TVMAZE_SHOW_IDS_CONTAINER", "tvshowidscontainer")
TVMAZE_SEASONS_EPISODES_CONTAINER = os.getenv("TVMAZE_SEASONS_EPISODES_CONTAINER", "tvseasonsepisodescontainer")
TVMAZE_SEASONS_EPISODES_QUEUE = os.getenv("TVMAZE_SEASONS_EPISODES_QUEUE", "tvseasonsepisodesqueue")

# Season storage
TVMAZE_SEASONS_CONTAINER = os.getenv("TVMAZE_SEASONS_CONTAINER", "tvseasonscontainer")
SEASON_STAGE_CONTAINER = os.getenv("SEASON_STAGE_CONTAINER", "seasonstagecontainer")
SEASON_UPSERT_CONTAINER = os.getenv("SEASON_UPSERT_CONTAINER", "seasonupsertcontainer")

# Episode storage
TVMAZE_EPISODES_CONTAINER = os.getenv("TVMAZE_EPISODES_CONTAINER", "tvepisodescontainer")
EPISODE_STAGE_CONTAINER = os.getenv("EPISODE_STAGE_CONTAINER", "episodestagecontainer")
EPISODE_UPSERT_CONTAINER = os.getenv("EPISODE_UPSERT_CONTAINER", "episodeupsertcontainer")

# Update storage
TVMAZE_UPDATES_CONTAINER = os.getenv("TVMAZE_UPDATES_CONTAINER", "tvupdates")
TVMAZE_SHOWS_UPDATE_QUEUE = os.getenv("TVMAZE_SHOWS_UPDATE_QUEUE", "tvshowsupdatequeue")
TVMAZE_SEASONS_EPISODES_UPDATE_TABLE = os.getenv(
    "TVMAZE_SEASONS_EPISODES_UPDATE_TABLE",
    "tvseasonsepisodesupdatetable"
)

# Update schedule
UPDATE_SHOWS_NCRON = _get_required_env("UPDATE_SHOWS_NCRON")
UPDATE_SEASONS_EPISODES_NCRON = _get_required_env("UPDATE_SEASONS_EPISODES_NCRON")
