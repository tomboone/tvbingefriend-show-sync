"""Get show episodes from TV Maze"""
import json
import logging
from typing import Any

import azure.functions as func

from tvbingefriend_show_sync.config import (
    EPISODE_UPSERT_CONTAINER,
    STORAGE_CONNECTION_SETTING_NAME,
    TVMAZE_EPISODES_CONTAINER
)
from tvbingefriend_show_sync.services.episode_service import EpisodeService
from tvbingefriend_show_sync.utils import db_session_manager

bp = func.Blueprint()


@bp.function_name(name="stage_show_episodes_for_upsert")
@bp.blob_trigger(
    arg_name="stageshowepisodes",
    path=TVMAZE_EPISODES_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def stage_show_episodes_for_upsert(stageshowepisodes: func.InputStream) -> None:
    """Stage show episodes for upsert

    Args:
        stageshowepisodes (func.InputStream): Blob input stream
    """
    episode_data: dict[str, Any] = json.loads(stageshowepisodes.read())  # get episode data from blob

    episode_service: EpisodeService = EpisodeService()
    episode_service.stage_episodes(episode_data)  # stage episodes for upsert


@bp.function_name(name="upsert_episode")
@bp.blob_trigger(
    arg_name="upsertepisode",
    path=EPISODE_UPSERT_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def upsert_episode(upsertepisode: func.InputStream) -> None:
    """Upsert episode

    Args:
        upsertepisode (func.InputStream): Blob input stream
    """
    episode: dict[str, Any] = json.loads(upsertepisode.read())  # get episode from blob

    logging.info(f"upsert_episode: Processing blob {upsertepisode.name}")

    try:
        episode_service: EpisodeService = EpisodeService()  # create episode service
        with db_session_manager() as db:
            episode_service.upsert_episode(episode, db)  # upsert episode
        logging.info(f"upsert_episode: Successfully upserted episode from blob {upsertepisode.name}")
    except Exception as e:  # catch errors and log them
        logging.error(
            f"upsert_episode: Unhandled exception for blob {upsertepisode.name}. Error: {e}",
            exc_info=True
        )
        raise
