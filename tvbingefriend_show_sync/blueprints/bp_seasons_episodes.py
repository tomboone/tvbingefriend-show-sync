"""Get seasons from TV Maze"""
import json
import logging
from typing import Any, List

import azure.functions as func

from tvbingefriend_show_sync.config import (
    STORAGE_CONNECTION_SETTING_NAME,
    TVMAZE_SEASONS_EPISODES_CONTAINER,
    TVMAZE_SEASONS_EPISODES_QUEUE,
    TVMAZE_SHOW_IDS_CONTAINER
)
from tvbingefriend_show_sync.services.seasons_episodes_service import SeasonsEpisodesService


bp = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="start_get_seasons_episodes")
@bp.route(route="start_get_seasons", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def start_get_seasons_episodes(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered function to start the season/episode retrieval workflow."""
    logging.info("start_get_seasons_episodes: HTTP trigger function processed a request.")
    seasons_episodes_service = SeasonsEpisodesService()
    return seasons_episodes_service.start_get_seasons_episodes()


@bp.function_name(name="stage_show_ids_for_retrieval")
@bp.blob_trigger(
    arg_name="stageshowidsblob",
    path=TVMAZE_SHOW_IDS_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def stage_show_ids_for_retrieval(stageshowidsblob: func.InputStream) -> None:
    """Blob-triggered function to queue up individual show IDs for processing."""
    logging.info(f"stage_show_ids_for_retrieval: Processing blob {stageshowidsblob.name}.")
    try:
        show_ids: List[int] = json.loads(stageshowidsblob.read())
        logging.info(
            f"stage_show_ids_for_retrieval: Staging {len(show_ids)} show IDs from blob {stageshowidsblob.name}."
        )
        seasons_episodes_service = SeasonsEpisodesService()
        seasons_episodes_service.stage_show_ids_for_retrieval(show_ids)
        logging.info(f"stage_show_ids_for_retrieval: Successfully staged show IDs from blob {stageshowidsblob.name}.")
    except Exception as e:
        logging.error(
            f"stage_show_ids_for_retrieval: Unhandled exception for blob {stageshowidsblob.name}. Error: {e}",
            exc_info=True
        )
        raise


@bp.function_name("get_show_seasons_episodes")
@bp.queue_trigger(
    arg_name="getshowseasonsepisodes",
    queue_name=TVMAZE_SEASONS_EPISODES_QUEUE,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def get_show_seasons_episodes(getshowseasonsepisodes: func.QueueMessage) -> None:
    """Queue-triggered function to fetch seasons/episodes for a single show."""
    logging.info(
        f"get_show_seasons_episodes: Processing queue message ID: {getshowseasonsepisodes.id}, "
        f"DequeueCount: {getshowseasonsepisodes.dequeue_count}"
    )
    try:
        msg: dict[str, Any] = getshowseasonsepisodes.get_json()
        show_id = msg.get("show_id", "N/A")
        logging.info(f"get_show_seasons_episodes: Fetching seasons/episodes for show_id: {show_id}")
        seasons_episodes_service = SeasonsEpisodesService()
        seasons_episodes_service.get_show_seasons_episodes(msg)
        logging.info(f"get_show_seasons_episodes: Successfully processed show_id: {show_id}")
    except Exception as e:
        logging.error(
            f"get_show_seasons_episodes: Unhandled exception for message ID {getshowseasonsepisodes.id}. Error: {e}",
            exc_info=True
        )
        raise


@bp.function_name("stage_show_seasons_episodes")
@bp.blob_trigger(
    arg_name="stageshowseasonsepisodes",
    path=TVMAZE_SEASONS_EPISODES_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def stage_show_seasons_episodes(stageshowseasonsepisodes: func.InputStream) -> None:
    """Blob-triggered function to process a show's raw data and stage its seasons and episodes."""
    logging.info(f"stage_show_seasons_episodes: Processing blob {stageshowseasonsepisodes.name}.")
    try:
        show_data: dict[str, Any] = json.loads(stageshowseasonsepisodes.read())
        show_id = show_data.get("id", "N/A")
        logging.info(
            f"stage_show_seasons_episodes: Staging seasons/episodes for show_id: {show_id} "
            f"from blob {stageshowseasonsepisodes.name}."
        )
        seasons_episodes_service: SeasonsEpisodesService = SeasonsEpisodesService()
        seasons_episodes_service.stage_show_seasons_episodes(show_data)
        logging.info(f"stage_show_seasons_episodes: Successfully staged seasons/episodes for show_id: {show_id}.")
    except Exception as e:
        logging.error(
            f"stage_show_seasons_episodes: Unhandled exception for blob {stageshowseasonsepisodes.name}. Error: {e}",
            exc_info=True
        )
        raise
