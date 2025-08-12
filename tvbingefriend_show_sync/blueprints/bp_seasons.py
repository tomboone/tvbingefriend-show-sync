"""Get show seasons from TV Maze"""
import json
import logging
from typing import Any

import azure.functions as func

from tvbingefriend_show_sync.config import (
    SEASON_UPSERT_CONTAINER,
    STORAGE_CONNECTION_SETTING_NAME,
    TVMAZE_SEASONS_CONTAINER
)
from tvbingefriend_show_sync.services.season_service import SeasonService
from tvbingefriend_show_sync.utils import db_session_manager

bp = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="stage_show_seasons_for_upsert")
@bp.blob_trigger(
    arg_name="stageshowseasons",
    path=TVMAZE_SEASONS_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def stage_show_seasons_for_upsert(stageshowseasons: func.InputStream) -> None:
    """Stage show seasons for upsert

    Args:
        stageshowseasons (func.InputStream): Blob input stream
    """
    logging.info(f"stage_show_seasons_for_upsert: Processing blob {stageshowseasons.name}.")
    try:
        season_data: dict[str, Any] = json.loads(stageshowseasons.read())  # get season data from blob
        show_id = season_data.get("show_id", "N/A")
        logging.info(f"stage_show_seasons_for_upsert: Staging seasons for show_id: {show_id} from {stageshowseasons.name}.")
        season_service: SeasonService = SeasonService()
        season_service.stage_seasons(season_data)  # stage seasons for upsert
        logging.info(f"stage_show_seasons_for_upsert: Successfully staged seasons for show_id: {show_id}.")
    except Exception as e:
        logging.error(
            f"stage_show_seasons_for_upsert: Unhandled exception for blob {stageshowseasons.name}. Error: {e}",
            exc_info=True
        )
        raise


@bp.function_name(name="upsert_season")
@bp.blob_trigger(
    arg_name="upsertseason",
    path=SEASON_UPSERT_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def upsert_season(upsertseason: func.InputStream) -> None:
    """Upsert season

    Args:
        upsertseason (func.InputStream): Blob input stream
    """
    logging.info(f"upsert_season: Processing blob {upsertseason.name}")

    try:
        season: dict[str, Any] = json.loads(upsertseason.read())  # get season from blob
        season_service: SeasonService = SeasonService()  # create season service
        with db_session_manager() as db:
            season_service.upsert_season(season, db)  # upsert season
        logging.info(f"upsert_season: Successfully upserted season from blob {upsertseason.name}")
    except Exception as e:
        logging.error(
            f"upsert_season: Unhandled exception for blob {upsertseason.name}. Error: {e}",
            exc_info=True
        )
        raise
