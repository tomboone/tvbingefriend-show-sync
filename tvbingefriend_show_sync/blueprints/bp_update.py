"""Update shows from TV Maze"""
import json
import logging
from typing import Any

import azure.functions as func

from tvbingefriend_show_sync.config import (
    STORAGE_CONNECTION_SETTING_NAME,
    TVMAZE_UPDATES_CONTAINER,
    UPDATE_SEASONS_EPISODES_NCRON,
    UPDATE_SHOWS_NCRON, TVMAZE_SHOWS_UPDATE_QUEUE
)
from tvbingefriend_show_sync.services.update_service import UpdateService

bp = func.Blueprint()


@bp.function_name(name="get_updates_manually")
@bp.route(route="update_shows_manually", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def get_updates_manually(req: func.HttpRequest) -> func.HttpResponse:
    """Update shows manually

    An optional 'since' query parameter can be provided to filter updates to a
    specified period (e.g., day, week, or month)

    Args:
        req (func.HttpRequest): Request object
    Returns:
        func.HttpResponse: Response object
    """
    # Get 'since' param, default to 'day' if not present in the query string.
    since: str = req.params.get('since', 'day')

    if since not in ('day', 'week', 'month'):  # if invalid, log error and return
        logging.error(f"Invalid since parameter provided: {since}")
        return func.HttpResponse(
            "Query parameter 'since' must be 'day', 'week', or 'month'.",
            status_code=400
        )

    update_service: UpdateService = UpdateService()  # create update service
    update_service.get_updates(since)  # update shows manually

    message = f"Getting all updates from TV Maze for the last {since}"

    return func.HttpResponse(message, status_code=202)


# noinspection PyUnusedLocal
@bp.function_name(name="get_updates_timer")
@bp.timer_trigger(
    arg_name="updateshows",
    schedule=UPDATE_SHOWS_NCRON,
    run_on_startup=False
)
def get_updates_timer(updateshows: func.TimerRequest) -> None:
    """Update shows from TV Maze"""
    update_service: UpdateService = UpdateService()  # create update service
    update_service.get_updates()


# noinspection PyUnusedLocal
@bp.function_name(name="update_seasons_episodes")
@bp.timer_trigger(
    arg_name="updateseasonsepisodes",
    schedule=UPDATE_SEASONS_EPISODES_NCRON,
    run_on_startup=False
)
def update_seasons_episodes(updateseasonsepisodes: func.TimerRequest) -> None:
    """Update seasons and episodes from TV Maze"""
    update_service: UpdateService = UpdateService()  # create update service
    update_service.update_seasons_episodes()


@bp.function_name(name="get_show_update_details")
@bp.queue_trigger(
    arg_name="updateshowmsg",
    queue_name=TVMAZE_SHOWS_UPDATE_QUEUE,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def get_show_update_details(updateshowmsg: func.QueueMessage) -> None:
    """Get show update details

    Args:
        updateshowmsg (func.QueueMessage): Queue message
    """
    message: dict[str, Any] = updateshowmsg.get_json()  # get message from queue
    show_id = message.get("show_id")

    if not show_id:
        logging.error("Received show update message without a 'show_id'. Aborting.")
        return

    update_service: UpdateService = UpdateService()  # create update service
    update_service.get_show_update_details(show_id)  # get show update details


@bp.function_name(name="stage_season_episode_updates_for_upsert")
@bp.blob_trigger(
    arg_name="stageblob",
    path=TVMAZE_UPDATES_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME,
)
def stage_season_episode_updates_for_upsert(stageblob: func.InputStream) -> None:
    """Stage season/episode updates for upsert

    Args:
        stageblob (func.InputStream): Blob input stream
    """
    updates: dict[str, Any] = json.loads(stageblob.read())  # get updates from blob
    update_service: UpdateService = UpdateService()  # create update service
    update_service.stage_updates_for_upsert(updates)  # stage updates for upsert
