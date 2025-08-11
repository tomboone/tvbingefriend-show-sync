"""Get shows from TV Maze"""
import json
import logging
from typing import Any

import azure.functions as func

from tvbingefriend_show_sync.config import (
    SHOW_STAGE_CONTAINER,
    SHOW_UPSERT_CONTAINER,
    STORAGE_CONNECTION_SETTING_NAME,
    TVMAZE_SHOWS_QUEUE
)
from tvbingefriend_show_sync.services.show_service import ShowService
from tvbingefriend_show_sync.utils import db_session_manager

bp = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="start_get_shows")
@bp.route(route="start_get_shows", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def start_get_shows(req: func.HttpRequest) -> func.HttpResponse:
    """Start get all shows from TV Maze

    An optional 'page' query parameter can be provided to start from a specific page.

    Args:
        req (func.HttpRequest): Request object

    Returns:
        func.HttpResponse: Response object
    """
    page: int = 0
    page_str: str | None = req.params.get('page')
    if page_str:
        try:
            page = int(page_str)
            if page < 0:
                logging.error(f"Invalid page number provided: {page}")
                return func.HttpResponse(
                    "Query parameter 'page' must be a non-negative integer.",
                    status_code=400
                )
        except ValueError:
            logging.error(f"Invalid page parameter provided: {page_str}")
            return func.HttpResponse(
                "Query parameter 'page' must be an integer.",
                status_code=400
            )
    show_service: ShowService = ShowService()  # create show service
    show_service.start_get_shows(page=page)  # initiate retrieval of all shows

    message = f"Getting all shows from TV Maze, starting from page {page}"
    return func.HttpResponse(message, status_code=202)


@bp.function_name(name="get_show_page")
@bp.queue_trigger(
    arg_name="getshowsmsg",
    queue_name=TVMAZE_SHOWS_QUEUE,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def get_show_page(getshowsmsg: func.QueueMessage) -> None:
    """Get one page of shows from TV Maze

    Args:
        getshowsmsg (func.QueueMessage): Queue message
    """
    logging.info(
        f"get_show_page: Processing queue message ID: {getshowsmsg.id}, "
        f"DequeueCount: {getshowsmsg.dequeue_count}"
    )
    try:
        message: dict[str, Any] = getshowsmsg.get_json()  # get message from queue
        show_service: ShowService = ShowService()  # create show service
        show_service.get_show_page(message)   # get show page
        logging.info(f"get_show_page: Successfully processed queue message ID: {getshowsmsg.id}")
    except Exception as e:
        logging.error(
            f"get_show_page: Unhandled exception for message ID {getshowsmsg.id}. Error: {e}",
            exc_info=True
        )
        raise


@bp.function_name(name="stage_shows_for_upsert")
@bp.blob_trigger(
    arg_name="stageblob",
    path=SHOW_STAGE_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME,
)
def stage_shows_for_upsert(stageblob: func.InputStream) -> None:
    """Stage one page of shows for upsert

    Args:
        stageblob (func.InputStream): Blob input stream
    """
    shows: list[dict[str, Any]] = json.loads(stageblob.read())  # get shows from blob

    show_service: ShowService = ShowService()  # create show service
    show_service.stage_shows_for_upsert(shows)  # stage shows for upsert


@bp.function_name(name="upsert_show")
@bp.blob_trigger(
    arg_name="upsertblob",
    path=SHOW_UPSERT_CONTAINER,
    connection=STORAGE_CONNECTION_SETTING_NAME,
)
def upsert_show(upsertblob: func.InputStream) -> None:
    """Upsert shows

    Args:
        upsertblob (func.InputStream): Blob input stream
    """
    show: dict[str, Any] = json.loads(upsertblob.read())  # get show data from blob

    show_service: ShowService = ShowService()  # create show service

    try:
        with db_session_manager() as db:
            show_service.upsert_show(show, db)  # upsert show
    except Exception as e:  # catch errors and log them
        logging.error(
            msg=f"bp_shows.upsert_show: Error upserting show: {e}"
        )
