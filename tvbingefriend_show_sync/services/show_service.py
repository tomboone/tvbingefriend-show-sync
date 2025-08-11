"""Service for TV show-related operations."""
import logging
from typing import Any
from requests.exceptions import HTTPError

from sqlalchemy.orm import Session

from tvbingefriend_show_sync.config import STORAGE_CONNECTION_STRING, TVMAZE_SHOWS_QUEUE, SHOW_STAGE_CONTAINER, \
    SHOW_UPSERT_CONTAINER
from tvbingefriend_show_sync.repositories.show_repo import ShowRepository
from tvbingefriend_show_sync.services.storage_service import StorageService
from tvbingefriend_tvmaze_client.tvmaze_api import TVMazeAPI


# noinspection PyMethodMayBeStatic
class ShowService:
    """Service for TV show-related operations."""
    def __init__(self, show_repository: ShowRepository | None = None) -> None:
        self.show_repository = show_repository or ShowRepository()
        self.storage_service = StorageService(STORAGE_CONNECTION_STRING)
        self.tvmaze_api = TVMazeAPI()

    def start_get_shows(self, page: int = 0) -> None:
        """Start get all shows from TV Maze

        Args:
            page (int): Page number to start from. Defaults to 0.
        """
        logging.info(f"ShowService.start_get_shows: Starting show retrieval from page {page}")

        msg: dict[str, Any] = {  # create message to retrieve first page of shows
            "page": page  # page number
        }
        logging.debug(f"ShowService.start_get_shows: message: {msg}")

        self.storage_service.upload_queue_message(  # upload message to queue
            queue_name=TVMAZE_SHOWS_QUEUE,  # queue name
            message=msg  # message to upload
        )
        logging.info(f"ShowService.start_get_shows: Queued page {page} for retrieval")

    def get_show_page(self, message: dict[str, Any]):
        """Get one page of shows from TV Maze

        Args:
            message (dict[str, Any]): Message containing page number
        """
        page_number = message.get("page")
        logging.debug(f"ShowService.get_show_page: page_number: {page_number}")

        if page_number is None:
            logging.error(f"Queue message is missing 'page' number.")
            return

        logging.info(f"ShowService.get_show_page: Getting shows from TV Maze for page_number: {page_number}")
        shows: list[dict[str, Any]] | None = self.tvmaze_api.get_shows(page_number)  # get shows from TV Maze API

        if shows:  # if shows are returned
            blob_name = f"shows_page_{page_number}.json"

            logging.debug(f"ShowService.get_show_page: container_name: {SHOW_STAGE_CONTAINER}")
            logging.debug(f"ShowService.get_show_page: Blob name: {blob_name}")

            self.storage_service.upload_blob_data(  # upload shows to blob storage
                container_name=SHOW_STAGE_CONTAINER,  # container name
                blob_name=blob_name,  # blob name
                data=shows  # data to upload
            )
            logging.info(f"Staged all shows from page {page_number} for upsert in blob {blob_name}")

            message["page"] += 1  # increment page number

            self.storage_service.upload_queue_message(  # upload next page of shows to queue
                queue_name=TVMAZE_SHOWS_QUEUE,  # queue name
                message=message  # message to upload
            )
            logging.info(f"Queued page {message['page']} for retrieval")

    def stage_shows_for_upsert(self, shows: list[dict[str, Any]]):
        """Stage shows for upsert

        Args:
            shows (list[dict[str, Any]]): Shows to stage
        """

        logging.info(f"ShowService.stage_shows_for_upsert: Staging shows for upsert")

        for show in shows:  # for each show
            tvmaze_id = show.get("id")  # get TV Maze id
            logging.debug(f"ShowService.stage_shows_for_upsert: tvmaze_id: {tvmaze_id}")

            blob_name = f"tv_show_{tvmaze_id}.json"
            logging.debug(f"ShowService.stage_shows_for_upsert: blob_name: {blob_name}")
            logging.debug(f"ShowService.stage_shows_for_upsert: container_name: {SHOW_UPSERT_CONTAINER}")

            self.storage_service.upload_blob_data(  # upload show to blob storage
                container_name=SHOW_UPSERT_CONTAINER,  # container name
                blob_name=blob_name,  # blob name
                data=show  # data to upload
            )
            logging.info(f"Staged show {tvmaze_id} for upsert")

    def get_all_show_ids(self, db: Session):
        """Get all show ids"""
        logging.info("ShowService.get_all_show_ids: Get all show ids")
        show_ids = self.show_repository.get_all_show_ids(db=db)
        return show_ids

    def upsert_show(self, show: dict[str, Any], db: Session) -> None:
        """Upsert a show in the database

        Args:
            show (dict[str, Any]): Show to upsert
            db (Session): Database session
        """
        logging.info("ShowService.upsert_show: Upserting show")
        self.show_repository.upsert_show(show, db)
