"""Service for TV show-related operations."""
import logging
from typing import Literal, Any

from tvbingefriend_show_sync.config import (
    STORAGE_CONNECTION_STRING,
    TVMAZE_SEASONS_EPISODES_QUEUE,
    TVMAZE_SEASONS_EPISODES_UPDATE_TABLE,
    TVMAZE_SHOWS_UPDATE_QUEUE,
    TVMAZE_UPDATES_CONTAINER, SHOW_UPSERT_CONTAINER
)
from tvbingefriend_show_sync.services.storage_service import StorageService
from tvbingefriend_tvmaze_client.tvmaze_api import TVMazeAPI


class UpdateService:
    """Service for updating shows from TV Maze"""
    def __init__(self) -> None:
        self.storage_service = StorageService(STORAGE_CONNECTION_STRING)
        self.tvmaze_api = TVMazeAPI()

    def get_updates(self, since: Literal['day', 'week', 'month'] | str = "day") -> None:
        """Get updates from TV Maze"""
        logging.debug(f"UpdateService.get_updates: since: {since}")

        try:
            logging.info(f"UpdateService.get_updates: Getting updates from TV Maze for since: {since}")
            updates: dict[str, Any] = self.tvmaze_api.get_show_updates(period=since)  # get updates from TV Maze API
        except Exception as e:  # catch any errors, log error, and return
            logging.error(f"Error getting updates from TV Maze: {e}")
            return

        if updates:  # if updates are returned
            blob_name: str = f"updates_{since}.json"  # set blob name

            logging.debug(f"UpdateService.get_updates: container_name: {TVMAZE_UPDATES_CONTAINER}")
            logging.debug(f"UpdateService.get_updates: Blob name: {blob_name}")

            self.storage_service.upload_blob_data(  # upload updates to blob storage
                container_name=TVMAZE_UPDATES_CONTAINER,  # container name
                blob_name=blob_name,  # blob name
                data=updates  # data to upload
            )

            logging.info(f"Staged all updates for {since} in blob {blob_name}")

    def stage_updates_for_upsert(self, updates: dict[str, Any]) -> None:
        """Stage updates for upsert

        Args:
            updates (dict[str, Any]): Updates to stage
        """
        logging.info("UpdateService.stage_updates_for_upsert: Staging updates for upsert")

        for show_id, last_updated in updates.items():  # for each update
            logging.debug(f"UpdateService.stage_updates_for_upsert: show_id: {show_id}")

            msg: dict[str, Any] = {  # create message to retrieve show
                "show_id": int(show_id)  # show_id
            }

            self.storage_service.upload_queue_message(  # upload queue message to trigger retrieval
                queue_name=TVMAZE_SHOWS_UPDATE_QUEUE,  # queue name
                message=msg  # message to upload
            )

            entity: dict[str, Any] = {
                "PartitionKey": "show",
                "RowKey": str(show_id),
                "LastUpdated": last_updated
            }
            self.storage_service.upsert_entity(  # upsert entity in table for later season/episode retrieval
                table_name=TVMAZE_SEASONS_EPISODES_UPDATE_TABLE,  # table name
                entity=entity  # entity to upsert
            )

    def get_show_update_details(self, show_id: int):
        """Update a show from TV Maze

        Args:
            show_id (int): ID of the show to update
        """
        logging.info(f"UpdateService.get_show_update_details: Updating show {show_id} from TV Maze")

        show: dict[str, Any] = self.tvmaze_api.get_show_details(show_id)

        if show:
            blob_name: str = f"tv_show_{show_id}.json"

            logging.debug(f"UpdateService.get_show_update_details: container_name: {SHOW_UPSERT_CONTAINER}")
            logging.debug(f"UpdateService.get_show_update_details: Blob name: {blob_name}")

            self.storage_service.upload_blob_data(  # upload show for upsert
                container_name=SHOW_UPSERT_CONTAINER,  # container name
                blob_name=blob_name,  # blob name
                data=show  # data to upload
            )

    def update_seasons_episodes(self) -> None:
        """Update seasons and episodes from TV Maze"""
        logging.info("UpdateService.update_seasons_episodes: Updating seasons and episodes from TV Maze")

        staged_shows: list[dict[str, Any]] = self.storage_service.get_entities(TVMAZE_SEASONS_EPISODES_UPDATE_TABLE)
        if not staged_shows:
            logging.info("UpdateService.update_seasons_episodes: No shows staged for season/episode updates.")
            return

        show_ids_to_process: list[str] = [entity['RowKey'] for entity in staged_shows]

        for show_id in show_ids_to_process:
            msg: dict[str, Any] = {
                "show_id": int(show_id)
            }
            logging.debug(f"UpdateService.update_seasons_episodes: Queuing show {show_id} for season/episode update.")
            self.storage_service.upload_queue_message(
                queue_name=TVMAZE_SEASONS_EPISODES_QUEUE,
                message=msg
            )

        logging.info(
            f"UpdateService.update_seasons_episodes: Deleting {len(staged_shows)} processed entities from the "
            f"update table."
        )
        self.storage_service.delete_entities_batch(
            table_name=TVMAZE_SEASONS_EPISODES_UPDATE_TABLE,
            entities=staged_shows
        )
