"""Service for TV season/episode-related operations."""
import logging
from typing import Any

import azure.functions as func
from tvbingefriend_tvmaze_client.tvmaze_api import TVMazeAPI

from tvbingefriend_show_sync.config import (
    STORAGE_CONNECTION_STRING,
    TVMAZE_SHOW_IDS_CONTAINER,
    TVMAZE_SEASONS_EPISODES_QUEUE,
    TVMAZE_SEASONS_EPISODES_CONTAINER
)
from tvbingefriend_show_sync.repositories.database import SessionLocal
from tvbingefriend_show_sync.services.episode_service import EpisodeService
from tvbingefriend_show_sync.services.season_service import SeasonService
from tvbingefriend_show_sync.services.show_service import ShowService
from tvbingefriend_show_sync.services.storage_service import StorageService


# noinspection PyMethodMayBeStatic
class SeasonsEpisodesService:
    """Service for TV season/episode-related operations."""
    def __init__(self) -> None:
        self.show_service = ShowService()
        self.season_service = SeasonService()
        self.episode_service = EpisodeService()
        self.storage_service = StorageService(STORAGE_CONNECTION_STRING)
        self.tvmaze_api = TVMazeAPI()

    def start_get_seasons_episodes(self) -> func.HttpResponse:
        """Starts the workflow by fetching all show IDs and staging them in a blob."""
        logging.info("SeasonsEpisodesService: Starting season/episode retrieval workflow.")
        db = SessionLocal()

        try:
            show_ids: list[int] | None = self.show_service.get_all_show_ids(db)
            if not show_ids:
                logging.warning("SeasonsEpisodesService: No show IDs found in the database.")
                return func.HttpResponse("No show IDs found to process.", status_code=200)

            blob_name = "all_show_ids.json"
            self.storage_service.upload_blob_data(
                container_name=TVMAZE_SHOW_IDS_CONTAINER,
                blob_name=blob_name,
                data=show_ids
            )
            logging.info(f"SeasonsEpisodesService: Staged {len(show_ids)} show IDs to blob '{blob_name}'.")
            return func.HttpResponse(f"Successfully started processing for {len(show_ids)} shows.", status_code=202)

        except Exception as e:  # catch any errors, log error, and return 500
            logging.error(f"SeasonsEpisodesService: Failed to start workflow: {e}", exc_info=True)
            return func.HttpResponse("Failed to start season/episode retrieval process.", status_code=500)
        finally:
            db.close()

    def stage_show_ids_for_retrieval(self, show_ids: list[int]) -> None:
        """Takes a list of show IDs and queues each one for individual retrieval."""
        logging.info(f"SeasonsEpisodesService: Queuing {len(show_ids)} show IDs for season/episode retrieval.")
        for show_id in show_ids:  # for each show id
            try:
                message = {'show_id': show_id}
                self.storage_service.upload_queue_message(

                    queue_name=TVMAZE_SEASONS_EPISODES_QUEUE,
                    message=message
                )

            except Exception as e:
                logging.error(
                    f"SeasonsEpisodesService: Failed to queue message for show ID {show_id}: {e}", exc_info=True
                )
        logging.info("SeasonsEpisodesService: Finished queuing all show IDs.")

    def get_show_seasons_episodes(self, msg: dict[str, Any]) -> None:
        """Fetches show details with embedded seasons/episodes and stages them in a blob."""
        show_id = msg.get("show_id")
        if not show_id:
            logging.error(f"SeasonsEpisodesService: Message is missing 'show_id'. Message content: {msg}")
            return

        logging.info(f"SeasonsEpisodesService: Getting seasons and episodes for show ID {show_id}")
        show_data = self.tvmaze_api.get_show_details(show_id=show_id, embed=['seasons', 'episodes'])

        if not show_data:
            logging.warning(f"SeasonsEpisodesService: No data returned from TVMaze API for show ID {show_id}")
            return
        self.storage_service.upload_blob_data(
            container_name=TVMAZE_SEASONS_EPISODES_CONTAINER,
            blob_name=f"tv_show_{show_id}.json",
            data=show_data
        )
        logging.info(f"SeasonsEpisodesService: Staged raw show data for show ID {show_id}")

    def stage_show_seasons_episodes(self, show_data: dict[str, Any]) -> None:
        """Extracts seasons and episodes from raw show data and delegates to the appropriate services for staging."""
        show_id = show_data.get('id')
        if not show_id:
            logging.error(f"SeasonsEpisodesService: Raw show data from blob is missing 'id'. Data: {show_data}")
            return

        logging.info(f"SeasonsEpisodesService: Staging seasons and episodes for show ID {show_id}")
        embedded_data = show_data.get('_embedded', {})

        # Delegate to SeasonService to stage seasons
        seasons = embedded_data.get('seasons', [])
        if seasons:
            seasons_payload = {'show_id': show_id, 'seasons': seasons}
            self.season_service.stage_seasons(seasons_payload)

        # Delegate to EpisodeService to stage episodes
        episodes = embedded_data.get('episodes', [])
        if episodes:
            episodes_payload = {'show_id': show_id, 'episodes': episodes}
            self.episode_service.stage_episodes(episodes_payload)
