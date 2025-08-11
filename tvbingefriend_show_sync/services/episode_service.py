"""Service for TV episode-related operations."""
import logging
from typing import Any

from sqlalchemy.orm.session import Session

from tvbingefriend_show_sync.config import EPISODE_UPSERT_CONTAINER, STORAGE_CONNECTION_STRING

from tvbingefriend_show_sync.repositories.episode_repo import EpisodeRepository
from tvbingefriend_show_sync.services.storage_service import StorageService


class EpisodeService:
    """Service for TV episode-related operations."""
    def __init__(self, episode_repository: EpisodeRepository | None = None) -> None:
        self.episode_repository = episode_repository or EpisodeRepository()
        self.storage_service = StorageService(STORAGE_CONNECTION_STRING)

    # noinspection PyMethodMayBeStatic
    def stage_episodes(self, episode_data: dict[str, Any]) -> None:
        """Stage episodes for upsert

        Args:
            episode_data (dict[str, Any]): Episode data
        """
        logging.info(
            msg="EpisodeService.stage_episodes: Staging episodes for upsert"
        )

        show_id = episode_data.get('show_id')  # get show_id from episode_data
        logging.debug(
            msg=f"EpisodeService.stage_episodes: show_id: {show_id}"
        )

        episodes: list[dict[str, Any]] = episode_data.get('episodes', [])  # get episodes from episode_data

        if not show_id or not episodes:  # if show_id or episodes are missing, log error and return
            logging.error(
                msg="EpisodeService.stage_episodes: Error staging episodes: Show must have both show_id and episodes"
            )
            return

        logging.info(
            msg=f"EpisodeService.stage_episodes: Staging all episodes for show id {show_id}"
        )

        for episode in episodes:  # for each episode
            blob_data: dict[str, Any] = {  # create blob data
                'show_id': show_id,
                'episode': episode  # episode
            }
            episode_id = episode.get('id')  # get episode_id from episode
            logging.debug(
                msg=f"EpisodeService.stage_episodes: episode_id: {episode_id}"
            )

            blob_name = f"tv_show_{show_id}_episode_{episode_id}.json"
            logging.debug(
                msg=f"EpisodeService.stage_episodes: blob_name: {blob_name}"
            )

            logging.debug(
                msg=f"EpisodeService.stage_episodes: container_name: {EPISODE_UPSERT_CONTAINER}"
            )

            self.storage_service.upload_blob_data(  # upload episode to blob container
                container_name=EPISODE_UPSERT_CONTAINER,  # container name
                blob_name=blob_name,  # blob name
                data=blob_data  # data to upload
            )
            logging.info(
                msg=f"EpisodeService.stage_episodes: Staged episode {episode_id} for show id {show_id}"
            )
        logging.info(
            msg=f"EpisodeService.stage_episodes: Staged all episodes for show id {show_id}"
        )

    def upsert_episode(self, episode: dict[str, Any], db: Session) -> None:
        """Upsert an episode in the database

        Args:
            episode (dict[str, Any]): Episode to upsert
            db (Session): Database session
        """
        episode_id = episode.get('episode', {}).get('id')
        logging.info(
            msg=f"EpisodeService.upsert_episode: Upserting episode ID {episode_id}"
        )

        self.episode_repository.upsert_episode(episode, db)

        logging.info(
            msg=f"EpisodeService.upsert_episode: Upserted episode {episode_id}"
        )
