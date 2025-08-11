"""Service for TV season-related operations."""
import logging
from typing import Any

from sqlalchemy.orm.session import Session

from tvbingefriend_show_sync.config import STORAGE_CONNECTION_STRING, SEASON_UPSERT_CONTAINER
from tvbingefriend_show_sync.repositories.season_repo import SeasonRepository
from tvbingefriend_show_sync.services.storage_service import StorageService


# noinspection PyMethodMayBeStatic
class SeasonService:
    """Service for TV season-related operations."""
    def __init__(self, season_repository: SeasonRepository | None = None) -> None:
        self.season_repository = season_repository or SeasonRepository()
        self.storage_service = StorageService(STORAGE_CONNECTION_STRING)

    def stage_seasons(self, season_data: dict[str, Any]):
        """Stage seasons for upsert

        Args:
            season_data (dict[str, Any]): Season data
        """
        logging.info(
            msg="SeasonService.stage_seasons: Staging seasons for upsert"
        )

        show_id = season_data.get('show_id')  # get show_id from season_data
        logging.debug(
            msg=f"SeasonService.stage_seasons: show_id: {show_id}"
        )

        seasons: list[dict[str, Any]] = season_data.get('seasons', [])  # get seasons from season_data

        if not show_id or not seasons:  # if show_id or seasons are missing, log error and return
            logging.error(
                msg="SeasonService.stage_seasons: Error staging seasons: Show must have both show_id and seasons"
            )
            return

        logging.info(
            msg=f"SeasonService.stage_seasons: Staging all seasons for show id {show_id}"
        )

        logging.debug(
            msg=f"SeasonService.stage_seasons: STORAGE_CONNECTION_STRING: {STORAGE_CONNECTION_STRING}"
        )

        for season in seasons:  # for each season
            blob_data: dict[str, Any] = {  # create blob data
                'show_id': show_id,  # show_id
                'season': season  # season
            }
            season_id = season.get('id')  # get season_id from season
            logging.debug(
                msg=f"SeasonService.stage_seasons: season_id: {season_id}"
            )

            blob_name = f"tv_show_{show_id}_season_{season_id}.json"  # set blob name
            logging.debug(
                msg=f"SeasonService.stage_seasons: blob_name: {blob_name}"
            )

            logging.debug(
                msg=f"SeasonService.stage_seasons: container_name: {SEASON_UPSERT_CONTAINER}"
            )

            self.storage_service.upload_blob_data(  # upload season to blob container
                container_name=SEASON_UPSERT_CONTAINER,  # container name
                blob_name=blob_name,  # blob name
                data=blob_data  # data to upload
            )
            logging.info(
                msg=f"SeasonService.stage_seasons: Staged season {season_id} for show id {show_id}"
            )

        logging.info(
            msg=f"SeasonService.stage_seasons: Staged all seasons for show id {show_id}"
        )

    def upsert_season(self, season: dict[str, Any], db: Session) -> None:
        """Upsert a season in the database

        Args:
            season (dict[str, Any]): Season to upsert
            db (Session): Database session
        """
        season_id = season.get('season', {}).get('id')
        logging.info(
            msg=f"SeasonService.upsert_season: Upserting season ID {season_id}"
        )

        self.season_repository.upsert_season(season, db)  # upsert season

        logging.info(
            msg=f"SeasonService.upsert_season: Upserted season {season_id}"
        )

