"""Repository for episodes."""
import logging
from typing import Any

from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, Mapper
from sqlalchemy.orm.properties import ColumnProperty
from tvbingefriend_tvmaze_models.models.episode import Episode


# noinspection PyMethodMayBeStatic
class EpisodeRepository:
    """Repository for episodes."""
    def upsert_episode(self, episode: dict[str, Any], db: Session) -> None:
        """Upsert an episode in the database

        Args:
            episode (dict[str, Any]): Episode to upsert
            db (Session): Database session
        """
        logging.info(
            msg="EpisodeRepository.upsert_episode: Upserting episode"
        )

        show_id = episode.get('show_id')  # get show_id from episode
        logging.debug(
            msg=f"EpisodeRepository.upsert_episode: show_id: {show_id}"
        )

        episode_data: dict[str, Any] = episode.get('episode')  # get episode from episode

        if not show_id or not episode_data:  # if show_id or episode is missing, log error and return
            logging.error(
                msg="EpisodeRepository.upsert_episode: Episode must have both show_id and episode_data"
            )
            return

        episode_id: int | None = episode_data.get('id')  # get episode_id from episode
        logging.debug(
            msg=f"EpisodeRepository.upsert_episode: episode_id: {episode_id}"
        )

        if not episode_id:  # if episode_id is missing, log error and return
            logging.error(
                msg="EpisodeRepository.upsert_episode: Episode must have episode_id"
            )
            return

        logging.info(
            msg=f"EpisodeRepository.upsert_episode: Upserting episode ID {episode_id} for show ID {show_id}"
        )

        mapper: Mapper = inspect(Episode)  # get mapper for Episode
        episode_columns: set[str] = {  # get columns for Episode
            prop.key for prop in mapper.attrs.values() if isinstance(prop, ColumnProperty)
        }

        insert_values: dict[str, Any] = {  # create insert values
            key: value for key, value in episode_data.items() if key in episode_columns
        }
        insert_values['id'] = episode_id  # set id
        insert_values['show_id'] = show_id  # set show_id

        update_values: dict[str, Any] = {  # create update values
            key: value for key, value in insert_values.items() if key != 'id'
        }

        try:
            stmt: mysql_insert = mysql_insert(Episode).values(insert_values)  # create insert statement
            stmt = stmt.on_duplicate_key_update(**update_values)  # create duplicate key update statement

            db.execute(stmt)  # execute insert statement
            db.flush()  # flush changes

        except SQLAlchemyError as e:  # catch any SQLAchemy errors and log them
            logging.error(
                msg=f"Database error during upsert of episode_id {episode_id}: {e}"
            )
        except Exception as e:  # catch any other errors and log them
            logging.error(
                msg=f"Unexpected error during upsert of episode_id {episode_id}: {e}"
            )
