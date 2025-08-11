"""Repository for shows"""
import logging
from typing import Any

from sqlalchemy import inspect, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine.result import Result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, Mapper, ColumnProperty

from tvbingefriend_tvmaze_models.models.show import Show


# noinspection PyMethodMayBeStatic
class ShowRepository:
    """Repository for shows"""
    def get_all_show_ids(self, db: Session) -> list[int] | None:
        """Get all show ids"""
        try:
            stmt: select = select(Show.id)  # create select statement
            logging.debug(f"ShowRepository.get_all_show_ids: stmt: {stmt}")

            result: Result[tuple[int]] = db.execute(stmt)  # execute select statement

            show_ids = [row[0] for row in result]  # get show ids from result

            return show_ids

        except SQLAlchemyError as e:  # catch any SQLAchemy errors, log them, and return None
            logging.error(f"ShowRepository.get_all_show_ids: Database error during get_all_show_ids: {e}")
            return None
        except Exception as e:  # catch any other errors, log them, and return None
            logging.error(f"ShowRepository.get_all_show_ids: Unexpected error during get_all_show_ids: {e}")
            return None

    def upsert_show(self, show: dict[str, Any], db: Session) -> None:
        """Upsert a show in the database

        Args:
            show (dict[str, Any]): Show to upsert
            db (Session): Database session
        """
        show_id: int | None = show.get("id")  # get show_id from show
        logging.debug(f"ShowRepository.upsert_show: show_id: {show_id}")

        if not show_id:  # if show_id is missing, log error and return
            logging.error(f"show_repository.upsert_show: Error upserting show: Show must have a show_id")
            return

        mapper: Mapper = inspect(Show)  # get show mapper
        show_columns: set[str] = {  # get show columns
            prop.key for prop in mapper.attrs.values() if isinstance(prop, ColumnProperty)
        }

        insert_values: dict[str, Any] = {  # create insert values
            key: value for key, value in show.items() if key in show_columns
        }
        insert_values["id"] = show_id  # add id value to insert values

        update_values: dict[str, Any] = {  # create update values
            key: value for key, value in insert_values.items() if key != "id"
        }

        try:

            stmt: mysql_insert = mysql_insert(Show).values(insert_values)  # create insert statement
            stmt = stmt.on_duplicate_key_update(**update_values)  # add duplicate key update statement

            db.execute(stmt)  # execute insert statement
            db.flush()  # flush changes

        except SQLAlchemyError as e:  # catch any SQLAchemy errors and log them
            logging.error(f"show_repository.upsert_show: Database error during upsert of show_id {show_id}: {e}")
        except Exception as e:  # catch any other errors and log them
            logging.error(f"show_repository.upsert_show: Unexpected error during upsert of show show_id {show_id}: {e}")
