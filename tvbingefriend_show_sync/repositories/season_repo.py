"""Repository for seasons."""
import logging
from typing import Any

from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, Mapper
from sqlalchemy.orm.properties import ColumnProperty
from tvbingefriend_tvmaze_models.models.season import Season


# noinspection PyMethodMayBeStatic
class SeasonRepository:
    """Repository for seasons."""
    def upsert_season(self, season: dict[str, Any], db: Session) -> None:
        """Upsert a season in the database

        Args:
            season (dict[str, Any]): Season to upsert
            db (Session): Database session
        """
        show_id: int | None = season.get("show_id")  # get show_id from season
        logging.debug(
            msg=f"SeasonRepository.upsert_season: show_id: {show_id}"
        )
        season_data: dict[str, Any] | None = season.get("season")  # get season from season

        if not show_id or not season_data:
            logging.error(
                msg="SeasonRepository.upsert_season: Season must have a show_id and season"
            )
            return

        season_id: int | None = season_data.get("id")
        logging.debug(
            msg=f"SeasonRepository.upsert_season: season_id: {season_id}"
        )

        if not season_id:
            logging.error(
                msg="SeasonRepository.upsert_season: Season must have a season_id"
            )
            return

        mapper: Mapper = inspect(Season)  # get mapper for Season
        season_columns: set[str] = {  # get columns for Season
            prop.key for prop in mapper.attrs.values() if isinstance(prop, ColumnProperty)
        }

        insert_values: dict[str, Any] = {  # create insert values
            key: value for key, value in season_data.items() if key in season_columns
        }
        insert_values["id"] = season_id  # set id
        insert_values["show_id"] = show_id  # set show_id

        update_values: dict[str, Any] = {  # create update values
            key: value for key, value in insert_values.items() if key != "id"
        }

        try:
            stmt: mysql_insert = mysql_insert(Season).values(insert_values)  # create insert statement
            stmt = stmt.on_duplicate_key_update(**update_values)  # create duplicate key update statement

            db.execute(stmt)  # execute insert statement
            db.flush()  # flush changes

        except SQLAlchemyError as e:  # catch any SQLAchemy errors and log them
            logging.error(
                msg=f"Database error during upsert of season_id {season_id}: {e}"
            )
        except Exception as e:  # catch any other errors and log them
            logging.error(
                msg=f"Unexpected error during upsert of season_id {season_id}: {e}"
            )
