# BingeFriend Show Sync

Azure Function to synchronize TV show information from the TV Maze API.

## Functionality

*   **Ingest Shows**: Initial manual data sync of TV Maze shows
*   **Ingest Seasons and Episodes**: Initial manual data sync of TV Maze seasons and episodes
*   **Update Shows**: Scheduled retrieval of updated TV Maze shows
*   **Update Seasons and Episodes**: Scheduled retrieval of season and episode updates for updated TV Maze shows

## Requirements

### Packages

*   [tvbingefriend-tvmaze-client](https://github.com/tomboone/tvbingefriend-tvmaze-client)
*   [tvbingefriend-tvmaze-models](https://github.com/tomboone/bingefriend-tvmaze-models)

### Environment Variables

_Required_ - The application will not function without these envrionment variables:

*   `SQLALCHEMY_CONNECTION_STRING`: MySQL connection string for the app database.
*   `UPDATE_SHOWS_NCRON`: Schedule for retrieving updates and upserting show entities.
*   `UPDATE_SEASONS_EPISODES_NCRON`: Schedule for processing season and episode upserts for updated shows.

_Optional_ - Default names for the following storage queues, containers, and tables are provided in config.py, but can be customized with environment variables:

*   `TVMAZE_SHOWS_QUEUE`: Triggers TV Maze API call to bulk retrieve a page of TV shows on initial ingest.
*   `TVMAZE_SEASONS_EPISODES_QUEUE`: Triggers API call to retrieve a show's seasons and episodes on initial ingest.
*   `TVMAZE_SHOWS_UPDATE_QUEUE`: Triggers API call to retrieve TV Maze show updates.
*   `SHOW_STAGE_CONTAINER`: Stores a page of multiple shows to be processed on initial ingest.
*   `TVMAZE_SHOW_IDS_CONTAINER`:  Stores all show IDs for retrieval of seasons and episodes on initial ingest.
*   `SHOW_UPSERT_CONTAINER`: Stores one show to be upserted.
*   `TVMAZE_SEASONS_EPISODES_CONTAINER`: Stores show's seasons and episodes to be separated.
*   `TVMAZE_SEASONS_CONTAINER`: Stores a show's seasons to be processed
*   `SEASON_UPSERT_CONTAINER`: Stores one season to be upserted.
*   `TVMAZE_EPISODES_CONTAINER`: Stores a show's episodes to be processed.
*   `EPISODE_UPSERT_CONTAINER`: Stores one episode to be upserted.
*   `TVMAZE_UPDATES_CONTAINER`: Stores list of shows to update
*   `TVMAZE_SEASONS_EPISODES_UPDATE_TABLE`: Caches show IDs for later updating of seasons and episodes

## License

This project is licensed under the MIT License. See the [`LICENSE`](LICENSE) file for details.

## Disclaimer

This project is not affiliated with or endorsed by TV Maze.
