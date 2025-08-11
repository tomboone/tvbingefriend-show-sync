# BingeFriend Show Sync

Azure Function to synchronize TV show information from the TV Maze API.

## Functionality

*   **Ingest Shows**: Initial manual data sync of TV Maze shows
*   **Ingest Seasons and Episodes**: Initial manual data sync of TV Maze seasons and episodes
*   **Update Shows**: Scheduled retrieval of updated TV Maze shows
*   **Update Seasons and Episodes**: Scheduled retrieval of season and episode updates for updated TV Maze shows

## Requirements

### Packages

*   `tvbingefriend-tvmaze-client`
*   `tvbingefriend-tvmaze-models`

### Environment Variables

*   `SQLALCHEMY_CONNECTION_STRING`: MySQL connection string for the app database.
*   `UPDATE_SHOWS_NCRON`: Schedule for retrieving updates and upserting show entities.
*   `UPDATE_SEASONS_EPISODES_NCRON`: Schedule for processing season and episode upserts for updated shows.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Disclaimer

This project is not affiliated with or endorsed by TV Maze.
