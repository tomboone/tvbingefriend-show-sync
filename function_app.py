"""Azure Functions for syncing show data"""
import azure.functions as func

from tvbingefriend_show_sync.blueprints.bp_episodes import bp as bp_episodes
from tvbingefriend_show_sync.blueprints.bp_seasons import bp as bp_seasons
from tvbingefriend_show_sync.blueprints.bp_seasons_episodes import bp as bp_seasons_episodes
from tvbingefriend_show_sync.blueprints.bp_shows import bp as bp_shows
from tvbingefriend_show_sync.blueprints.bp_update import bp as bp_update

app = func.FunctionApp()

app.register_blueprint(bp_episodes)
app.register_blueprint(bp_seasons)
app.register_blueprint(bp_seasons_episodes)
app.register_blueprint(bp_shows)
app.register_blueprint(bp_update)
