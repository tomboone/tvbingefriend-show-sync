"""Configuration for alembic"""
import json
import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context


def load_env_from_local_settings_if_needed():
    """
    For local development, load settings from local.settings.json into environment
    variables if they aren't already set. This allows Alembic to work in an
    Azure Functions project structure without the Functions host running.
    This function must be called BEFORE any application modules are imported.
    """
    # We only need to do this if a key env var is missing.
    if "SQLALCHEMY_CONNECTION_STRING" in os.environ:
        return

    # Construct the path to local.settings.json, assuming this script is in alembic/
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    settings_path = os.path.join(project_root, 'local.settings.json')

    if not os.path.exists(settings_path):
        # If the file doesn't exist, we don't do anything. The application's
        # own config loading will raise the appropriate error.
        return

    print(f"INFO [alembic.env] Loading environment variables from {settings_path}")
    with open(settings_path) as f:
        settings = json.load(f)

    # Get the 'Values' and load them into the environment
    env_vars = settings.get("Values", {})
    for key, value in env_vars.items():
        if key not in os.environ:
            os.environ[key] = value


# Execute the environment loader before importing any application code.
load_env_from_local_settings_if_needed()

# Now that the environment is patched, we can safely import application modules.
from tvbingefriend_show_sync.config import SQLALCHEMY_CONNECTION_STRING
from tvbingefriend_tvmaze_models.models.base import Base

# For autogenerate to work, you must import your model classes here.
# This allows them to be registered with the Base.metadata object.
# Assuming your models are in a package named 'tvbingefriend-tvmaze-models'
from tvbingefriend_tvmaze_models.models.episode import Episode  # noqa: F401
from tvbingefriend_tvmaze_models.models.season import Season  # noqa: F401
from tvbingefriend_tvmaze_models.models.show import Show  # noqa: F401

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Set the database URL from your application's configuration.
# This overrides the sqlalchemy.url in alembic.ini.
config.set_main_option("sqlalchemy.url", SQLALCHEMY_CONNECTION_STRING)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(config.get_section(config.config_ini_section, {}), poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
