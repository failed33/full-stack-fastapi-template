import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Determine the path to alembic.ini relative to this env.py file
# env.py is in app/alembic/, alembic.ini is in the root of the app directory (/app)
# So, from app/app/alembic/env.py, alembic.ini is at ../../alembic.ini
current_dir = os.path.dirname(os.path.abspath(__file__))
alembic_ini_path = os.path.join(current_dir, "..", "..", "alembic.ini")

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if os.path.exists(alembic_ini_path):
    fileConfig(alembic_ini_path)
else:
    # Fallback or log error if alembic.ini is still not found, though it should be.
    # This helps in debugging if the path calculation is wrong.
    # For tests, if logging isn't critical immediately, we might proceed,
    # but Pydantic settings are more crucial.
    print(f"Warning: Alembic logging configuration file not found at {alembic_ini_path}. Logging may not be configured as expected.")
    # If running actual migrations, this would be a critical error.
    # Depending on strictness, you might raise an error here.
    # For now, a print for awareness during tests.

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
# target_metadata = None

from app.models import SQLModel  # noqa
from app.core.config import settings # noqa

target_metadata = SQLModel.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url():
    return str(settings.SQLALCHEMY_DATABASE_URI)


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    context.configure(
        url=url, target_metadata=target_metadata, literal_binds=True, compare_type=True
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, compare_type=True
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
