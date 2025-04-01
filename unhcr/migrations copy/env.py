import logging
from sqlalchemy import engine_from_config, text
from sqlalchemy import pool
from alembic import context
from unhcr import app_utils
from unhcr import constants as const
from unhcr import models  # Import your SQLAlchemy models
Base = models.Base #Import your Base

mods=[
    ["app_utils", "app_utils"],
    ["constants", "const"],
    ["models", "models"],
]

res = app_utils.app_init(mods, "unhcr.update_all.log", "0.4.6", level="INFO", override=True)
if const.LOCAL:
    logger, app_utils, const, models = res
else:
    logger = res

target_metadata=Base.metadata
# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

#!!!! we use our logging
# Interpret the config file for Python logger.
# This line sets up loggers basically.
# if config.config_file_name is not None:
#     fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata


def exclude_public(object, name, type_, reflected, compare_to):
    # Filter only the tables in 'solarman' schema, reduce unnecessary checks
    try:
        print(object.schema)
    except:
        pass
    # Exclude tables not in the 'solarman' schema
    if type_ == "table" and object.schema != 'solarman':
        return False

    # Exclude sequences not in the 'solarman' schema
    if type_ == "sequence" and object.schema != 'solarman':
        return False

    # Exclude indexes where the table's schema is not 'solarman'
    if type_ == "index" and object.table.schema != 'solarman':
        return False

    return True  # Keep everything else

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # Set the search path to only relevant schemas ('solarman' and 'public')
        connection.execute(text("SET search_path TO solarman;"))

        # Configure Alembic to process only objects from the 'solarman' schema
        context.configure(
            connection=connection,
            target_metadata=Base.metadata,
            #include_schemas=True,  # Multi-schema DB support
            version_table_schema="solarman",  # Track migrations in 'solarman'
            compare_type=True,  # Compare column types
            compare_server_default=True,  # Compare default values
            include_object=exclude_public  # Efficient filtering
        )

        # Run migrations only for relevant tables in the 'solarman' schema
        with context.begin_transaction():
            try:
                print(f"XXRunning migrations from {context.get_head_revision()} to {context.get_revision_argument()} {context.get_starting_revision_argument}")
            except:
                pass
            context.run_migrations()
            print(f"XXCompleted migrations to {context.get_head_revision()}")


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
        target_metadata=Base.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,  # Ensures Alembic compares schemas
        version_table_schema="solarman",  # Ensures version tracking is correct
        compare_type=True,  # Ensures column type changes are detected
    )

    with context.begin_transaction():
        context.run_migrations()


# def run_migrations_online() -> None:
#     """Run migrations in 'online' mode.

#     In this scenario we need to create an Engine
#     and associate a connection with the context.

#     """
#     connectable = engine_from_config(
#         config.get_section(config.config_ini_section, {}),
#         prefix="sqlalchemy.",
#         poolclass=pool.NullPool,
#     )

#     with connectable.connect() as connection:
#         context.configure(
#             connection=connection, target_metadata=target_metadata
#         )

#         with context.begin_transaction():
#             context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
