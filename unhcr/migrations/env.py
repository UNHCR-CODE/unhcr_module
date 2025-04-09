import logging
from sqlalchemy.dialects import postgresql
import re
from sqlalchemy import engine_from_config, event, text
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

res = app_utils.app_init(mods=mods, log_file="unhcr.update_all.log", version="0.4.6", level="INFO", override=True)
if const.LOCAL:
    logger, app_utils, const, models = res
else:
    logger = res

target_metadata=Base.metadata
# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config


def add_if_exists_to_drop_constraint(connection, cursor, statement, parameters, context, executemany):
    """Modify DROP CONSTRAINT statements to add 'IF EXISTS'."""
    # Ensure we only modify ALTER TABLE statements
    if isinstance(statement, str) and "DROP CONSTRAINT" in statement:
        # Using regex to add IF EXISTS to DROP CONSTRAINT commands
        statement = re.sub(
            r"ALTER TABLE\s+(\S+)\s+DROP CONSTRAINT\s+(\S+)",
            r"ALTER TABLE \1 DROP CONSTRAINT IF EXISTS \2",
            statement
        )
        print(f"Modified SQL: {statement}")  # For debugging purposes
    # Execute the modified statement
    return statement

def register_listener(connection):
    event.listen(connection, "after_cursor_execute", add_if_exists_to_drop_constraint)

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # Register the event listener for all SQL execution
        event.listen(connection, "after_cursor_execute", add_if_exists_to_drop_constraint)

        context.configure(
            connection=connection,
            target_metadata=models.metadata,
            #include_schemas=True,
            version_table_schema="solarman",
        )

        with context.begin_transaction():
            context.run_migrations()
            
            
            


#!!!! we use our logging
# Interpret the config file for Python logger.
# This line sets up loggers basically.
# if config.config_file_name is not None:
#     fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata


# def exclude_public(object, name, type_, reflected, compare_to):
#     # Filter only the tables in 'solarman' schema, reduce unnecessary checks
#     try:
#         print(object.schema)
#     except:
#         pass
#     # Exclude tables not in the 'solarman' schema
#     if type_ == "table" and object.schema != 'solarman':
#         return False

#     # Exclude sequences not in the 'solarman' schema
#     if type_ == "sequence" and object.schema != 'solarman':
#         return False

#     # Exclude indexes where the table's schema is not 'solarman'
#     if type_ == "index" and object.table.schema != 'solarman':
#         return False

#     return True  # Keep everything else




# from sqlalchemy.sql.elements import TextClause

# orig_exec = None  # Store original execution method


# def schema_qualified_exec(self, statement, *multiparams, **params):
#     """Intercept and modify SQL statements to ensure schema qualification for ALTER TABLE."""

#     # Convert SQLAlchemy objects to raw SQL if necessary
#     if isinstance(statement, TextClause):
#         statement = str(statement)

#     # Modify only ALTER TABLE statements missing schema
#     if isinstance(statement, str) and re.search(r'ALTER TABLE\s+(?!solarman\.)', statement, re.IGNORECASE):
#         statement = re.sub(r'ALTER TABLE\s+(\w+)', r'ALTER TABLE solarman.\1', statement, flags=re.IGNORECASE)

#     # Call the original execute function with correctly formatted parameters
#     return orig_exec(self, statement, *multiparams, **params)


# def run_migrations_online():
#     from alembic.script import ScriptDirectory

#     connectable = engine_from_config(
#         config.get_section(config.config_ini_section),
#         prefix="sqlalchemy.",
#         poolclass=pool.NullPool,
#     )

#     with connectable.connect() as connection:
#         global orig_exec
#         orig_exec = connection.execute
#         connection.execute = schema_qualified_exec.__get__(connection)  # Bind custom function

#         context.configure(
#             connection=connection,
#             target_metadata=models.metadata,
#             include_schemas=True,
#             version_table_schema="solarman"
#         )

#         # Get the current head revision from Alembic
#         script = ScriptDirectory.from_config(config)
#         current_rev = script.get_current_head()

#         with context.begin_transaction():
#             try:
#                 head_rev = context.get_head_revision()
#                 print(f"Running migrations from {current_rev} to {head_rev}")
#             except Exception as e:
#                 print(f"Error retrieving Alembic revision info: {e}")

#             context.run_migrations()
#             print(f"Completed migrations to {context.get_head_revision()}")

#         # Restore the original execute function
#         connection.execute = orig_exec



                
            # ... existing migration code ...        
        
        
    # # Store the original impl
    #     original_impl = context.get_context().impl
        
    #     # Override the _exec method to ensure schema qualification
    #     def _exec_with_schema(self, stmt, execution_options=None):
    #         # If this is a DROP CONSTRAINT statement without schema qualification
    #         if isinstance(stmt, str) and "DROP CONSTRAINT" in stmt and "solarman." not in stmt:
    #             # Add schema qualification
    #             stmt = stmt.replace("TABLE ", "TABLE solarman.")
    #         return original_impl._exec(stmt, execution_options)
        
    #     # Apply our override
    #     context.get_context().impl._exec = _exec_with_schema.__get__(context.get_context().impl)
        
    #     context.configure(
    #         connection=connection,
    #         target_metadata=target_metadata,
    #         include_schemas=True,
    #         version_table_schema="solarman",
    #         # Only process objects in the solarman schema
    #         include_object=lambda obj, name, type_, reflected, compare_to: 
    #             getattr(obj, 'schema', None) == 'solarman' if hasattr(obj, 'schema') else name in target_metadata.tables
    #     )        
        
        
        
        # Set the search path to only relevant schemas ('solarman' and 'public')
        #connection.execute(text("SET search_path TO solarman;"))

        # Configure Alembic to process only objects from the 'solarman' schema
        # context.configure(
        #     connection=connection,
        #     target_metadata=Base.metadata,
        #     #include_schemas=True,  # Multi-schema DB support
        #     version_table_schema="solarman",  # Track migrations in 'solarman'
        #     compare_type=True,  # Compare column types
        #     compare_server_default=True,  # Compare default values
        #     include_object=exclude_public  # Efficient filtering
        # )
        
        # context.configure(
        #     connection=connection,
        #     target_metadata=models.metadata,
        #     # Important: This ensures Alembic generates schema-qualified table names
        #     include_schemas=True,
        #     # Use the schema from your metadata
        #     version_table_schema=models.metadata.schema,
        #     # Compare against the specified schema
        #     include_object=exclude_public
        #     #include_object=lambda obj, name, type_, reflected, compare_to: obj.schema == 'solarman' if hasattr(obj, 'schema') else True
        # )

        # # Run migrations only for relevant tables in the 'solarman' schema
        # with context.begin_transaction():
        #     try:
        #         print(f"XXRunning migrations from {context.get_head_revision()} to {context.get_revision_argument()} {context.get_starting_revision_argument}")
        #     except:
        #         pass
        #     res = context.run_migrations()
        #     print(f"XXCompleted migrations to {context.get_head_revision()}")


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
