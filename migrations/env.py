from alembic import context
from sqlalchemy import engine_from_config, pool

from bamaq.adapters.database import metadata
from bamaq.config import Settings

config = context.config
config.set_main_option("sqlalchemy.url", Settings().database_url.replace("%", "%%"))

if context.is_offline_mode():
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    engine = engine_from_config(
        config.get_section(config.config_ini_section), poolclass=pool.NullPool
    )
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
