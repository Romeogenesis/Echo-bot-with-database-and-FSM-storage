import asyncio
import logging
import os
import sys

from config.config import Config, load_config
from psycopg_pool import AsyncConnectionPool
from psycopg import Error

config: Config = load_config()

logging.basicConfig(
    level=logging.getLevelName(level=config.log.level),
    format=config.log.format,
)

logger = logging.getLogger(__name__)

if sys.platform.startswith("win") or os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# SQL-запросы
CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE,
    username VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    language VARCHAR(10) NOT NULL DEFAULT 'ru',
    role VARCHAR(30) NOT NULL DEFAULT 'user',
    is_alive BOOLEAN NOT NULL DEFAULT TRUE,
    banned BOOLEAN NOT NULL DEFAULT FALSE
);
"""

CREATE_ACTIVITY_TABLE = """
CREATE TABLE IF NOT EXISTS activity (
    id SERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activity_date DATE NOT NULL DEFAULT CURRENT_DATE,
    actions INT NOT NULL DEFAULT 1
);
"""

CREATE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_activity_user_day ON activity (user_id, activity_date);
"""


async def main():
    # Строка подключения
    conninfo = (
        f"host={config.db.host} "
        f"port={config.db.port} "
        f"dbname={config.db.name} "
        f"user={config.db.user} "
        f"password={config.db.password}"
    )

    # Создаём пул (даже с одним соединением)
    pool = AsyncConnectionPool(conninfo=conninfo, min_size=1, max_size=1)

    async with pool:
        try:
            async with pool.connection() as conn:
                async with conn.transaction():
                    await conn.execute(CREATE_USERS_TABLE)
                    await conn.execute(CREATE_ACTIVITY_TABLE)
                    await conn.execute(CREATE_INDEX)
                    logger.info("✅ Таблицы `users` и `activity` успешно созданы")
        except Exception as e:
            logger.error("❌ Ошибка при создании таблиц: %s", e)
            raise
        finally:
            await pool.close()


if __name__ == "__main__":
    asyncio.run(main())