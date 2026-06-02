import asyncpg
import os

_pool = None

async def create_pool():
    global _pool
    db_url = os.getenv("POSTGRES_URL", "postgresql://engine_user:engine_password@postgres:5432/outlier_db")
    _pool = await asyncpg.create_pool(db_url)

async def close_pool():
    global _pool
    if _pool:
        await _pool.close()

def get_pool():
    if not _pool:
        raise Exception("Database pool is not initialized")
    return _pool
