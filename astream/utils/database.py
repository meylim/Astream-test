import os
import time
import json
import asyncio

from astream.utils.logger import logger
from astream.config.settings import database, settings

DATABASE_VERSION = "2.0"


async def setup_database():
    try:
        if settings.DATABASE_TYPE == "sqlite":
            os.makedirs(os.path.dirname(settings.DATABASE_PATH), exist_ok=True)
            if not os.path.exists(settings.DATABASE_PATH):
                with open(settings.DATABASE_PATH, "a"):
                    pass

        await database.connect()

        await database.execute("CREATE TABLE IF NOT EXISTS db_version (id INTEGER PRIMARY KEY CHECK (id = 1), version TEXT)")
        current_version = await database.fetch_val("SELECT version FROM db_version WHERE id = 1")

        if current_version != DATABASE_VERSION:
            logger.log("DATABASE", f"Migration v{current_version} → v{DATABASE_VERSION}")

            allowed_tables = {'scrape_lock', 'metadata', 'animesama', 'tmdb'}

            if settings.DATABASE_TYPE == "sqlite":
                tables = await database.fetch_all("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('db_version', 'sqlite_sequence')")
                for table in tables:
                    table_name = table['name']
                    if table_name not in allowed_tables:
                        logger.warning(f"Table non autorisée ignorée: {table_name}")
                        continue
                    if not table_name.replace('_', '').isalnum() or len(table_name) > 64:
                        logger.warning(f"Table format nom invalide ignorée: {table_name}")
                        continue
                    if table_name in allowed_tables and table_name.replace('_', '').isalnum():
                        await database.execute("DROP TABLE IF EXISTS " + table_name)
                        logger.log("DATABASE", f"Table supprimée: {table_name}")
                    else:
                        logger.error(f"Tentative suppression table non autorisée: {table_name}")
            else:
                tables = await database.fetch_all("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename != 'db_version'")
                for table in tables:
                    table_name = table['tablename']
                    if table_name not in allowed_tables:
                        logger.warning(f"Table non autorisée ignorée: {table_name}")
                        continue
                    if not table_name.replace('_', '').isalnum() or len(table_name) > 64:
                        logger.warning(f"Table format nom invalide ignorée: {table_name}")
                        continue
                    if table_name in allowed_tables and table_name.replace('_', '').isalnum():
                        await database.execute(f"DROP TABLE IF EXISTS {table_name}")
                        logger.log("DATABASE", f"Table supprimée: {table_name}")
                    else:
                        logger.error(f"Tentative suppression table non autorisée: {table_name}")

            if settings.DATABASE_TYPE == "sqlite":
                await database.execute("INSERT OR REPLACE INTO db_version VALUES (1, :version)", {"version": DATABASE_VERSION})
            else:
                await database.execute("INSERT INTO db_version VALUES (1, :version) ON CONFLICT (id) DO UPDATE SET version = :version", {"version": DATABASE_VERSION})
            logger.log("DATABASE", f"Migration base de données vers v{DATABASE_VERSION} terminée")

        await database.execute("CREATE TABLE IF NOT EXISTS scrape_lock (lock_key TEXT PRIMARY KEY, instance_id TEXT, timestamp INTEGER, expires_at INTEGER)")

        await database.execute("CREATE TABLE IF NOT EXISTS animesama (key TEXT PRIMARY KEY, content TEXT NOT NULL, created_at INTEGER, expires_at INTEGER)")
        await database.execute("CREATE TABLE IF NOT EXISTS tmdb (key TEXT PRIMARY KEY, content TEXT NOT NULL, created_at INTEGER, expires_at INTEGER)")

        await database.execute("CREATE INDEX IF NOT EXISTS idx_scrape_lock_key ON scrape_lock(lock_key)")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_scrape_lock_expires ON scrape_lock(expires_at)")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_animesama_key ON animesama(key)")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_animesama_expires ON animesama(expires_at)")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_tmdb_key ON tmdb(key)")
        await database.execute("CREATE INDEX IF NOT EXISTS idx_tmdb_expires ON tmdb(expires_at)")

        if settings.DATABASE_TYPE == "sqlite":
            await database.execute("PRAGMA busy_timeout=30000")
            await database.execute("PRAGMA journal_mode=WAL")
            await database.execute("PRAGMA synchronous=NORMAL")
            await database.execute("PRAGMA temp_store=MEMORY")
            await database.execute("PRAGMA cache_size=-2000")
            await database.execute("PRAGMA foreign_keys=ON")

        current_time = time.time()
        await database.execute("DELETE FROM animesama WHERE expires_at IS NOT NULL AND expires_at < :current_time;", {"current_time": current_time})
        await database.execute("DELETE FROM tmdb WHERE expires_at IS NOT NULL AND expires_at < :current_time;", {"current_time": current_time})

    except Exception as e:
        logger.error(f"Erreur configuration base de données: {e}")


async def cleanup_expired_locks():
    while True:
        try:
            current_time = int(time.time())
            await database.execute("DELETE FROM scrape_lock WHERE expires_at < :current_time", {"current_time": current_time})
        except Exception as e:
            logger.error(f"Erreur nettoyage périodique verrous: {e}")
        await asyncio.sleep(60)


async def get_metadata_from_cache(cache_id: str):
    current_time = time.time()

    if cache_id.startswith("as:"):
        table_name = "animesama"
    elif cache_id.startswith("tmdb:"):
        table_name = "tmdb"
    else:
        logger.warning(f"Préfixe de cache inconnu: {cache_id}")
        return None

    query = f"SELECT content FROM {table_name} WHERE key = :cache_id AND expires_at > :current_time"
    result = await database.fetch_one(query, {"cache_id": cache_id, "current_time": current_time})
    if not result or not result["content"]:
        return None
    try:
        return json.loads(result["content"])
    except json.JSONDecodeError:
        return None


async def set_metadata_to_cache(cache_id: str, data, ttl: int = None):
    current_time = time.time()

    if cache_id.startswith("as:"):
        table_name = "animesama"
    elif cache_id.startswith("tmdb:"):
        table_name = "tmdb"
    else:
        logger.warning(f"Préfixe de cache inconnu: {cache_id}")
        return

    if ttl is None:
        ttl = await _calculate_context_aware_ttl(cache_id)

    expires_at = current_time + ttl
    if settings.DATABASE_TYPE == "sqlite":
        query = f"INSERT OR REPLACE INTO {table_name} (key, content, created_at, expires_at) VALUES (:cache_id, :content, :created_at, :expires_at)"
    else:
        query = f"INSERT INTO {table_name} (key, content, created_at, expires_at) VALUES (:cache_id, :content, :created_at, :expires_at) ON CONFLICT (key) DO UPDATE SET content = :content, created_at = :created_at, expires_at = :expires_at"
    values = {"cache_id": cache_id, "content": json.dumps(data), "created_at": current_time, "expires_at": expires_at}
    await database.execute(query, values)


async def delete_metadata_from_cache(cache_id: str):
    """Supprime une entrée du cache pour forcer un re-fetch."""
    if cache_id.startswith("as:"):
        table_name = "animesama"
    elif cache_id.startswith("tmdb:"):
        table_name = "tmdb"
    else:
        logger.warning(f"Préfixe de cache inconnu pour suppression: {cache_id}")
        return

    query = f"DELETE FROM {table_name} WHERE key = :cache_id"
    await database.execute(query, {"cache_id": cache_id})


async def get_cache_age(cache_id: str) -> float:
    """
    Retourne l'âge en secondes d'une entrée cache.
    Retourne float('inf') si l'entrée n'existe pas ou est expirée.
    Utilisé par sorties_du_jour pour savoir si la homepage est assez fraîche.
    """
    current_time = time.time()

    if cache_id.startswith("as:"):
        table_name = "animesama"
    elif cache_id.startswith("tmdb:"):
        table_name = "tmdb"
    else:
        return float('inf')

    query = f"SELECT created_at FROM {table_name} WHERE key = :cache_id AND expires_at > :current_time"
    result = await database.fetch_one(query, {"cache_id": cache_id, "current_time": current_time})

    if not result or not result["created_at"]:
        return float('inf')

    return current_time - result["created_at"]


async def _calculate_context_aware_ttl(cache_id: str) -> int:
    try:
        if cache_id.startswith("tmdb:"):
            return settings.TMDB_TTL

        if cache_id == "as:planning":
            return settings.PLANNING_TTL

        if cache_id == "as:homepage":
            return settings.DYNAMIC_LIST_TTL

        if cache_id.startswith("as:") and ":s" in cache_id and "e" in cache_id:
            return settings.EPISODE_TTL

        if cache_id.startswith("as:search:"):
            return settings.DYNAMIC_LIST_TTL

        if cache_id.startswith("as:") and not any(x in cache_id for x in ["search", "homepage", "planning", ":s", ":e"]):
            anime_slug = cache_id.replace("as:", "")
            from astream.scrapers.animesama.planning import get_smart_cache_ttl
            return await get_smart_cache_ttl(anime_slug)
        if cache_id.startswith("as:"):
            return settings.DYNAMIC_LIST_TTL
        return settings.DYNAMIC_LIST_TTL

    except Exception as e:
        logger.log("PERFORMANCE", f"Erreur calcul TTL intelligent '{cache_id}': {e}")
        return settings.EPISODE_TTL


async def acquire_lock(lock_key: str, instance_id: str, duration: int = None) -> bool:
    try:
        current_time = int(time.time())
        lock_duration = duration if duration is not None else settings.SCRAPE_LOCK_TTL
        expires_at = current_time + lock_duration

        await database.execute(
            "DELETE FROM scrape_lock WHERE lock_key = :lock_key AND expires_at < :current_time",
            {"lock_key": lock_key, "current_time": current_time}
        )

        if settings.DATABASE_TYPE == "sqlite":
            await database.execute(
                "INSERT OR IGNORE INTO scrape_lock (lock_key, instance_id, timestamp, expires_at) VALUES (:lock_key, :instance_id, :timestamp, :expires_at)",
                {"lock_key": lock_key, "instance_id": instance_id, "timestamp": current_time, "expires_at": expires_at}
            )
            existing_lock = await database.fetch_one(
                "SELECT instance_id FROM scrape_lock WHERE lock_key = :lock_key",
                {"lock_key": lock_key}
            )
            if existing_lock and existing_lock["instance_id"] == instance_id:
                return True
            else:
                return False
        else:
            result = await database.fetch_one(
                "INSERT INTO scrape_lock (lock_key, instance_id, timestamp, expires_at) VALUES (:lock_key, :instance_id, :timestamp, :expires_at) ON CONFLICT (lock_key) DO NOTHING RETURNING instance_id",
                {"lock_key": lock_key, "instance_id": instance_id, "timestamp": current_time, "expires_at": expires_at}
            )
            if result:
                return True
            else:
                return False

    except Exception as e:
        logger.warning(f"Échec acquisition verrou {lock_key}: {e}")
        return False


async def release_lock(lock_key: str, instance_id: str) -> bool:
    try:
        await database.execute("DELETE FROM scrape_lock WHERE lock_key = :lock_key AND instance_id = :instance_id", {"lock_key": lock_key, "instance_id": instance_id})
        return True
    except Exception as e:
        logger.warning(f"Échec libération verrou {lock_key}: {e}")
        return False


class DistributedLock:

    def __init__(self, lock_key: str, instance_id: str = None, duration: int = None):
        self.lock_key = lock_key
        self.instance_id = instance_id or f"astream_{int(time.time())}"
        self.duration = duration if duration is not None else settings.SCRAPE_LOCK_TTL
        self.acquired = False

    async def __aenter__(self):
        start_time = time.time()
        timeout = settings.SCRAPE_WAIT_TIMEOUT

        while time.time() - start_time < timeout:
            self.acquired = await acquire_lock(self.lock_key, self.instance_id, self.duration)
            if self.acquired:
                return self

            await asyncio.sleep(1)

        raise LockAcquisitionError(f"Impossible d'acquérir le verrou {self.lock_key} après {timeout}s")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            await release_lock(self.lock_key, self.instance_id)


class LockAcquisitionError(Exception):
    pass


async def teardown_database():
    try:
        await database.disconnect()
    except Exception as e:
        logger.error(f"Erreur fermeture base de données: {e}")
                        
