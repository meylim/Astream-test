"""
PATCH pour astream/utils/database.py

Ajouter cette fonction APRÈS set_metadata_to_cache() (ligne ~148).
Elle est utilisée par le scheduler pour invalider les caches journaliers.
"""


async def delete_metadata_from_cache(cache_id: str):
    """
    Supprime une entrée du cache. Utilisé par le scheduler
    pour forcer un re-fetch des données journalières.
    """
    if cache_id.startswith("as:"):
        table_name = "animesama"
    elif cache_id.startswith("tmdb:"):
        table_name = "tmdb"
    else:
        logger.warning(f"Préfixe de cache inconnu pour suppression: {cache_id}")
        return

    query = f"DELETE FROM {table_name} WHERE key = :cache_id"
    await database.execute(query, {"cache_id": cache_id})
        
