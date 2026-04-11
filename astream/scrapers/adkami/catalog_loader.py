"""
CatalogLoader — Téléchargement des catalogues Adkami depuis GitHub.

Méthode identique à DatasetLoader (data_loader.py) mais pour les catalogues.

Workflow :
  1. Au démarrage : utilise les fichiers locaux seed (data/catalogues/*.json)
                    si ADKAMI_CATALOGS_URL est configurée, télécharge les plus récents
  2. À 3h Paris    : re-télécharge tous les fichiers depuis GitHub

Format GitHub attendu :
  ADKAMI_CATALOGS_URL=https://raw.githubusercontent.com/user/repo/main/catalogues
  → https://raw.githubusercontent.com/.../final_action.json
  → https://raw.githubusercontent.com/.../simulcast.json
  → https://raw.githubusercontent.com/.../final_aventure.json
  → ...

Le script generateur_catalogue.py tourne en local, ses résultats sont pushés
sur GitHub, l'addon les récupère ici.
"""

import asyncio
import json
import os
import time
from typing import Dict, List, Any, Optional

from astream.utils.http_client import http_client
from astream.utils.logger import logger
from astream.config.settings import settings
from astream.scrapers.adkami.scraper import ADKAMI_GENRES, CATALOGUE_DIR

# Liste de tous les fichiers catalogue à télécharger
_CATALOG_FILES = ["simulcast.json"] + [
    f"final_{genre.lower()}.json" for genre in ADKAMI_GENRES.keys()
]


class CatalogLoader:
    """
    Gère le cycle de vie des fichiers catalogue JSON Adkami :
    - Initialisation depuis les fichiers locaux seed
    - Mise à jour depuis GitHub si ADKAMI_CATALOGS_URL est configurée
    - Re-téléchargement quotidien à 3h (appelé par le scheduler)
    """

    def __init__(self):
        self.base_url: Optional[str] = getattr(settings, "ADKAMI_CATALOGS_URL", None)
        os.makedirs(CATALOGUE_DIR, exist_ok=True)

    # ===========================
    # Initialisation au démarrage
    # ===========================

    async def initialize(self) -> None:
        """
        Vérifie les fichiers locaux et télécharge les manquants depuis GitHub.
        Les fichiers seed fournis avec le code sont utilisés tels quels si
        ADKAMI_CATALOGS_URL n'est pas configurée.
        """
        local_count = self._count_local_files()
        logger.log("ANIMESAMA", f"ADKAMI: {local_count}/{len(_CATALOG_FILES)} catalogues disponibles en local")

        if not self.base_url:
            logger.log("ANIMESAMA", "ADKAMI: ADKAMI_CATALOGS_URL non configurée — utilisation des fichiers seed locaux")
            return

        # Télécharger uniquement les fichiers manquants au démarrage
        missing = [f for f in _CATALOG_FILES if not self._file_exists(f)]
        if missing:
            logger.log("ANIMESAMA", f"ADKAMI: {len(missing)} fichiers manquants — téléchargement depuis GitHub...")
            await self._download_files(missing)
        else:
            logger.log("ANIMESAMA", "ADKAMI: Tous les catalogues sont disponibles en local ✅")

    # ===========================
    # Rafraîchissement complet (appelé à 3h par le scheduler)
    # ===========================

    async def refresh_all(self) -> None:
        """
        Re-télécharge TOUS les fichiers catalogue depuis GitHub.
        Appelé par le scheduler à 3h Paris.
        """
        if not self.base_url:
            logger.log("ANIMESAMA", "ADKAMI: ADKAMI_CATALOGS_URL non configurée — skip refresh GitHub")
            return

        logger.log("ANIMESAMA", f"ADKAMI: Refresh complet depuis GitHub ({len(_CATALOG_FILES)} fichiers)...")
        await self._download_files(_CATALOG_FILES)
        logger.log("ANIMESAMA", "ADKAMI: ✅ Refresh catalogues terminé")

    # ===========================
    # Téléchargement d'une liste de fichiers
    # ===========================

    async def _download_files(self, filenames: List[str]) -> None:
        """
        Télécharge les fichiers en séquence depuis GitHub raw.
        Garde les fichiers locaux existants si le téléchargement échoue.
        """
        ok = 0
        fail = 0
        base = self.base_url.rstrip("/")

        for filename in filenames:
            url = f"{base}/{filename}"
            try:
                logger.log("ANIMESAMA", f"ADKAMI: ⬇ {filename}")
                response = await http_client.get(url)

                if response.status_code == 200:
                    # Vérifier que le JSON est valide avant d'écraser
                    try:
                        data = response.json()
                    except Exception:
                        text = response.text
                        data = json.loads(text)

                    dest = os.path.join(CATALOGUE_DIR, filename)
                    with open(dest, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    ok += 1
                    logger.log("ANIMESAMA", f"ADKAMI: ✅ {filename} ({len(data)} entrées)")

                elif response.status_code == 404:
                    logger.warning(f"ADKAMI: {filename} introuvable sur GitHub (404) — fichier local conservé")
                    fail += 1
                else:
                    logger.warning(f"ADKAMI: {filename} HTTP {response.status_code} — fichier local conservé")
                    fail += 1

                # Pause entre requêtes (politesse GitHub)
                await asyncio.sleep(0.3)

            except Exception as e:
                logger.error(f"ADKAMI: Erreur téléchargement {filename}: {e}")
                fail += 1

        logger.log("ANIMESAMA", f"ADKAMI: Téléchargement terminé — {ok} OK / {fail} échecs")

    # ===========================
    # Helpers
    # ===========================

    def _file_exists(self, filename: str) -> bool:
        path = os.path.join(CATALOGUE_DIR, filename)
        return os.path.exists(path) and os.path.getsize(path) > 100

    def _count_local_files(self) -> int:
        return sum(1 for f in _CATALOG_FILES if self._file_exists(f))

    def get_file_ages(self) -> Dict[str, float]:
        """Retourne l'âge en heures de chaque fichier catalogue."""
        now = time.time()
        ages = {}
        for f in _CATALOG_FILES:
            path = os.path.join(CATALOGUE_DIR, f)
            if os.path.exists(path):
                ages[f] = (now - os.path.getmtime(path)) / 3600
        return ages


# ===========================
# Instance Singleton
# ===========================
catalog_loader = CatalogLoader()
