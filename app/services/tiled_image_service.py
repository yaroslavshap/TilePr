from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Tuple

from app.domain.tiles import TileFormat
from app.contracts.tiles_repository import TileRepository, ManifestRepository
from app.utils.ttl_cache import InMemoryTTLCache

TileKey = Tuple[str, int, int, int, str]   # (uuid, z, y, x, fmt)
ManifestKey = Tuple[str, str]              # ("manifest", uuid)


@dataclass(frozen=True)
class TileBytes:
    data: bytes
    media_type: str


class TiledImageService:
    def __init__(
        self,
        *,
        tile_repo: TileRepository,
        manifest_repo: ManifestRepository,
        cache: InMemoryTTLCache[object, bytes],
    ):
        self.tile_repo = tile_repo
        self.manifest_repo = manifest_repo
        self.cache = cache

    def _get_manifest_cached(self, uuid: str) -> Optional[dict]:
        mkey: ManifestKey = ("manifest", uuid)

        cached = self.cache.get(mkey)
        if cached is not None:
            try:
                return json.loads(cached.decode("utf-8"))
            except Exception:
                self.cache.delete(mkey)

        raw = self.manifest_repo.get_manifest(uuid)
        if raw is None:
            return None

        self.cache.set(mkey, raw, size=len(raw))

        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def get_tile_bytes(self, uuid: str, z: int, y: int, x: int) -> TileBytes:
        man = self._get_manifest_cached(uuid)
        if not man:
            raise FileNotFoundError("Manifest not found")

        fmt: TileFormat = man["format"]
        levels = man["levels"]
        lvl = levels.get(str(z))
        if not lvl:
            raise FileNotFoundError("Level not found")

        if not (0 <= x < int(lvl["tiles_x"]) and 0 <= y < int(lvl["tiles_y"])):
            raise FileNotFoundError("Tile out of range")

        key: TileKey = (uuid, int(z), int(y), int(x), str(fmt))

        cached = self.cache.get(key)
        if cached is not None:
            return TileBytes(
                data=cached,
                media_type="image/webp" if fmt == "webp" else "image/png",
            )

        # MISS -> читаем из репозитория и кладём в кеш
        uri, stream = self.tile_repo.open_tile(uuid, z, y, x, fmt=fmt)
        try:
            data = stream.read()
        finally:
            try:
                stream.close()
                getattr(stream, "release_conn", lambda: None)()
            except Exception:
                pass

        self.cache.set(key, data, size=len(data))

        return TileBytes(
            data=data,
            media_type="image/webp" if fmt == "webp" else "image/png",
        )

    def invalidate_one_tile(self, uuid: str, z: int, y: int, x: int, fmt: str) -> None:
        self.cache.delete((uuid, z, y, x, fmt))

    def invalidate_manifest(self, uuid: str) -> None:
        self.cache.delete(("manifest", uuid))