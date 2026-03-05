# app/services/tiles_service.py
import json
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from PIL import Image
Image.MAX_IMAGE_PIXELS = None  # полностью отключить защиту


from app.domain.tiles_domain import TileManifest, LevelInfo, TileFormat
from app.contracts.tiles_repository import TilesRepository
from app.utils.ttl_cache import InMemoryTTLCache

TileKey = Tuple[str, int, int, int, str]   # (uuid, z, y, x, fmt)
ManifestKey = Tuple[str, str]              # ("manifest", uuid)

@dataclass(frozen=True)
class TileBytes:
    data: bytes
    media_type: str

class TilesService:
    """
    Use-cases для тайлов:
    - build pyramid + save tiles + manifest
    - read tile (with manifest validation + TTL cache)
    - read manifest
    - deletes + cache invalidation
    """
    def __init__(
        self,
        *,
        repo: TilesRepository,
        cache: InMemoryTTLCache[object, bytes],
    ):
        self.repo = repo
        self.cache = cache

    # -------- build --------

    def build_pyramid(
        self,
        *,
        uuid: str,
        image: Image.Image,
        tile_size: int,
        fmt: TileFormat,
        lossless: bool = False,
    ) -> TileManifest:
        if tile_size not in (256, 512):
            raise ValueError("tile_size must be 256 or 512")
        if fmt not in ("webp", "png"):
            raise ValueError("fmt must be webp or png")

        # normalize to RGBA (nice padding behavior)
        if image.mode != "RGBA":
            image = image.convert("RGBA")

        # build levels (downscale /2) until fits
        levels: List[Image.Image] = [image]
        cur = image
        while max(cur.width, cur.height) > tile_size:
            new_w = max(1, (cur.width + 1) // 2)
            new_h = max(1, (cur.height + 1) // 2)
            cur = cur.resize((new_w, new_h), resample=Image.Resampling.LANCZOS)
            levels.append(cur)

        levels = list(reversed(levels))  # z=0 is smallest/top

        manifest_levels: Dict[int, LevelInfo] = {}

        for z, lvl_img in enumerate(levels):
            print("z = ", z)
            tiles_x = math.ceil(lvl_img.width / tile_size)
            tiles_y = math.ceil(lvl_img.height / tile_size)

            manifest_levels[z] = LevelInfo(
                z=z,
                width=lvl_img.width,
                height=lvl_img.height,
                tiles_x=tiles_x,
                tiles_y=tiles_y,
            )

            for y in range(tiles_y):
                print("y = ", y)
                for x in range(tiles_x):
                    left = x * tile_size
                    upper = y * tile_size
                    right = min(left + tile_size, lvl_img.width)
                    lower = min(upper + tile_size, lvl_img.height)

                    tile = lvl_img.crop((left, upper, right, lower))

                    # pad to full tile
                    if tile.size != (tile_size, tile_size):
                        canvas = Image.new("RGBA", (tile_size, tile_size), (0, 0, 0, 0))
                        canvas.paste(tile, (0, 0))
                        tile = canvas

                    data = self._encode(tile, fmt=fmt, lossless=lossless)
                    self.repo.put_tile(uuid, z, y, x, data=data, fmt=fmt)

        manifest = TileManifest(
            uuid=uuid,
            tile_size=tile_size,
            format=fmt,
            lossless=lossless,
            levels=manifest_levels,
        )

        manifest_json = self._manifest_to_json(manifest)
        self.repo.put_manifest(uuid, manifest_json)

        # invalidate cache for this uuid (manifest + tiles)
        self.invalidate_manifest(uuid)
        return manifest

    def _encode(self, tile: Image.Image, *, fmt: TileFormat, lossless: bool) -> bytes:
        import io
        buf = io.BytesIO()
        if fmt == "png":
            tile.save(buf, format="PNG", optimize=False)
        else:
            tile.save(buf, format="WEBP", lossless=lossless, quality=100, method=3)
        return buf.getvalue()

    def _manifest_to_json(self, manifest: TileManifest) -> bytes:
        obj = {
            "uuid": manifest.uuid,
            "tile_size": manifest.tile_size,
            "format": manifest.format,
            "lossless": manifest.lossless,
            "levels": {
                str(z): {
                    "z": li.z,
                    "width": li.width,
                    "height": li.height,
                    "tiles_x": li.tiles_x,
                    "tiles_y": li.tiles_y,
                }
                for z, li in manifest.levels.items()
            },
        }
        return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")

    # -------- manifest + tile read (cached) --------

    def get_manifest_dict(self, uuid: str) -> dict:
        man = self._get_manifest_cached(uuid)
        if not man:
            raise FileNotFoundError("Manifest not found")
        return man

    def _get_manifest_cached(self, uuid: str) -> Optional[dict]:
        mkey: ManifestKey = ("manifest", uuid)

        cached = self.cache.get(mkey)
        if cached is not None:
            try:
                return json.loads(cached.decode("utf-8"))
            except Exception:
                self.cache.delete(mkey)

        raw = self.repo.get_manifest(uuid)
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
        print("CACHE KEY:", key)
        print("CACHE STATS:", self.cache.stats())
        import os
        print("GET pid", os.getpid(), "cache_id", id(self.cache), "key", key)

        cached = self.cache.get(key)
        if cached is not None:
            return TileBytes(
                data=cached,
                media_type="image/webp" if fmt == "webp" else "image/png",
            )

        uri, stream = self.repo.open_tile(uuid, z, y, x, fmt=fmt)
        try:
            data = stream.read()
        finally:
            try:
                stream.close()
                getattr(stream, "release_conn", lambda: None)()
            except Exception:
                pass

        self.cache.set(key, data, size=len(data))
        return TileBytes(data=data, media_type="image/webp" if fmt == "webp" else "image/png")

    # -------- deletes + cache invalidation --------

    def delete_one_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: Optional[TileFormat] = None) -> TileFormat:
        # infer fmt from manifest if not provided
        if fmt is None:
            man = self.get_manifest_dict(uuid)
            fmt = man["format"]
            if fmt not in ("webp", "png"):
                raise ValueError("Invalid manifest format")

        self.repo.delete_tile(uuid, z, y, x, fmt=fmt)
        self.invalidate_one_tile(uuid, z, y, x, fmt)
        return fmt

    def delete_all_tiles(self, uuid: str) -> dict:
        stats = self.repo.delete_all_tiles(uuid)
        self.invalidate_manifest(uuid)
        # грубо можно чистить весь кеш, но лучше точечно:
        # (тайлы этого uuid мы не знаем какие именно — оставим как есть)
        return stats

    def delete_all_tiles_global(self) -> dict:
        stats = self.repo.delete_all_tiles_global()
        self.cache.clear()
        return stats

    def invalidate_one_tile(self, uuid: str, z: int, y: int, x: int, fmt: str) -> None:
        self.cache.delete((uuid, z, y, x, fmt))

    def invalidate_manifest(self, uuid: str) -> None:
        self.cache.delete(("manifest", uuid))


# один сервис TilesService, который:
#
# строит пирамиду
#
# отдаёт тайл с кешем
#
# отдаёт manifest
#
# удаляет тайлы/все тайлы/глобально
#
# инвалидирует кеш
# и API слой больше не лезет в репо напрямую.