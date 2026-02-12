# app/services/tile_pyramid_builder.py

from __future__ import annotations
import math
import json
from typing import List
from PIL import Image

from app.domain.tiles import TileManifest, LevelInfo, TileFormat
from app.contracts.tiles_repository import TileRepository, ManifestRepository

class TilePyramidBuilder:
    def __init__(self, tile_repo: TileRepository, manifest_repo: ManifestRepository):
        self.tile_repo = tile_repo
        self.manifest_repo = manifest_repo

    def build(self, *, uuid: str, image: Image.Image, tile_size: int, fmt: TileFormat, lossless: bool = True) -> TileManifest:
        if tile_size not in (256, 512):
            raise ValueError("tile_size must be 256 or 512")

        # 1) Normalize mode for predictable output
        # (для lossless webp/PNG хорошо иметь RGBA, чтобы паддинг был прозрачным)
        if image.mode not in ("RGB", "RGBA"):
            image = image.convert("RGBA")
        elif image.mode == "RGB":
            image = image.convert("RGBA")

        # 2) Build pyramid levels (downscale by /2) until fits tile_size
        levels: List[Image.Image] = []
        cur = image
        levels.append(cur)

        while max(cur.width, cur.height) > tile_size:
            new_w = max(1, (cur.width + 1) // 2)
            new_h = max(1, (cur.height + 1) // 2)
            cur = cur.resize((new_w, new_h), resample=Image.Resampling.LANCZOS)
            levels.append(cur)

        # Now levels are from original -> smaller. Reverse so z=0 is top
        levels = list(reversed(levels))
        print("levels = ", len(levels))

        manifest_levels = {}

        # 3) Cut & save tiles for each level
        for z, lvl_img in enumerate(levels):
            print("\n\n\nz = ", z)
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
                print("y - ", y)
                for x in range(tiles_x):
                    print("x - ", x)
                    left = x * tile_size
                    upper = y * tile_size
                    right = min(left + tile_size, lvl_img.width)
                    lower = min(upper + tile_size, lvl_img.height)

                    tile = lvl_img.crop((left, upper, right, lower))

                    # pad to full tile_size
                    if tile.size != (tile_size, tile_size):
                        canvas = Image.new("RGBA", (tile_size, tile_size), (0, 0, 0, 0))
                        canvas.paste(tile, (0, 0))
                        tile = canvas

                    data = self._encode(tile, fmt=fmt, lossless=lossless)
                    # data = tile.tobytes()
                    # data = b''
                    # data = os.urandom(601 * 1024)
                    self.tile_repo.put_tile(uuid, z, y, x, data=data, fmt=fmt)

        manifest = TileManifest(
            uuid=uuid,
            tile_size=tile_size,
            format=fmt,
            lossless=lossless,
            levels=manifest_levels,
        )

        manifest_json = self._manifest_to_json(manifest)
        self.manifest_repo.put_manifest(uuid, manifest_json)

        return manifest

    def _encode(self, tile: Image.Image, *, fmt: TileFormat, lossless: bool) -> bytes:
        import io
        buf = io.BytesIO()
        if fmt == "png":
            tile.save(buf, format="PNG", optimize=False)
        else:
            # WebP lossless
            print(tile)
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
