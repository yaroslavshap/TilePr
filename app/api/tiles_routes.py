# app/api/tiles_routes.py

from __future__ import annotations
import json
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from PIL import Image

from app.api.deps import get_original_service, get_tile_repo, get_tile_builder
from app.services.original_image_service import OriginalImageService
from app.services.tile_pyramid_builder import TilePyramidBuilder
from app.domain.tiles import TileFormat

import tempfile, os

router = APIRouter(prefix="/tiles", tags=["Tiles · Pyramid & Access"])


def _stream_minio(resp):
    try:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            yield chunk
    finally:
        try:
            resp.close()
            resp.release_conn()
        except Exception:
            pass


@router.post("/{uuid}/build")
def build_tiles(
    uuid: str,
    tile_size: int = Query(256, description="256 or 512"),
    fmt: TileFormat = Query("webp", description="webp or png"),
    svc: OriginalImageService = Depends(get_original_service),
    builder: TilePyramidBuilder = Depends(get_tile_builder),
):
    """
    #3: Препроцессинг: строим пирамиду тайлов и сохраняем в репозиторий + manifest.json
    """
    # 1) открыть оригинал через метаданные (Mongo -> storage)
    try:
        meta, loc, stream = svc.open_original(uuid)
        print(meta, loc, stream, sep="\n\n\n")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Original not found")

    # 2) Pillow лучше работает с файлом/BytesIO. Спулим в память НЕ надо для 250MB.
    # Сделаем временный файл.
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".img") as tmp:
            tmp_path = tmp.name
            if meta.storage == "s3":
                for chunk in _stream_minio(stream):
                    tmp.write(chunk)
            else:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp.write(chunk)
            tmp.flush()
        print("tmp_path", tmp_path)

        with Image.open(tmp_path) as im:
            im.load()
            print("im.size", im.size)
            manifest = builder.build(uuid=uuid, image=im, tile_size=tile_size, fmt=fmt, lossless=True)

        return {
            "uuid": manifest.uuid,
            "tile_size": manifest.tile_size,
            "format": manifest.format,
            "lossless": manifest.lossless,
            "levels": {str(z): vars(li) for z, li in manifest.levels.items()},
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Build failed: {e}")
    finally:
        try:
            stream.close()
            getattr(stream, "release_conn", lambda: None)()
        except Exception:
            pass
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


@router.get("/{uuid}/{z}/{y}/{x}")
def get_tile(
    uuid: str,
    z: int,
    y: int,
    x: int,
    repo = Depends(get_tile_repo),
):
    """
    #4: Доступ к тайлу по координатам (z, y, x)
    """
    # 1) проверим по manifest границы
    manifest_bytes = repo.get_manifest(uuid)
    if not manifest_bytes:
        raise HTTPException(status_code=404, detail="Manifest not found (tiles not built)")

    try:
        man = json.loads(manifest_bytes.decode("utf-8"))
        fmt = man["format"]
        levels = man["levels"]
        lvl = levels.get(str(z))
        if not lvl:
            raise HTTPException(status_code=404, detail="Level z not found")
        if not (0 <= x < lvl["tiles_x"] and 0 <= y < lvl["tiles_y"]):
            raise HTTPException(status_code=404, detail="Tile out of range")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Corrupted manifest")

    # 2) открыть тайл
    try:
        uri, stream = repo.open_tile(uuid, z, y, x, fmt=fmt)
    except Exception:
        raise HTTPException(status_code=404, detail="Tile not found")

    media = "image/webp" if fmt == "webp" else "image/png"

    # S3 stream требует закрытия
    if uri.startswith("minio://"):
        return StreamingResponse(_stream_minio(stream), media_type=media)

    # FS stream
    return StreamingResponse(stream, media_type=media)
