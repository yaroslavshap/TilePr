# # app/api/tiles_routes.py
import os
import tempfile
from PIL import Image

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from fastapi import Query

from app.api.deps import (
    get_original_service,
    get_tiles_service,
    get_tiles_cache,
)
from app.services.original_image_service import OriginalImageService
from app.services.tiles_service import TilesService
from app.utils.ttl_cache import InMemoryTTLCache

from app.api.schemas.tiles import (
    CacheStatsResponse, CacheStatsDTO,
    BuildTilesRequest, BuildTilesResponse, TileManifestDTO, LevelInfoDTO,
    DeleteOneTileResponse, DeleteAllTilesResponse, DeleteAllTilesGlobalResponse,
    ClearCacheResponse, BulkDeleteStats,
)
from app.domain.tiles_domain import TileFormat
from config import settings

tiles_router = APIRouter(prefix="/tiles", tags=["TILES"])

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

@tiles_router.get("/_cache/stats", response_model=CacheStatsResponse)
def cache_stats(
    cache: InMemoryTTLCache[object, bytes] = Depends(get_tiles_cache),
):
    stats = cache.stats()
    storage = {"bucket": settings.TILES_BUCKET} if settings.TILES_BACKEND == "s3" else {"directory": settings.TILES_FS_DIR}

    return CacheStatsResponse(
        backend=settings.TILES_BACKEND,
        storage=storage,
        cache=CacheStatsDTO(**stats),
    )

@tiles_router.post("/_cache/reset-metrics")
def reset_cache_metrics(cache: InMemoryTTLCache[object, bytes] = Depends(get_tiles_cache)):
    before = cache.stats()
    cache.reset_metrics()
    after = cache.stats()
    return {"before": before, "after": after}


# ======== NEW ========
@tiles_router.delete("/cache")
def clear_cache1(
    cache: InMemoryTTLCache[object, bytes] = Depends(get_tiles_cache),
):
    before = cache.stats()
    cache.clear()
    after = cache.stats()
    print("CACHE STATS:", before, after)
    print("cache_id ",  id(cache))
    return {
        "message": "cache cleared",
        "pid": os.getpid(),
        "cache_id": id(cache),
        "before": before,
        "after": after,
    }
# ======== NEW ========

@tiles_router.post("/{uuid}/build", response_model=BuildTilesResponse)
def build_tiles(
    uuid: str,
    req: BuildTilesRequest = Depends(),  # query params as model
    original: OriginalImageService = Depends(get_original_service),
    tiles: TilesService = Depends(get_tiles_service),
):
    """
    Строим пирамиду тайлов + manifest.
    """
    meta, loc, stream = original.open_original(uuid)

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

        with Image.open(tmp_path) as im:
            im.load()
            manifest = tiles.build_pyramid(
                uuid=uuid,
                image=im,
                tile_size=req.tile_size,
                fmt=req.fmt,
                lossless=req.lossless,
            )

        return BuildTilesResponse(
            uuid=manifest.uuid,
            tile_size=manifest.tile_size,
            format=manifest.format,
            lossless=manifest.lossless,
            levels={
                z: LevelInfoDTO(
                    z=li.z, width=li.width, height=li.height, tiles_x=li.tiles_x, tiles_y=li.tiles_y
                )
                for z, li in manifest.levels.items()
            },
        )

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


@tiles_router.get("/{uuid}/{z:int}/{y:int}/{x:int}")
def get_tile(
    uuid: str,
    z: int,
    y: int,
    x: int,
    tiles: TilesService = Depends(get_tiles_service),
):
    tb = tiles.get_tile_bytes(uuid, z, y, x)
    return Response(content=tb.data, media_type=tb.media_type)


@tiles_router.get("/{uuid}/manifest", response_model=TileManifestDTO)
def get_manifest(uuid: str, tiles: TilesService = Depends(get_tiles_service)):
    man = tiles.get_manifest_dict(uuid)

    return TileManifestDTO(
        uuid=man["uuid"],
        tile_size=int(man["tile_size"]),
        format=man["format"],
        lossless=bool(man["lossless"]),
        levels={
            int(z): LevelInfoDTO(
                z=int(li["z"]),
                width=int(li["width"]),
                height=int(li["height"]),
                tiles_x=int(li["tiles_x"]),
                tiles_y=int(li["tiles_y"]),
            )
            for z, li in man["levels"].items()
        },
    )

@tiles_router.delete("/{uuid}/{z}/{y}/{x}", response_model=DeleteOneTileResponse)
def delete_one_tile(
    uuid: str,
    z: int,
    y: int,
    x: int,
    fmt: TileFormat | None = Query(default=None, description="Override format (webp/png). If not set, uses manifest."),
    tiles: TilesService = Depends(get_tiles_service),
):
    real_fmt = tiles.delete_one_tile(uuid, z, y, x, fmt=fmt)
    return DeleteOneTileResponse(uuid=uuid, z=z, y=y, x=x, fmt=real_fmt)

@tiles_router.delete("/{uuid}", response_model=DeleteAllTilesResponse)
def delete_all_tiles(
    uuid: str,
    tiles: TilesService = Depends(get_tiles_service),
):
    stats = tiles.delete_all_tiles(uuid)
    return DeleteAllTilesResponse(
        uuid=uuid,
        stats=BulkDeleteStats(deleted=int(stats.get("deleted", 0)), failed=int(stats.get("failed", 0))),
    )

@tiles_router.delete("", response_model=DeleteAllTilesGlobalResponse)
def delete_all_tiles_global(
    tiles: TilesService = Depends(get_tiles_service),
):
    stats = tiles.delete_all_tiles_global()
    return DeleteAllTilesGlobalResponse(
        stats=BulkDeleteStats(deleted=int(stats.get("deleted", 0)), failed=int(stats.get("failed", 0))),
    )
