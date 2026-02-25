# app/api/tiles_routes.py

from __future__ import annotations
import json
from PIL import Image

from app.api.deps import get_original_service, get_tile_repo, get_tile_builder
from app.services.original_image_service import OriginalImageService
from app.services.tile_pyramid_builder import TilePyramidBuilder
from app.domain.tiles import TileFormat

from fastapi.responses import Response
from app.services.tiled_image_service import TiledImageService
from app.api.deps import get_tiled_image_service

from app.api.deps import get_tiles_cache, TILES_BACKEND, TILES_BUCKET, TILES_FS_DIR
from app.utils.ttl_cache import InMemoryTTLCache


from uuid import uuid4
from fastapi import HTTPException, Depends, Query, APIRouter
from app.api.deps import get_tile_build_queue, get_jobs_repo
from app.services.tile_build_queue import TileBuildQueue
from app.repos.mongo_jobs_repo import MongoJobsRepository


import tempfile, os

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



@tiles_router.get("/_cache/stats")
def cache_stats(
    cache: InMemoryTTLCache[object, bytes] = Depends(get_tiles_cache),
):
    """
    Админ-ручка: состояние in-memory TTL кеша тайлов.
    """
    stats = cache.stats()

    return {
        "status": "ok",
        "backend": TILES_BACKEND,
        "storage": (
            {"bucket": TILES_BUCKET} if TILES_BACKEND == "s3"
            else {"directory": TILES_FS_DIR}
        ),
        "cache": stats,
    }


@tiles_router.get("/jobs/{job_id}")
def get_job(job_id: str, jobs: MongoJobsRepository = Depends(get_jobs_repo)):
    doc = jobs.get(job_id)
    if not doc:
        raise HTTPException(status_code=404, detail="job not found")
    return doc

# GET /tiles/jobs/_stats → сколько всего job-ов и разбивка по статусам
@tiles_router.get("/jobs/_stats")
def jobs_stats(jobs: MongoJobsRepository = Depends(get_jobs_repo)):
    return {
        "status": "ok",
        "total": jobs.count(),
        "by_status": jobs.count_by_status(),
    }

# GET /tiles/jobs/by-uuid/<uuid>?limit=50 → последние job-ы по конкретному изображению
@tiles_router.get("/jobs/by-uuid/{uuid}")
def jobs_by_uuid(uuid: str, jobs: MongoJobsRepository = Depends(get_jobs_repo), limit: int = Query(50, ge=1, le=500)):
    return {
        "status": "ok",
        "uuid": uuid,
        "items": jobs.find_by_uuid(uuid, limit=limit),
    }


@tiles_router.post("/{uuid}/build-async")
def build_tiles_async(
    uuid: str,
    tile_size: int = Query(256, description="256 or 512"),
    fmt: TileFormat = Query("webp", description="webp or png"),
    lossless: bool = Query(False),
    q: TileBuildQueue = Depends(get_tile_build_queue),
    jobs: MongoJobsRepository = Depends(get_jobs_repo),
):
    if tile_size not in (256, 512):
        raise HTTPException(status_code=400, detail="tile_size must be 256 or 512")

    job_id = str(uuid4())

    payload = {
        "job_id": job_id,
        "uuid": uuid,
        "tile_size": tile_size,
        "fmt": fmt,
        "lossless": lossless,
        "attempt": 0,
    }

    jobs.create(job_id=job_id, uuid=uuid, payload=payload)
    q.publish_build(payload)

    return {"status": "queued", "job_id": job_id, "uuid": uuid}







@tiles_router.post("/{uuid}/build")
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
            manifest = builder.build(uuid=uuid, image=im, tile_size=tile_size, fmt=fmt, lossless=False)

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


# @tiles_router.get("/{uuid}/{z}/{y}/{x}")
# def get_tile(
#     uuid: str,
#     z: int,
#     y: int,
#     x: int,
#     repo = Depends(get_tile_repo),
# ):
#     """
#     #4: Доступ к тайлу по координатам (z, y, x)
#     """
#     # 1) проверим по manifest границы
#     manifest_bytes = repo.get_manifest(uuid)
#     if not manifest_bytes:
#         raise HTTPException(status_code=404, detail="Manifest not found (tiles not built)")
#
#     try:
#         man = json.loads(manifest_bytes.decode("utf-8"))
#         fmt = man["format"]
#         levels = man["levels"]
#         lvl = levels.get(str(z))
#         if not lvl:
#             raise HTTPException(status_code=404, detail="Level z not found")
#         if not (0 <= x < lvl["tiles_x"] and 0 <= y < lvl["tiles_y"]):
#             raise HTTPException(status_code=404, detail="Tile out of range")
#     except HTTPException:
#         raise
#     except Exception:
#         raise HTTPException(status_code=500, detail="Corrupted manifest")
#
#     # 2) открыть тайл
#     try:
#         uri, stream = repo.open_tile(uuid, z, y, x, fmt=fmt)
#     except Exception:
#         raise HTTPException(status_code=404, detail="Tile not found")
#
#     media = "image/webp" if fmt == "webp" else "image/png"
#
#     # S3 stream требует закрытия
#     if uri.startswith("minio://"):
#         return StreamingResponse(_stream_minio(stream), media_type=media)
#
#     # FS stream
#     return StreamingResponse(stream, media_type=media)
#


@tiles_router.get("/{uuid}/{z}/{y}/{x}")
def get_tile(
    uuid: str,
    z: int,
    y: int,
    x: int,
    svc: TiledImageService = Depends(get_tiled_image_service),
):
    """
    #4: Доступ к тайлу по координатам (z, y, x) + кеширование (TTL)
    """
    try:
        tb = svc.get_tile_bytes(uuid, z, y, x)
        return Response(content=tb.data, media_type=tb.media_type)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tile read failed: {e}")





@tiles_router.get("/{uuid}/manifest")
def admin_get_manifest(uuid: str, repo=Depends(get_tile_repo)):
    data = repo.get_manifest(uuid)
    if not data:
        raise HTTPException(status_code=404, detail="Manifest not found")
    return json.loads(data.decode("utf-8"))


@tiles_router.delete("/{uuid}/{z}/{y}/{x}")
def admin_delete_one_tile(
    uuid: str,
    z: int,
    y: int,
    x: int,
    fmt: str | None = Query(default=None, description="Override format (webp/png). If not set, uses manifest."),
    repo=Depends(get_tile_repo),
):
    """
    Удалить один тайл.
    Если fmt не задан — берём формат из manifest.json
    """
    if fmt is None:
        man_bytes = repo.get_manifest(uuid)
        if not man_bytes:
            raise HTTPException(status_code=404, detail="Manifest not found (cannot infer format)")
        try:
            man = json.loads(man_bytes.decode("utf-8"))
            fmt = man["format"]
        except Exception:
            raise HTTPException(status_code=500, detail="Corrupted manifest")

    if fmt not in ("webp", "png"):
        raise HTTPException(status_code=400, detail="fmt must be webp or png")

    try:
        repo.delete_tile(uuid, z, y, x, fmt=fmt)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Tile delete failed: {e}")
    try:
        svc = get_tiled_image_service()  # можно так, либо через Depends отдельным параметром
        svc.invalidate_one_tile(uuid, z, y, x, fmt)
    except Exception:
        pass

    return {"status": "ok", "uuid": uuid, "deleted": "tile", "z": z, "y": y, "x": x, "fmt": fmt}


@tiles_router.delete("/{uuid}")
def admin_delete_all_tiles(uuid: str, repo=Depends(get_tile_repo)):
    """
    Удалить все тайлы по uuid (включая manifest).
    """
    try:
        stats = repo.delete_all_tiles(uuid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk delete failed: {e}")
    try:
        svc = get_tiled_image_service()
        svc.invalidate_manifest(uuid)
        # грубо и гарантированно убрать старые тайлы этого uuid:
        # get_tiles_cache().clear()
    except Exception:
        pass

    return {"status": "ok", "uuid": uuid, "deleted": "all_tiles", **stats}


@tiles_router.delete("")
def admin_delete_all_tiles_global(repo=Depends(get_tile_repo)):
    """
    Удалить ВСЕ тайлы ВСЕХ изображений (очень опасно).
    """
    if not hasattr(repo, "delete_all_tiles_global"):
        raise HTTPException(status_code=500, detail="Tile repository doesn't support global delete")

    try:
        stats = repo.delete_all_tiles_global()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Global tiles delete failed: {e}")

    return {"status": "ok", "deleted": "all_tiles_global", **stats}



@tiles_router.delete("/_cache")
def clear_cache(
    cache: InMemoryTTLCache[object, bytes] = Depends(get_tiles_cache),
):
    cache.clear()
    return {"status": "ok", "message": "cache cleared"}


