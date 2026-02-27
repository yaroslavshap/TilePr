# app/api/admin_routes.py

from uuid import uuid4
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, Request
from fastapi.responses import StreamingResponse

from app.contracts.image_repository import ImageRepository
from app.contracts.metadata_repository import MetadataRepository

from app.services.original_image_service import OriginalImageService
from app.services.tile_build_queue import TileBuildQueue

from app.api.deps import get_tile_build_queue, get_jobs_repo, get_original_service, get_metadata_repo, get_image_repo, get_ingest_service
from app.api.schemas.images_list import ImageListResponse, ImageListItem

from app.repos.mongo_jobs_repo import MongoJobsRepository
from app.domain.tiles import TileFormat

from app.api.schemas.images import (
    IngestResponse,
    UploadOnlyResponse,
    ImageMetadataDTO,
    DeleteOneResponse,
    BulkOpResponse,
)

# ------------------- Ingest (upload + parse + mongo) -------------------

ingest_router = APIRouter(prefix="/ingest", tags=["INGEST = STORAGE + MINIO"])


@ingest_router.post("/images/{storage}/ingest", response_model=IngestResponse)
def ingest(
    storage: str,
    file: UploadFile = File(...),
    uuid: str | None = Query(default=None, description="Optional UUID for idempotency"),
    on_conflict: str = Query(default="error", pattern="^(error|overwrite|skip)$"),
):
    try:
        svc = get_ingest_service(storage)
    except Exception:
        raise HTTPException(status_code=400, detail="storage must be one of: fs, mem, s3")

    try:
        meta = svc.ingest(
            uuid=uuid,
            on_conflict=on_conflict,  # type: ignore[arg-type]
            filename=file.filename,
            content_type=file.content_type,
            upload_file_stream=file.file,
        )
    except FileExistsError as e:
        # on_conflict=error
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ingest failed: {e}")

    return IngestResponse(
        uuid=meta.uuid,
        uri=meta.uri,
        storage=meta.storage,
        name=meta.name,
        last_updated=meta.last_updated,
        content_type=meta.content_type,
        size_bytes=meta.size_bytes,
        width=meta.width,
        height=meta.height,
        format=meta.format,
        mode=meta.mode,
        bucket=meta.bucket,
        key=meta.key,
        path=meta.path,
    )

@ingest_router.delete("/purge", response_model=BulkOpResponse)
def admin_purge_all(
    batch_size: int = Query(1000, ge=1, le=5000),
    svc: OriginalImageService = Depends(get_original_service),
):
    res = svc.bulk_delete_fully(batch_size=batch_size)
    return BulkOpResponse(
        total=res.total,
        storage_deleted=res.storage_deleted,
        metadata_deleted=res.metadata_deleted,
        failed=res.failed,
        note="strict consistency: metadata deleted only after successful storage delete",
    )


# -------------------- STORAGE (original images) Upload only  --------------------

storage_router = APIRouter(prefix="/storage", tags=["Storage (MINIO)"])


def stream_minio(resp):
    """Если это MinIO HTTPResponse — надо корректно закрыть. Для fs/mem close сделает StreamingResponse."""
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



@storage_router.post("/images/{storage}/upload", response_model=UploadOnlyResponse)
def upload_only(
    storage: str,
    file: UploadFile = File(...),
    repo: ImageRepository = Depends(get_image_repo),
):
    # FastAPI не умеет автоматически прокинуть path-param в Depends без обёртки,
    # поэтому repo берём вручную:
    try:
        repo = get_image_repo(storage)
    except Exception:
        raise HTTPException(status_code=400, detail="storage must be one of: fs, mem, s3")

    from uuid import uuid4
    from app.domain.images import ImageId
    image_id = ImageId(str(uuid4()))

    try:
        loc = repo.upload(image_id, file.file, original_name=file.filename, content_type=file.content_type)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Upload failed: {e}")

    return UploadOnlyResponse(
        uuid=image_id.value,
        uri=loc.uri,
        storage=loc.storage,  # type: ignore[arg-type] если ругается mypy
        size_bytes=loc.size_bytes,
        content_type=loc.content_type,
    )


@storage_router.delete("/{uuid}", response_model=DeleteOneResponse)
def admin_delete_original_only(uuid: str, svc: OriginalImageService = Depends(get_original_service)):
    try:
        svc.delete_storage_only(uuid)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Metadata not found (cannot locate storage object)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Storage delete failed: {e}")

    return DeleteOneResponse(uuid=uuid, deleted="storage_only")



@storage_router.delete("", response_model=BulkOpResponse)
def admin_delete_all_originals_only(
    svc: OriginalImageService = Depends(get_original_service),
    batch_size: int = Query(1000, ge=1, le=5000),
):
    res = svc.bulk_delete_storage_only(batch_size=batch_size)
    return BulkOpResponse(
        total=res.total,
        storage_deleted=res.storage_deleted,
        failed=res.failed,
        note="metadata NOT deleted",
    )


# -------------------- METADATA - Meta / Download / Delete (через Mongo) --------------------

meta_router = APIRouter(prefix="/metadata", tags=["Metadata (Mongo)"])

@meta_router.get("/images/{uuid}/meta", response_model=ImageMetadataDTO)
def get_meta(uuid: str, svc: OriginalImageService = Depends(get_original_service)):
    try:
        meta = svc.get_metadata(uuid)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Metadata not found")

    return ImageMetadataDTO(
        uuid=meta.uuid,
        name=meta.name,
        last_updated=meta.last_updated,
        uri=meta.uri,
        storage=meta.storage,
        path=meta.path,
        bucket=meta.bucket,
        key=meta.key,
        content_type=meta.content_type,
        size_bytes=meta.size_bytes,
        width=meta.width,
        height=meta.height,
        format=meta.format,
        mode=meta.mode,
    )

@meta_router.get("/images/{uuid}")
def download_original(uuid: str, svc: OriginalImageService = Depends(get_original_service)):
    try:
        meta, loc, stream = svc.open_original(uuid)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Not found")

    # fs/mem: можно просто вернуть stream
    if meta.storage in ("fs", "mem"):
        return StreamingResponse(
            stream,
            media_type=meta.content_type or "application/octet-stream",
            headers={"Content-Disposition": f'inline; filename="{meta.name or meta.uuid}"'},
        )

    # s3: нужен правильный close/release_conn
    return StreamingResponse(
        stream_minio(stream),
        media_type=meta.content_type or "application/octet-stream",
        headers={"Content-Disposition": f'inline; filename="{meta.name or meta.uuid}"'},
    )


@meta_router.delete("/{uuid}", response_model=DeleteOneResponse)
def admin_delete_metadata_only(
    uuid: str,
    meta_repo: MetadataRepository = Depends(get_metadata_repo),
):
    """
    Удаляет только метаданные из Mongo.
    Оригинал в storage НЕ трогает
    """
    try:
        meta_repo.delete(uuid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Metadata delete failed: {e}")
    return DeleteOneResponse(uuid=uuid, deleted="metadata_only")


@meta_router.delete("", response_model=BulkOpResponse)
def admin_delete_all_metadata(
    svc: OriginalImageService = Depends(get_original_service),
):
    res = svc.bulk_delete_metadata_only()
    return BulkOpResponse(
        total=res.total,
        metadata_deleted=res.metadata_deleted,
        failed=res.failed,
        note="storage NOT deleted",
    )


@ingest_router.post("/images/{storage}/ingest2")
def ingest2(
    storage: str,
    file: UploadFile = File(...),
    uuid: str | None = Query(default=None, description="Optional UUID for idempotency"),
    on_conflict: str = Query(default="error", pattern="^(error|overwrite|skip)$"),

    # --- NEW: флаг + параметры билда тайлов ---
    build_tiles: bool = Query(default=False, description="If true — enqueue tiles build job"),
    tiles_tile_size: int = Query(256, description="256 or 512", alias="tiles_tile_size"),
    tiles_fmt: TileFormat = Query("webp", description="webp or png", alias="tiles_fmt"),
    tiles_lossless: bool = Query(False, alias="tiles_lossless"),

    q: TileBuildQueue = Depends(get_tile_build_queue),
    jobs: MongoJobsRepository = Depends(get_jobs_repo),
):
    try:
        svc = get_ingest_service(storage)
    except Exception:
        raise HTTPException(status_code=400, detail="storage must be one of: fs, mem, s3")

    try:
        meta = svc.ingest(
            uuid=uuid,
            on_conflict=on_conflict,  # type: ignore[arg-type]
            filename=file.filename,
            content_type=file.content_type,
            upload_file_stream=file.file,
        )
    except FileExistsError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ingest failed: {e}")

    job_info = None
    if build_tiles:
        if tiles_tile_size not in (256, 512):
            raise HTTPException(status_code=400, detail="tiles_tile_size must be 256 or 512")
        if tiles_fmt not in ("webp", "png"):
            raise HTTPException(status_code=400, detail="tiles_fmt must be webp or png")

        job_id = str(uuid4())
        payload = {
            "job_id": job_id,
            "uuid": meta.uuid,
            "tile_size": tiles_tile_size,
            "fmt": tiles_fmt,
            "lossless": tiles_lossless,
            "attempt": 0,
            "source": "ingest",
        }

        # статус в Mongo + публикация в Rabbit
        try:
            jobs.create(job_id=job_id, uuid=meta.uuid, payload=payload)
            q.publish_build(payload)
            job_info = {"job_id": job_id, "status": "queued"}
        except Exception as e:
            # ingestion уже успешен — просто возвращаем, что enqueue не удалось
            job_info = {"job_id": job_id, "status": "enqueue_failed", "error": str(e)}

    return {
        "uuid": meta.uuid,
        "uri": meta.uri,
        "storage": meta.storage,
        "name": meta.name,
        "last_updated": meta.last_updated.isoformat(),
        "content_type": meta.content_type,
        "size_bytes": meta.size_bytes,
        "width": meta.width,
        "height": meta.height,
        "format": meta.format,
        "mode": meta.mode,
        "bucket": meta.bucket,
        "key": meta.key,
        "path": meta.path,
        "tiles_job": job_info,  # NEW
    }


# -------------------- IMAGES LIST (metadata + url) --------------------

images_list_router = APIRouter(prefix="/images", tags=["Images list"])

@images_list_router.get("", response_model=ImageListResponse)
def list_images(
    request: Request,
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    meta_repo: MetadataRepository = Depends(get_metadata_repo),
):
    # meta_repo.list должен вернуть (items, total)
    metas, total = meta_repo.list(limit=limit, offset=offset)

    # У тебя выдача оригинала тут:
    # @meta_router.get("/images/{uuid}") -> download_original
    def build_original_url(uuid: str) -> str:
        return f"/metadata/images/{uuid}"

    items = [
        ImageListItem(
            uuid=m.uuid,
            name=m.name,
            last_updated=m.last_updated,
            storage=m.storage,
            content_type=m.content_type,
            size_bytes=m.size_bytes,
            width=m.width,
            height=m.height,
            format=m.format,
            mode=m.mode,
            original_url=build_original_url(m.uuid),
        )
        for m in metas
    ]

    has_more = (offset + limit) < total

    return ImageListResponse(
        items=items,
        limit=limit,
        offset=offset,
        total=total,
        has_more=has_more,
    )
