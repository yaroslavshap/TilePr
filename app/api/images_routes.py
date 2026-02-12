# app/api/admin_routes.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from app.api.deps import get_original_service, get_metadata_repo, get_image_repo, get_ingest_service
from app.contracts.image_repository import ImageRepository
from app.services.original_image_service import OriginalImageService
from app.contracts.metadata_repository import MetadataRepository


# ------------------- Ingest (upload + parse + mongo) -------------------

ingest_router = APIRouter(prefix="/ingest", tags=["INGEST = STORAGE + MINIO"])


@ingest_router.post("/images/{storage}/ingest")
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
    }




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



@storage_router.post("/images/{storage}/upload")
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

    return {"uuid": image_id.value, "uri": loc.uri, "storage": loc.storage, "size_bytes": loc.size_bytes, "content_type": loc.content_type}



@storage_router.delete("/{uuid}")
def admin_delete_original_only(uuid: str, svc: OriginalImageService = Depends(get_original_service)):
    try:
        svc.delete_storage_only(uuid)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Metadata not found (cannot locate storage object)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Storage delete failed: {e}")

    return {"status": "ok", "uuid": uuid, "deleted": "storage_only"}



@storage_router.delete("")
def admin_delete_all_originals_only(
    meta_repo: MetadataRepository = Depends(get_metadata_repo),
    svc: OriginalImageService = Depends(get_original_service),
):
    if not hasattr(meta_repo, "col"):
        raise HTTPException(status_code=500, detail="MetadataRepository doesn't expose Mongo collection for bulk ops")

    deleted = 0
    failed = 0

    cursor = meta_repo.col.find({}, {"uuid": 1})  # type: ignore[attr-defined]
    for doc in cursor:
        uuid = doc.get("uuid")
        if not uuid:
            continue
        try:
            svc.delete_storage_only(uuid)
            deleted += 1
        except Exception:
            failed += 1

    return {"status": "ok", "deleted": deleted, "failed": failed, "note": "metadata NOT deleted"}





# -------------------- METADATA - Meta / Download / Delete (через Mongo) --------------------

meta_router = APIRouter(prefix="/metadata", tags=["Metadata (Mongo)"])

@meta_router.get("/images/{uuid}/meta")
def get_meta(uuid: str, svc: OriginalImageService = Depends(get_original_service)):
    try:
        meta = svc.get_metadata(uuid)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Metadata not found")

    return {
        "uuid": meta.uuid,
        "name": meta.name,
        "last_updated": meta.last_updated.isoformat(),
        "uri": meta.uri,
        "storage": meta.storage,
        "path": meta.path,
        "bucket": meta.bucket,
        "key": meta.key,
        "content_type": meta.content_type,
        "size_bytes": meta.size_bytes,
        "width": meta.width,
        "height": meta.height,
        "format": meta.format,
        "mode": meta.mode,
    }


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


@meta_router.delete("/{uuid}")
def admin_delete_metadata_only(
    uuid: str,
    meta_repo: MetadataRepository = Depends(get_metadata_repo),
):
    """
    Удаляет только метаданные из Mongo.
    Оригинал в storage НЕ трогает (останется “сирота” в хранилище).
    """
    try:
        meta_repo.delete(uuid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Metadata delete failed: {e}")
    return {"status": "ok", "uuid": uuid, "deleted": "metadata_only"}


@meta_router.delete("")
def admin_delete_all_metadata(
    meta_repo: MetadataRepository = Depends(get_metadata_repo),
):
    """
    Удаляет ВСЕ метаданные из Mongo (всю коллекцию).
    Оригиналы в storage НЕ трогаются.
    """
    if not hasattr(meta_repo, "col"):
        raise HTTPException(status_code=500, detail="MetadataRepository doesn't expose Mongo collection for bulk ops")

    try:
        res = meta_repo.col.delete_many({})  # type: ignore[attr-defined]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Metadata bulk delete failed: {e}")

    return {"status": "ok", "deleted_count": getattr(res, "deleted_count", None)}
