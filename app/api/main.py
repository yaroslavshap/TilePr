from __future__ import annotations
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse

from app.api.deps import get_image_repo, get_ingest_service, get_original_service
from app.contracts.image_repository import ImageRepository
from app.services.ingest_service import IngestService
from app.services.original_image_service import OriginalImageService
from app.api.admin_routes import router as admin_router
from app.api.tiles_routes import router as tiles_router


app = FastAPI(title="Original Images (Clean DI + Contracts)")
app.include_router(admin_router)
app.include_router(tiles_router)

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


# ------------------- Upload only (без Mongo) -------------------

@app.post("/images/{storage}/upload")
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


# ------------------- Ingest (upload + parse + mongo) -------------------

# app/api/main.py (замени функцию ingest)
from fastapi import Query

@app.post("/images/{storage}/ingest")
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



# ------------------- Meta / Download / Delete (через Mongo) -------------------

@app.get("/images/{uuid}/meta")
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


@app.get("/images/{uuid}")
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


@app.delete("/images/{uuid}")
def delete_original(uuid: str, svc: OriginalImageService = Depends(get_original_service)):
    try:
        svc.delete_original(uuid)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")
    return {"status": "ok", "uuid": uuid}
