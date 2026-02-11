# app/api/admin_routes.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.deps import get_original_service, get_metadata_repo
from app.services.original_image_service import OriginalImageService
from app.contracts.metadata_repository import MetadataRepository

router = APIRouter(prefix="/admin")


# -------------------- STORAGE (original images) --------------------

storage_router = APIRouter(prefix="/storage", tags=["Admin · Storage (Originals)"])


# app/api/admin_routes.py (замени только storage методы на эти)
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


# -------------------- METADATA (Mongo) --------------------

meta_router = APIRouter(prefix="/metadata", tags=["Admin · Metadata (Mongo)"])


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

router.include_router(storage_router)
router.include_router(meta_router)