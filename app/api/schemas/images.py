# app/api/schemas/images_domain.py


from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field

from app.domain.images_domain import StorageKind

OnConflict = Literal["error", "overwrite", "skip"]

class ImageMetadataDTO(BaseModel):
    uuid: str
    name: Optional[str] = None
    last_updated: datetime

    uri: str
    storage: StorageKind

    path: Optional[str] = None
    bucket: Optional[str] = None
    key: Optional[str] = None

    content_type: Optional[str] = None
    size_bytes: Optional[int] = None

    width: Optional[int] = None
    height: Optional[int] = None
    format: Optional[str] = None
    mode: Optional[str] = None


class IngestResponse(ImageMetadataDTO):
    """Ответ на ingest: по сути это метаданные созданного изображения."""
    pass


class UploadOnlyResponse(BaseModel):
    uuid: str
    uri: str
    storage: StorageKind
    size_bytes: Optional[int] = None
    content_type: Optional[str] = None


class DeleteOneResponse(BaseModel):
    status: Literal["ok"] = "ok"
    uuid: str
    deleted: str  # "storage_only" | "metadata_only" | "fully"


class BulkOpResponse(BaseModel):
    status: Literal["ok"] = "ok"
    total: int
    storage_deleted: int = 0
    metadata_deleted: int = 0
    failed: int = 0
    note: Optional[str] = None