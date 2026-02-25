# app/domain/metadata.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from app.domain.images import StorageKind

@dataclass(frozen=True)
class ImageMetadata:
    # минимум по ТЗ
    uuid: str
    name: Optional[str]
    last_updated: datetime

    # инвариант
    uri: str
    storage: StorageKind

    # storage details
    path: Optional[str] = None
    bucket: Optional[str] = None
    key: Optional[str] = None

    # полезные поля
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None

    # Pillow probe
    width: Optional[int] = None
    height: Optional[int] = None
    format: Optional[str] = None
    mode: Optional[str] = None

