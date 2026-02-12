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


# Я заметил, просмотривая тайлы в минио что некоторые из них не квадратные.
# Мне нужно чтобы все тайлы были квадратные. То есть все тайлы на всех уровнях были размером 256 на 256.
# Если на како - то тайл не хватает пикселей, то их нужно дорисовывать
# Вот мой код:

