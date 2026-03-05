# app/domain/images.py

from dataclasses import dataclass
from typing import Optional, Literal

StorageKind = Literal["fs", "mem", "s3"]

@dataclass(frozen=True)
class ImageId:
    value: str

@dataclass(frozen=True)
class ImageLocation:
    uri: str
    storage: StorageKind

    # fs
    path: Optional[str] = None

    # s3
    bucket: Optional[str] = None
    key: Optional[str] = None

    content_type: Optional[str] = None
    size_bytes: Optional[int] = None
