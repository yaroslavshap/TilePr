from __future__ import annotations
from typing import Protocol, Optional, BinaryIO, Tuple
from app.domain.images import ImageId, ImageLocation

class ImageRepository(Protocol):
    def storage_kind(self) -> str: ...

    def upload(
        self,
        image_id: ImageId,
        src: BinaryIO,
        *,
        original_name: Optional[str],
        content_type: Optional[str],
    ) -> ImageLocation:
        ...

    def open_by_location(self, loc: ImageLocation) -> Tuple[ImageLocation, BinaryIO]:
        """Открыть поток чтения по location (fs: path, s3: bucket/key, mem: uuid)."""
        ...

    def delete_by_location(self, loc: ImageLocation, image_id: ImageId) -> None:
        """Удалить оригинал по location."""
        ...
