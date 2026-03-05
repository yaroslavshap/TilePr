# app/repos/mem_image_repo.py


from io import BytesIO
from typing import Optional, BinaryIO, Tuple

from app.domain.images_domain import ImageId, ImageLocation

class InMemoryImageRepository:
    def __init__(self, max_bytes: int):
        self.max_bytes = max_bytes
        self._store: dict[str, tuple[bytes, Optional[str], Optional[str]]] = {}
        # uuid -> (data, filename, content_type)

    def storage_kind(self) -> str:
        return "mem"

    def upload(self, image_id: ImageId, src: BinaryIO, *, original_name: Optional[str], content_type: Optional[str]) -> ImageLocation:
        data = src.read(self.max_bytes + 1)
        if len(data) > self.max_bytes:
            raise ValueError(f"InMemory limit exceeded: {self.max_bytes} bytes")
        self._store[image_id.value] = (data, original_name, content_type)
        return ImageLocation(
            uri=f"mem://images/{image_id.value}",
            storage="mem",
            content_type=content_type,
            size_bytes=len(data),
        )

    def open_by_location(self, loc: ImageLocation) -> Tuple[ImageLocation, BinaryIO]:
        # loc.uri хранит uuid, но мы используем uuid отдельно в service при delete/open
        uuid = loc.uri.rsplit("/", 1)[-1]
        if uuid not in self._store:
            raise FileNotFoundError(uuid)
        data, _, ct = self._store[uuid]
        return ImageLocation(uri=loc.uri, storage="mem", content_type=ct, size_bytes=len(data)), BytesIO(data)

    def delete_by_location(self, loc: ImageLocation, image_id: ImageId) -> None:
        self._store.pop(image_id.value, None)



