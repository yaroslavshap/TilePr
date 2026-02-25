# app/services/original_image_service.py

from __future__ import annotations
from typing import BinaryIO, Tuple

from app.contracts.image_repository import ImageRepository
from app.contracts.metadata_repository import MetadataRepository
from app.domain.images import ImageLocation, ImageId
from app.domain.metadata import ImageMetadata


class OriginalImageService:
    def __init__(self, meta_repo: MetadataRepository, repo_resolver):
        self.meta_repo = meta_repo
        self.repo_resolver = repo_resolver  # callable(storage_kind)->ImageRepository

    def get_metadata(self, uuid: str) -> ImageMetadata:
        meta = self.meta_repo.get(uuid)
        if not meta:
            raise FileNotFoundError("metadata not found")
        return meta

    def _location_from_meta(self, meta: ImageMetadata) -> ImageLocation:
        return ImageLocation(
            uri=meta.uri,
            storage=meta.storage,
            path=meta.path,
            bucket=meta.bucket,
            key=meta.key,
            content_type=meta.content_type,
            size_bytes=meta.size_bytes,
        )

    def open_original(self, uuid: str) -> Tuple[ImageMetadata, ImageLocation, BinaryIO]:
        meta = self.get_metadata(uuid)
        repo = self.repo_resolver(meta.storage)
        loc = self._location_from_meta(meta)
        loc2, stream = repo.open_by_location(loc)
        return meta, loc2, stream

    def delete_original(self, uuid: str) -> None:
        """Удалить оригинал + метаданные (обычная ручка)."""
        meta = self.get_metadata(uuid)
        repo = self.repo_resolver(meta.storage)
        loc = self._location_from_meta(meta)
        repo.delete_by_location(loc, ImageId(uuid))
        self.meta_repo.delete(uuid)

    def delete_storage_only(self, uuid: str) -> None:
        """Удалить только оригинал в хранилище, метаданные не трогать."""
        meta = self.get_metadata(uuid)
        repo = self.repo_resolver(meta.storage)
        loc = self._location_from_meta(meta)
        repo.delete_by_location(loc, ImageId(uuid))


