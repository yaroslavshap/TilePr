# app/services/original_image_service.py
from dataclasses import dataclass
from typing import BinaryIO, Tuple, Callable

from app.contracts.metadata_repository import MetadataRepository
from app.contracts.image_repository import ImageRepository
from app.domain.images import ImageLocation, ImageId, StorageKind
from app.domain.metadata import ImageMetadata


@dataclass(frozen=True)
class BulkOpResult:
    total: int
    storage_deleted: int
    metadata_deleted: int
    failed: int


#  repo_resolver у тебя не репозиторий, а функция, которая выбирает репозиторий по storage.
# repo_resolver — это callable, который принимает "fs"|"s3"|"mem" и возвращает нужный ImageRepository.

class OriginalImageService:
    def __init__(self, meta_repo: MetadataRepository, repo_resolver: Callable[[StorageKind], ImageRepository]):
        self.meta_repo = meta_repo
        self.repo_resolver = repo_resolver

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


    def bulk_delete_storage_only(self, *, batch_size: int = 1000) -> BulkOpResult:
        """Удалить все объекты в storage, метаданные оставить."""
        storage_deleted = 0
        failed = 0
        total = 0
        for uuid in self.meta_repo.iter_uuids(batch_size=batch_size):
            total += 1
            try:
                self.delete_storage_only(uuid)
                storage_deleted += 1
            except Exception:
                failed += 1
        return BulkOpResult(
            total=total,
            storage_deleted=storage_deleted,
            metadata_deleted=0,
            failed=failed,
        )

    def bulk_delete_metadata_only(self) -> BulkOpResult:
        """Удалить все метаданные из Mongo, storage не трогать."""
        deleted = self.meta_repo.delete_all()
        return BulkOpResult(
            total=deleted,
            storage_deleted=0,
            metadata_deleted=deleted,
            failed=0,
        )

    def bulk_delete_fully(self, *, batch_size: int = 1000) -> BulkOpResult:
        """
        Строгая согласованность:
        - сначала удаляем объект из storage
        - только если storage удалился успешно — удаляем metadata
        - если storage не удалился — metadata НЕ трогаем
        """
        storage_deleted = 0
        metadata_deleted = 0
        failed = 0
        total = 0

        for uuid in self.meta_repo.iter_uuids(batch_size=batch_size):
            total += 1

            # 1) storage
            try:
                self.delete_storage_only(uuid)
                storage_deleted += 1
            except Exception:
                failed += 1
                continue  # ВАЖНО: метаданные не удаляем

            # 2) metadata (только после успешного storage)
            try:
                self.meta_repo.delete(uuid)
                metadata_deleted += 1
            except Exception:
                # storage уже удалили, а мета — нет: это плохо, фиксируем как failed
                failed += 1

        return BulkOpResult(
            total=total,
            storage_deleted=storage_deleted,
            metadata_deleted=metadata_deleted,
            failed=failed,
        )




