# app/repos/mongo_metadata_repo.py

from typing import Optional, Sequence, Tuple, Iterable
from pymongo.collection import Collection
from app.domain.metadata_domain import ImageMetadata
from pymongo import errors as pymongo_errors
from app.exceptions.repo_errors import MetadataDBError, MetadataConflictError, MetadataDataError

class MongoDBMetadataRepository:
    def __init__(self, collection: Collection):
        self.col = collection
        try:
            self.col.create_index("uuid", unique=True)
        except pymongo_errors.PyMongoError as e:
            raise MetadataDBError(f"Не удалось создать индекс для uuid: {e}") from e


    def upsert(self, meta: ImageMetadata) -> None:
        doc = self._to_doc(meta)
        try:
            self.col.update_one({"uuid": meta.uuid}, {"$set": doc}, upsert=True)
        except pymongo_errors.DuplicateKeyError as e:
            raise MetadataConflictError(f"Метаданные с uuid уже существуют: {meta.uuid}") from e
        except pymongo_errors.PyMongoError as e:
            raise MetadataDBError(f"Ошибка базы данных при сохранении метаданных: {e}") from e

    def get(self, uuid: str) -> Optional[ImageMetadata]:
        try:
            doc = self.col.find_one({"uuid": uuid})
        except pymongo_errors.PyMongoError as e:
            raise MetadataDBError(f"Ошибка базы данных при получении метаданных: {e}") from e
        return self._from_doc(doc) if doc else None

    def delete(self, uuid: str) -> None:
        try:
            self.col.delete_one({"uuid": uuid})
        except pymongo_errors.PyMongoError as e:
            raise MetadataDBError(f"Ошибка базы данных при удалении метаданных: {e}") from e

    def list(self, *, limit: int, offset: int) -> Tuple[Sequence[ImageMetadata], int]:
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        try:
            total = self.col.count_documents({})
            docs = list(
                self.col.find({})
                .sort("_id", -1)
                .skip(offset)
                .limit(limit)
            )
        except pymongo_errors.PyMongoError as e:
            raise MetadataDBError(f"Ошибка базы данных при получении списка метаданных: {e}") from e

        return [self._from_doc(d) for d in docs], total


    def iter_uuids(self, *, batch_size: int = 1000) -> Iterable[str]:
        batch_size = max(1, min(int(batch_size), 5000))
        cur = self.col.find({}, {"uuid": 1}).batch_size(batch_size)
        for doc in cur:
            u = doc.get("uuid")
            if u:
                yield u

    def delete_all(self) -> int:
        try:
            res = self.col.delete_many({})
            return int(getattr(res, "deleted_count", 0))
        except pymongo_errors.PyMongoError as e:
            raise MetadataDBError(f"Ошибка базы данных при удалении всех метаданных: {e}") from e


    # --- mappers ---

    def _to_doc(self, meta: ImageMetadata) -> dict:
        return {
            "uuid": meta.uuid,
            "name": meta.name,
            "last_updated": meta.last_updated,
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

    def _from_doc(self, doc: dict) -> ImageMetadata:
        # doc["..."] — обязательные поля, doc.get — опциональные
        return ImageMetadata(
            uuid=doc["uuid"],
            name=doc.get("name"),
            last_updated=doc["last_updated"],
            uri=doc["uri"],
            storage=doc["storage"],

            path=doc.get("path"),
            bucket=doc.get("bucket"),
            key=doc.get("key"),

            content_type=doc.get("content_type"),
            size_bytes=doc.get("size_bytes"),

            width=doc.get("width"),
            height=doc.get("height"),
            format=doc.get("format"),
            mode=doc.get("mode"),
        )