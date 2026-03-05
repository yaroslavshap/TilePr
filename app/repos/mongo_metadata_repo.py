# app/repos/mongo_metadata_repo.py

from typing import Optional, Sequence, Tuple, Iterable
from pymongo.collection import Collection
from app.domain.metadata_domain import ImageMetadata

class MongoDBMetadataRepository:
    def __init__(self, collection: Collection):
        self.col = collection
        self.col.create_index("uuid", unique=True)

    def upsert(self, meta: ImageMetadata) -> None:
        doc = self._to_doc(meta)
        self.col.update_one({"uuid": meta.uuid}, {"$set": doc}, upsert=True)

    def get(self, uuid: str) -> Optional[ImageMetadata]:
        doc = self.col.find_one({"uuid": uuid})
        return self._from_doc(doc) if doc else None

    def delete(self, uuid: str) -> None:
        self.col.delete_one({"uuid": uuid})

    def list(self, *, limit: int, offset: int) -> Tuple[Sequence[ImageMetadata], int]:
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))

        total = self.col.count_documents({})
        docs = list(
            self.col.find({})
            .sort("_id", -1)
            .skip(offset)
            .limit(limit)
        )
        return [self._from_doc(d) for d in docs], total


    def iter_uuids(self, *, batch_size: int = 1000) -> Iterable[str]:
        batch_size = max(1, min(int(batch_size), 5000))
        cur = self.col.find({}, {"uuid": 1}).batch_size(batch_size)
        for doc in cur:
            u = doc.get("uuid")
            if u:
                yield u

    def delete_all(self) -> int:
        res = self.col.delete_many({})
        return int(getattr(res, "deleted_count", 0))

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