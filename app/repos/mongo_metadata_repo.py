from __future__ import annotations
from typing import Optional
from pymongo.collection import Collection
from app.domain.metadata import ImageMetadata

class MongoDBMetadataRepository:
    def __init__(self, collection: Collection):
        self.col = collection
        self.col.create_index("uuid", unique=True)

    def upsert(self, meta: ImageMetadata) -> None:
        doc = {
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
        self.col.update_one({"uuid": meta.uuid}, {"$set": doc}, upsert=True)

    def get(self, uuid: str) -> Optional[ImageMetadata]:
        doc = self.col.find_one({"uuid": uuid})
        if not doc:
            return None
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

    def delete(self, uuid: str) -> None:
        self.col.delete_one({"uuid": uuid})
