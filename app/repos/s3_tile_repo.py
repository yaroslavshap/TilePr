# app/repos/s3_tile_repo.py


from __future__ import annotations
from typing import Tuple, BinaryIO, Optional
from io import BytesIO
from minio import Minio
from app.domain.tiles import TileFormat
from minio.deleteobjects import DeleteObject


class S3TileRepository:
    def __init__(self, client: Minio, bucket: str):
        self.client = client
        self.bucket = bucket

    def ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def _tile_key(self, uuid: str, z: int, y: int, x: int, fmt: TileFormat) -> str:
        return f"tiles/{uuid}/{z}/{y}/{x}.{fmt}"

    def _manifest_key(self, uuid: str) -> str:
        return f"tiles/{uuid}/manifest.json"

    def put_tile(self, uuid: str, z: int, y: int, x: int, data: bytes, *, fmt: TileFormat) -> str:
        key = self._tile_key(uuid, z, y, x, fmt)
        self.client.put_object(
            self.bucket, key,
            data=BytesIO(data),
            length=len(data),
            content_type="image/webp" if fmt == "webp" else "image/png",
        )
        return f"minio://{self.bucket}/{key}"

    def open_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: TileFormat) -> Tuple[str, BinaryIO]:
        key = self._tile_key(uuid, z, y, x, fmt)
        resp = self.client.get_object(self.bucket, key)
        return f"minio://{self.bucket}/{key}", resp

    def put_manifest(self, uuid: str, manifest_json: bytes) -> str:
        key = self._manifest_key(uuid)
        self.client.put_object(
            self.bucket, key,
            data=BytesIO(manifest_json),
            length=len(manifest_json),
            content_type="application/json",
        )
        return f"minio://{self.bucket}/{key}"

    def get_manifest(self, uuid: str) -> Optional[bytes]:
        key = self._manifest_key(uuid)
        try:
            resp = self.client.get_object(self.bucket, key)
        except Exception:
            return None
        try:
            return resp.read()
        finally:
            try:
                resp.close()
                resp.release_conn()
            except Exception:
                pass

    def delete_prefix(self, uuid: str) -> None:
        prefix = f"tiles/{uuid}/"
        # minio требует delete_objects; соберём список
        objs = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        to_delete = [o.object_name for o in objs]
        if to_delete:
            for err in self.client.remove_objects(self.bucket, to_delete):
                _ = err


    def delete_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: str) -> None:
        key = self._tile_key(uuid, z, y, x, fmt)
        self.client.remove_object(self.bucket, key)

    def delete_all_tiles(self, uuid: str) -> dict:
        prefix = f"tiles/{uuid}/"
        objs = list(self.client.list_objects(self.bucket, prefix=prefix, recursive=True))
        if not objs:
            return {"deleted": 0, "failed": 0}

        delete_list = [DeleteObject(o.object_name) for o in objs]

        failed = 0
        for err in self.client.remove_objects(self.bucket, delete_list):
            failed += 1

        return {"deleted": len(objs) - failed, "failed": failed}


    def delete_all_tiles_global(self) -> dict:
        """
        Удаляет ВСЕ тайлы ВСЕХ изображений (prefix tiles/).
        """
        prefix = "tiles/"
        objs = list(self.client.list_objects(self.bucket, prefix=prefix, recursive=True))
        if not objs:
            return {"deleted": 0, "failed": 0}

        delete_list = [DeleteObject(o.object_name) for o in objs]

        failed = 0
        for err in self.client.remove_objects(self.bucket, delete_list):
            failed += 1

        return {"deleted": len(objs) - failed, "failed": failed}


