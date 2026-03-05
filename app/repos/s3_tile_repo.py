# app/repos/s3_tile_repo.py


from typing import Tuple, BinaryIO, Optional
from io import BytesIO
from minio import Minio, S3Error
from app.domain.tiles_domain import TileFormat
from minio.deleteobjects import DeleteObject

from app.exceptions.repo_errors import StorageIOError, StorageNotFoundError


class S3TileRepository:
    def __init__(self, client: Minio, bucket: str):
        self.client = client
        self.bucket = bucket

    def ensure_bucket(self) -> None:
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except Exception as e:
            raise StorageIOError(f"Не удалось проверить/создать бакет '{self.bucket}': {e}") from e

    def _tile_key(self, uuid: str, z: int, y: int, x: int, fmt: TileFormat) -> str:
        return f"tiles/{uuid}/{z}/{y}/{x}.{fmt}"

    def _manifest_key(self, uuid: str) -> str:
        return f"tiles/{uuid}/manifest.json"

    def put_tile(self, uuid: str, z: int, y: int, x: int, data: bytes, *, fmt: TileFormat) -> str:
        key = self._tile_key(uuid, z, y, x, fmt)
        try:
            self.client.put_object(
                self.bucket, key,
                data=BytesIO(data),
                length=len(data),
                content_type="image/webp" if fmt == "webp" else "image/png",
            )
        except Exception as e:
            raise StorageIOError(f"Не удалось сохранить тайл '{key}' в бакет '{self.bucket}': {e}") from e
        return f"minio://{self.bucket}/{key}"

    # ======== NEW ========
    def open_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: TileFormat) -> Tuple[str, BinaryIO]:
        key = self._tile_key(uuid, z, y, x, fmt)
        try:
            resp = self.client.get_object(self.bucket, key)
            return f"minio://{self.bucket}/{key}", resp
        except S3Error as e:
            if getattr(e, "code", None) in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                raise StorageNotFoundError("Тайл не найден") from e
            raise StorageIOError(f"Ошибка S3 при чтении тайла '{key}': {e}") from e
        except Exception as e:
            raise StorageIOError(f"Не удалось прочитать тайл '{key}': {e}") from e
    # ======== NEW ========

    def put_manifest(self, uuid: str, manifest_json: bytes) -> str:
        key = self._manifest_key(uuid)
        try:
            self.client.put_object(
                self.bucket, key,
                data=BytesIO(manifest_json),
                length=len(manifest_json),
                content_type="application/json",
            )
        except Exception as e:
            raise StorageIOError(f"Не удалось сохранить манифест '{key}': {e}") from e
        return f"minio://{self.bucket}/{key}"

    def get_manifest(self, uuid: str) -> Optional[bytes]:
        key = self._manifest_key(uuid)
        try:
            resp = self.client.get_object(self.bucket, key)
        except S3Error as e:
            if getattr(e, "code", None) in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                return None
            raise StorageIOError(f"Ошибка S3 при чтении манифеста '{key}': {e}") from e
        except Exception as e:
            raise StorageIOError(f"Не удалось получить манифест '{key}': {e}") from e

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
        try:
            objs = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
            to_delete = [o.object_name for o in objs]
            if to_delete:
                for err in self.client.remove_objects(self.bucket, to_delete):
                    _ = err
        except Exception as e:
            raise StorageIOError(f"Не удалось удалить префикс '{prefix}': {e}") from e


    def delete_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: str) -> None:
        key = self._tile_key(uuid, z, y, x, fmt)
        try:
            self.client.remove_object(self.bucket, key)
        except S3Error as e:
            if getattr(e, "code", None) in ("NoSuchKey", "NoSuchObject"):
                raise StorageNotFoundError("Тайл не найден") from e
            raise StorageIOError(f"Ошибка S3 при удалении тайла '{key}': {e}") from e
        except Exception as e:
            raise StorageIOError(f"Не удалось удалить тайл '{key}': {e}") from e


    def delete_all_tiles(self, uuid: str) -> dict:
        prefix = f"tiles/{uuid}/"
        try:
            objs = list(self.client.list_objects(self.bucket, prefix=prefix, recursive=True))
        except Exception as e:
            raise StorageIOError(f"Не удалось получить список объектов '{prefix}': {e}") from e
        if not objs:
            return {"deleted": 0, "failed": 0}

        delete_list = [DeleteObject(o.object_name) for o in objs]

        failed = 0
        try:
            for err in self.client.remove_objects(self.bucket, delete_list):
                failed += 1
        except Exception as e:
            raise StorageIOError(f"Не удалось массово удалить '{prefix}': {e}") from e

        return {"deleted": len(objs) - failed, "failed": failed}


    def delete_all_tiles_global(self) -> dict:
        """
        Удаляет ВСЕ тайлы ВСЕХ изображений (prefix tiles/).
        """
        prefix = "tiles/"
        try:
            objs = list(self.client.list_objects(self.bucket, prefix=prefix, recursive=True))
        except Exception as e:
            raise StorageIOError(f"Не удалось получить список объектов '{prefix}': {e}") from e

        if not objs:
            return {"deleted": 0, "failed": 0}

        delete_list = [DeleteObject(o.object_name) for o in objs]

        failed = 0
        try:
            for err in self.client.remove_objects(self.bucket, delete_list):
                failed += 1
        except Exception as e:
            raise StorageIOError(f"Не удалось удалить все тайлы '{prefix}': {e}") from e

        return {"deleted": len(objs) - failed, "failed": failed}


