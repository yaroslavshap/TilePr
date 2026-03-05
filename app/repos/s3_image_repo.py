# app/repos/s3_image_repo.py


from typing import Optional, BinaryIO, Tuple
from minio import Minio, S3Error

from app.domain.images_domain import ImageId, ImageLocation
from app.exceptions.repo_errors import StorageIOError, StorageLocationError
from app.utils.counting_stream import CountingReader

def _safe_ext(filename: Optional[str]) -> str:
    if filename and "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext in {"png", "jpg", "jpeg", "webp"}:
            return ext
    return "bin"

class S3ImageRepository:
    def __init__(self, client: Minio, bucket: str):
        self.client = client
        self.bucket = bucket

    def storage_kind(self) -> str:
        return "s3"

    def ensure_bucket(self) -> None:
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except Exception as e:
            raise StorageIOError(f"Не удалось проверить/создать бакет '{self.bucket}': {e}") from e

    def _key(self, image_id: ImageId, original_name: Optional[str]) -> str:
        ext = _safe_ext(original_name)
        return f"images/{image_id.value}/original.{ext}"

    def upload(self, image_id: ImageId, src: BinaryIO, *, original_name: Optional[str], content_type: Optional[str],
               part_size: int = 10 * 1024 * 1024) -> ImageLocation:
        key = self._key(image_id, original_name)
        cr = CountingReader(src)

        try:
            self.client.put_object(
                self.bucket,
                key,
                data=cr,
                length=-1,
                part_size=part_size,
                content_type=content_type or "application/octet-stream",
            )
        except Exception as e:
            raise StorageIOError(f"Не удалось загрузить объект '{key}' в бакет '{self.bucket}': {e}") from e

        return ImageLocation(
            uri=f"minio://{self.bucket}/{key}",
            storage="s3",
            bucket=self.bucket,
            key=key,
            content_type=content_type,
            size_bytes=cr.count,
        )

    def open_by_location(self, loc: ImageLocation) -> Tuple[ImageLocation, BinaryIO]:
        if not loc.bucket or not loc.key:
            raise StorageLocationError("Для S3-локации не указаны bucket и/или key")
        try:
            resp = self.client.get_object(loc.bucket, loc.key)
        except S3Error as e:
            raise StorageIOError(f"Ошибка S3 при чтении объекта {loc.bucket}/{loc.key}: {e}") from e
        except Exception as e:
            raise StorageIOError(f"Не удалось прочитать объект {loc.bucket}/{loc.key}: {e}") from e
        return loc, resp

    def delete_by_location(self, loc: ImageLocation, image_id: ImageId) -> None:
        if loc.bucket and loc.key:
            try:
                self.client.remove_object(loc.bucket, loc.key)
            except S3Error as e:
                raise StorageIOError(f"Ошибка S3 при удалении объекта {loc.bucket}/{loc.key}: {e}") from e
            except Exception as e:
                raise StorageIOError(f"Не удалось удалить объект {loc.bucket}/{loc.key}: {e}") from e



