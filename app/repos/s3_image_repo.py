# app/repos/s3_image_repo.py


from typing import Optional, BinaryIO, Tuple
from minio import Minio

from app.domain.images_domain import ImageId, ImageLocation
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
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def _key(self, image_id: ImageId, original_name: Optional[str]) -> str:
        ext = _safe_ext(original_name)
        return f"images/{image_id.value}/original.{ext}"

    def upload(self, image_id: ImageId, src: BinaryIO, *, original_name: Optional[str], content_type: Optional[str],
               part_size: int = 10 * 1024 * 1024) -> ImageLocation:
        key = self._key(image_id, original_name)
        cr = CountingReader(src)

        self.client.put_object(
            self.bucket,
            key,
            data=cr,
            length=-1,
            part_size=part_size,
            content_type=content_type or "application/octet-stream",
        )

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
            raise FileNotFoundError("S3 location missing bucket/key")
        resp = self.client.get_object(loc.bucket, loc.key)
        return loc, resp

    def delete_by_location(self, loc: ImageLocation, image_id: ImageId) -> None:
        if loc.bucket and loc.key:
            self.client.remove_object(loc.bucket, loc.key)



