# app/repos/fs_image_repo.py

from __future__ import annotations
from pathlib import Path
from typing import Optional, BinaryIO, Tuple
import shutil
import os

from app.domain.images import ImageId, ImageLocation

def _safe_ext(filename: Optional[str]) -> str:
    if filename and "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext in {"png", "jpg", "jpeg", "webp"}:
            return ext
    return "bin"

class FileSystemImageRepository:
    def __init__(self, root_dir: str):
        self.root = Path(root_dir)

    def storage_kind(self) -> str:
        return "fs"

    def _dir(self, image_id: ImageId) -> Path:
        return self.root / "images" / image_id.value

    def _path(self, image_id: ImageId, original_name: Optional[str]) -> Path:
        return self._dir(image_id) / f"original.{_safe_ext(original_name)}"

    def upload(self, image_id: ImageId, src: BinaryIO, *, original_name: Optional[str], content_type: Optional[str],
               chunk_size: int = 1024 * 1024) -> ImageLocation:
        d = self._dir(image_id)
        d.mkdir(parents=True, exist_ok=True)
        path = self._path(image_id, original_name)

        size = 0
        with open(path, "wb") as out:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                size += len(chunk)
                out.write(chunk)

        return ImageLocation(
            uri=path.resolve().as_uri(),
            storage="fs",
            path=str(path),
            content_type=content_type,
            size_bytes=size,
        )

    def open_by_location(self, loc: ImageLocation) -> Tuple[ImageLocation, BinaryIO]:
        if not loc.path:
            raise FileNotFoundError("FS location missing path")
        f = open(loc.path, "rb")
        return loc, f

    def delete_by_location(self, loc: ImageLocation, image_id: ImageId) -> None:
        if loc.path:
            try:
                os.remove(loc.path)
            except FileNotFoundError:
                pass
        # “красиво” удалим папку uuid целиком
        d = self._dir(image_id)
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)



