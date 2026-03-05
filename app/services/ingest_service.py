# app/services/ingest_service.py

import os
import tempfile
from datetime import datetime
from typing import Optional, Literal
from uuid import uuid4

from app.contracts.image_repository import ImageRepository
from app.contracts.metadata_repository import MetadataRepository
from app.domain.images_domain import ImageId, ImageLocation
from app.domain.metadata_domain import ImageMetadata
from app.exceptions.usecase_errors import UseCaseValidationError
from app.utils.image_probe import probe_image


OnConflict = Literal["error", "overwrite", "skip"]


class IngestService:
    def __init__(self, image_repo: ImageRepository, meta_repo: MetadataRepository):
        self.image_repo = image_repo
        self.meta_repo = meta_repo

    def ingest(
        self,
        *,
        uuid: Optional[str],
        on_conflict: OnConflict,
        filename: Optional[str],
        content_type: Optional[str],
        upload_file_stream,
    ) -> ImageMetadata:
        """
        uuid:
          - None => генерируем новый
          - задан => используем его (идемпотентность/конфликт)
        on_conflict:
          - error     -> если uuid уже есть в Mongo: 409
          - overwrite -> удаляем старый объект в storage + перезаписываем мету
          - skip      -> ничего не делаем, возвращаем существующую мету
        """
        existing = self.meta_repo.get(uuid) if uuid else None

        if uuid and existing:
            if on_conflict == "skip":
                return existing
            if on_conflict == "error":
                raise FileExistsError(f"UUID already exists: {uuid}")
            if on_conflict == "overwrite":
                # удаляем старые данные из storage перед перезаписью
                old_loc = ImageLocation(
                    uri=existing.uri,
                    storage=existing.storage,
                    path=existing.path,
                    bucket=existing.bucket,
                    key=existing.key,
                    content_type=existing.content_type,
                    size_bytes=existing.size_bytes,
                )
                self.image_repo.delete_by_location(old_loc, ImageId(existing.uuid))
                # мету можно не удалять заранее — upsert перезапишет
        elif uuid and not existing:
            pass

        image_id = ImageId(uuid or str(uuid4()))
        now = datetime.utcnow()

        # 1) spool upload -> temp file (для probe и size)
        tmp_path = None
        size_bytes = 0
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".upload") as tmp:
                tmp_path = tmp.name
                while True:
                    chunk = upload_file_stream.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp.write(chunk)
                    size_bytes += len(chunk)
                tmp.flush()

            # 2) Pillow probe
            try:
                props = probe_image(tmp_path)
            except Exception as e:
                raise UseCaseValidationError(f"Файл не является корректным изображением: {e}") from e


            # 3) Upload to storage from temp file
            with open(tmp_path, "rb") as src:
                loc = self.image_repo.upload(
                    image_id=image_id,
                    src=src,
                    original_name=filename,
                    content_type=content_type,
                )

            # 4) Build metadata
            meta = ImageMetadata(
                uuid=image_id.value,
                name=filename,
                last_updated=now,
                uri=loc.uri,
                storage=loc.storage,  # type: ignore[arg-type]

                path=loc.path,
                bucket=loc.bucket,
                key=loc.key,

                content_type=content_type,
                size_bytes=size_bytes,

                width=props.get("width"),
                height=props.get("height"),
                format=props.get("format"),
                mode=props.get("mode"),
            )

            # 5) Write Mongo (can fail) -> rollback if failed
            try:
                self.meta_repo.upsert(meta)
            except Exception as e:
                try:
                    self.image_repo.delete_by_location(loc, image_id)
                except Exception:
                    pass
                raise

            return meta

        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass


