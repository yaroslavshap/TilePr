# app/contracts/tiles_repository.py

from __future__ import annotations
from typing import Protocol, BinaryIO, Tuple, Optional
from app.domain.tiles import TileFormat

class TileRepository(Protocol):
    def put_tile(self, uuid: str, z: int, y: int, x: int, data: bytes, *, fmt: TileFormat) -> str:
        """Сохраняет тайл. Возвращает uri."""
        ...

    def open_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: TileFormat) -> Tuple[str, BinaryIO]:
        """Открывает тайл на чтение. Возвращает (uri, stream)."""
        ...

    def delete_prefix(self, uuid: str) -> None:
        """Удалить все тайлы/манифест по uuid (опционально)."""
        ...

class ManifestRepository(Protocol):
    def put_manifest(self, uuid: str, manifest_json: bytes) -> str: ...
    def get_manifest(self, uuid: str) -> Optional[bytes]: ...


