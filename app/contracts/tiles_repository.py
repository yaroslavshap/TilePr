# app/contracts/tiles_repository.py

# from __future__ import annotations
# from typing import Protocol, BinaryIO, Tuple, Optional
# from app.domain.tiles import TileFormat
#
# class TileRepository(Protocol):
#     def put_tile(self, uuid: str, z: int, y: int, x: int, data: bytes, *, fmt: TileFormat) -> str:
#         """Сохраняет тайл. Возвращает uri."""
#         ...
#
#     def open_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: TileFormat) -> Tuple[str, BinaryIO]:
#         """Открывает тайл на чтение. Возвращает (uri, stream)."""
#         ...
#
#     def delete_prefix(self, uuid: str) -> None:
#         """Удалить все тайлы/манифест по uuid (опционально)."""
#         ...
#
# class ManifestRepository(Protocol):
#     def put_manifest(self, uuid: str, manifest_json: bytes) -> str: ...
#     def get_manifest(self, uuid: str) -> Optional[bytes]: ...
#
#

from __future__ import annotations
from typing import Protocol, BinaryIO, Tuple, Optional
from app.domain.tiles import TileFormat

class TilesRepository(Protocol):
    # tiles
    def put_tile(self, uuid: str, z: int, y: int, x: int, data: bytes, *, fmt: TileFormat) -> str: ...
    def open_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: TileFormat) -> Tuple[str, BinaryIO]: ...

    # manifest
    def put_manifest(self, uuid: str, manifest_json: bytes) -> str: ...
    def get_manifest(self, uuid: str) -> Optional[bytes]: ...

    # deletes (admin)
    def delete_prefix(self, uuid: str) -> None: ...
    def delete_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: TileFormat) -> None: ...
    def delete_all_tiles(self, uuid: str) -> dict: ...
    def delete_all_tiles_global(self) -> dict: ...


# Сейчас твой API вызывает методы, которых нет в контракте (delete_tile, delete_all_tiles, delete_all_tiles_global, get_manifest и т.д.).
# Если “всё публичное”, то либо:
#
# добавляем это в контракт (нормально), либо
#
# делаем отдельный TileAdminRepository (более строго).
#
# Чтобы не плодить сущности — сделаем один контракт TilesRepository, который покрывает и manifest, и админские удаления.

# Что это даёт
#
# API и services теперь зависят только от contracts, а не от конкретного repo.
#
# Реализации (fs/s3) должны иметь эти методы (у тебя они уже есть, кроме типизации fmt: str → TileFormat).


