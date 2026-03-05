# app/repos/fs_tile_repo.py

from pathlib import Path
from typing import Tuple, BinaryIO, Optional
from app.domain.tiles_domain import TileFormat
from app.exceptions.repo_errors import StorageNotFoundError, StorageIOError


class FileSystemTileRepository:
    def __init__(self, root_dir: str):
        self.root = Path(root_dir)

    def _tile_path(self, uuid: str, z: int, y: int, x: int, fmt: TileFormat) -> Path:
        return self.root / "tiles" / uuid / str(z) / str(y) / f"{x}.{fmt}"

    def _manifest_path(self, uuid: str) -> Path:
        return self.root / "tiles" / uuid / "manifest.json"

    def put_tile(self, uuid: str, z: int, y: int, x: int, data: bytes, *, fmt: TileFormat) -> str:
        p = self._tile_path(uuid, z, y, x, fmt)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return p.resolve().as_uri()

    def open_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: TileFormat) -> Tuple[str, BinaryIO]:
        p = self._tile_path(uuid, z, y, x, fmt)
        try:
            f = open(p, "rb")
        except FileNotFoundError as e:
            raise StorageNotFoundError(f"Тайл не найден: {p}") from e
        except OSError as e:
            raise StorageIOError(f"Не удалось открыть тайл: {p}. Ошибка: {e}") from e
        return p.resolve().as_uri(), f

    def put_manifest(self, uuid: str, manifest_json: bytes) -> str:
        p = self._manifest_path(uuid)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(manifest_json)
        return p.resolve().as_uri()

    def get_manifest(self, uuid: str) -> Optional[bytes]:
        p = self._manifest_path(uuid)
        if not p.exists():
            return None
        return p.read_bytes()

    def delete_prefix(self, uuid: str) -> None:
        import shutil
        d = self.root / "tiles" / uuid
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)


    def delete_tile(self, uuid: str, z: int, y: int, x: int, *, fmt: str) -> None:
        p = self._tile_path(uuid, z, y, x, fmt)
        p.unlink(missing_ok=True)

    def delete_all_tiles(self, uuid: str) -> dict:
        import shutil
        d = self.root / "tiles" / uuid
        if not d.exists():
            return {"deleted": 0, "failed": 0}
        # посчитать файлы (примерно)
        files = list(d.rglob("*"))
        shutil.rmtree(d, ignore_errors=True)
        return {"deleted": len(files), "failed": 0}


    def delete_all_tiles_global(self) -> dict:
        """
        Удаляет папку tiles целиком (все uuid).
        """
        import shutil
        d = self.root / "tiles"
        if not d.exists():
            return {"deleted": 0, "failed": 0}
        files = list(d.rglob("*"))
        shutil.rmtree(d, ignore_errors=True)
        return {"deleted": len(files), "failed": 0}


