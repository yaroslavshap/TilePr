from __future__ import annotations
from pathlib import Path
from typing import Tuple, BinaryIO, Optional
from app.domain.tiles import TileFormat

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
        f = open(p, "rb")
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
