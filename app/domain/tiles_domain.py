# app/domain/tiles_domain.py

from dataclasses import dataclass
from typing import Dict, Literal, Optional

TileFormat = Literal["webp", "png"]

@dataclass(frozen=True)
class LevelInfo:
    z: int
    width: int
    height: int
    tiles_x: int
    tiles_y: int

@dataclass(frozen=True)
class TileManifest:
    uuid: str
    tile_size: int
    format: TileFormat
    lossless: bool
    levels: Dict[int, LevelInfo]  # key=z
    # можно расширить: created_at, algo, checksum, etc.


