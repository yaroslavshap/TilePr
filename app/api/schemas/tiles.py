
from typing import Dict, Literal
from pydantic import BaseModel, Field, conint

TileFormat = Literal["webp", "png"]

class LevelInfoDTO(BaseModel):
    z: int
    width: int
    height: int
    tiles_x: int
    tiles_y: int

class TileManifestDTO(BaseModel):
    uuid: str
    tile_size: int
    format: TileFormat
    lossless: bool
    levels: Dict[int, LevelInfoDTO]  # key=z

class CacheStatsDTO(BaseModel):
    ttl_seconds: int
    items: int
    bytes: int
    max_items: int
    max_bytes: int

class CacheStatsResponse(BaseModel):
    status: Literal["ok"] = "ok"
    backend: str
    storage: Dict[str, str]
    cache: CacheStatsDTO

class BuildTilesRequest(BaseModel):
    tile_size: conint(ge=1) = Field(256, description="256 or 512")  # валидацию на 256/512 сделаем в сервисе/роуте
    fmt: TileFormat = Field("webp", description="webp or png")
    lossless: bool = Field(False)

class BuildTilesResponse(TileManifestDTO):
    pass

class DeleteOneTileResponse(BaseModel):
    status: Literal["ok"] = "ok"
    uuid: str
    deleted: Literal["tile"] = "tile"
    z: int
    y: int
    x: int
    fmt: TileFormat

class BulkDeleteStats(BaseModel):
    deleted: int
    failed: int

class DeleteAllTilesResponse(BaseModel):
    status: Literal["ok"] = "ok"
    uuid: str
    deleted: Literal["all_tiles"] = "all_tiles"
    stats: BulkDeleteStats

class DeleteAllTilesGlobalResponse(BaseModel):
    status: Literal["ok"] = "ok"
    deleted: Literal["all_tiles_global"] = "all_tiles_global"
    stats: BulkDeleteStats

class ClearCacheResponse(BaseModel):
    status: Literal["ok"] = "ok"
    message: str