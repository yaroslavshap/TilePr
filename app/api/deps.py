# app/api/deps.py

from functools import lru_cache

from fastapi import HTTPException
from pymongo import MongoClient
from minio import Minio

from app.contracts.image_repository import ImageRepository
from app.contracts.metadata_repository import MetadataRepository

from app.repos.fs_image_repo import FileSystemImageRepository
from app.repos.mem_image_repo import InMemoryImageRepository
from app.repos.s3_image_repo import S3ImageRepository
from app.repos.mongo_metadata_repo import MongoDBMetadataRepository

from app.repos.s3_tile_repo import S3TileRepository
from app.repos.fs_tile_repo import FileSystemTileRepository

from app.repos.mongo_jobs_repo import MongoJobsRepository

from app.services.ingest_service import IngestService
from app.services.original_image_service import OriginalImageService
from app.services.tile_build_queue import TileBuildQueue
from app.services.tiles_service import TilesService

from app.utils.ttl_cache import InMemoryTTLCache

from config import settings


@lru_cache(maxsize=1)
def get_mongo_client() -> MongoClient:
    return MongoClient(settings.MONGO_URL)


@lru_cache(maxsize=1)
def get_minio_client() -> Minio:
    return Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )


@lru_cache(maxsize=1)
def get_metadata_repo() -> MetadataRepository:
    mongo = get_mongo_client()
    col = mongo[settings.MONGO_DB][settings.MONGO_COLLECTION]
    return MongoDBMetadataRepository(col)


@lru_cache(maxsize=1)
def get_fs_repo() -> ImageRepository:
    return FileSystemImageRepository(settings.DATA_DIR)


@lru_cache(maxsize=1)
def get_mem_repo() -> ImageRepository:
    return InMemoryImageRepository(max_bytes=settings.MEM_MAX_BYTES)


@lru_cache(maxsize=1)
def get_s3_repo() -> ImageRepository:
    client = get_minio_client()
    repo = S3ImageRepository(client, settings.MINIO_BUCKET)
    repo.ensure_bucket()
    return repo



# ======== NEW ========
def resolve_image_repo(storage: str) -> ImageRepository:
    if storage == "fs":
        return get_fs_repo()
    if storage == "mem":
        return get_mem_repo()
    if storage == "s3":
        return get_s3_repo()
    raise ValueError("storage must be one of: fs, mem, s3")

def get_image_repo(storage: str) -> ImageRepository:
    try:
        return resolve_image_repo(storage)
    except ValueError:
        raise HTTPException(status_code=400, detail="storage must be one of: fs, mem, s3")



def get_ingest_service(storage: str) -> IngestService:
    try:
        return IngestService(image_repo=resolve_image_repo(storage), meta_repo=get_metadata_repo())
    except ValueError:
        raise HTTPException(status_code=400, detail="storage must be one of: fs, mem, s3")
# ======== NEW ========



def get_original_service() -> OriginalImageService:
    return OriginalImageService(meta_repo=get_metadata_repo(), repo_resolver=resolve_image_repo)


# ---------------- Tiles repo ----------------

@lru_cache(maxsize=1)
def get_tile_repo():
    if settings.TILES_BACKEND == "fs":
        return FileSystemTileRepository(settings.TILES_FS_DIR)

    bucket = settings.TILES_BUCKET or settings.MINIO_BUCKET
    repo = S3TileRepository(get_minio_client(), bucket)
    repo.ensure_bucket()
    return repo


# ---------------- Tiles cache ----------------

@lru_cache(maxsize=1)
def get_tiles_cache() -> InMemoryTTLCache[object, bytes]:
    return InMemoryTTLCache(
        ttl_seconds=settings.TILES_CACHE_TTL,
        max_items=settings.TILES_CACHE_MAX_ITEMS,
        max_bytes=settings.TILES_CACHE_MAX_BYTES,
    )


# ---------------- Jobs + Queue ----------------

@lru_cache(maxsize=1)
def get_jobs_repo() -> MongoJobsRepository:
    mongo = get_mongo_client()
    col = mongo[settings.MONGO_DB][settings.MONGO_JOBS_COLLECTION]
    return MongoJobsRepository(col, ttl_seconds=settings.MONGO_JOBS_TTL_SECONDS)


@lru_cache(maxsize=1)
def get_tile_build_queue() -> TileBuildQueue:
    return TileBuildQueue(settings.RABBIT_URL)


# ---------------- Tiles Service ----------------

@lru_cache(maxsize=1)
def get_tiles_service() -> TilesService:
    return TilesService(repo=get_tile_repo(), cache=get_tiles_cache())


# Сервис не выбирает реализацию.
# Сервис не знает реализацию.
# Сервис не импортирует реализацию.
# Сервис получает уже готовый объект.

# Это Dependency Injection.
