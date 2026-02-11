from __future__ import annotations
import os
from functools import lru_cache

from fastapi import Depends
from pymongo import MongoClient
from minio import Minio

from app.contracts.image_repository import ImageRepository
from app.contracts.metadata_repository import MetadataRepository

from app.repos.fs_image_repo import FileSystemImageRepository
from app.repos.mem_image_repo import InMemoryImageRepository
from app.repos.s3_image_repo import S3ImageRepository
from app.repos.mongo_metadata_repo import MongoDBMetadataRepository

from app.services.ingest_service import IngestService
from app.services.original_image_service import OriginalImageService

DATA_DIR = os.getenv("DATA_DIR", "./data")

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "images_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "image_metadata")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "palleon-track")

MEM_MAX_BYTES = int(os.getenv("MEM_MAX_BYTES", str(300 * 1024 * 1024)))


@lru_cache(maxsize=1)
def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URL)

@lru_cache(maxsize=1)
def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

@lru_cache(maxsize=1)
def get_metadata_repo() -> MetadataRepository:
    mongo = get_mongo_client()
    col = mongo[MONGO_DB][MONGO_COLLECTION]
    return MongoDBMetadataRepository(col)

@lru_cache(maxsize=1)
def get_fs_repo() -> ImageRepository:
    return FileSystemImageRepository(DATA_DIR)

@lru_cache(maxsize=1)
def get_mem_repo() -> ImageRepository:
    return InMemoryImageRepository(max_bytes=MEM_MAX_BYTES)

@lru_cache(maxsize=1)
def get_s3_repo() -> ImageRepository:
    client = get_minio_client()
    repo = S3ImageRepository(client, MINIO_BUCKET)
    repo.ensure_bucket()
    return repo

def resolve_image_repo(storage: str) -> ImageRepository:
    if storage == "fs":
        return get_fs_repo()
    if storage == "mem":
        return get_mem_repo()
    if storage == "s3":
        return get_s3_repo()
    raise ValueError("storage must be one of: fs, mem, s3")

def get_image_repo(storage: str) -> ImageRepository:
    return resolve_image_repo(storage)

def get_ingest_service(storage: str) -> IngestService:
    return IngestService(image_repo=resolve_image_repo(storage), meta_repo=get_metadata_repo())

def get_original_service() -> OriginalImageService:
    return OriginalImageService(meta_repo=get_metadata_repo(), repo_resolver=resolve_image_repo)


from functools import lru_cache
from app.repos.s3_tile_repo import S3TileRepository
from app.repos.fs_tile_repo import FileSystemTileRepository
from app.services.tile_pyramid_builder import TilePyramidBuilder

TILES_BACKEND = os.getenv("TILES_BACKEND", "s3")  # s3 | fs
TILES_FS_DIR = os.getenv("TILES_FS_DIR", "./data")
TILES_BUCKET = os.getenv("TILES_BUCKET", os.getenv("MINIO_BUCKET", "palleon-track"))

@lru_cache(maxsize=1)
def get_tile_repo():
    if TILES_BACKEND == "fs":
        return FileSystemTileRepository(TILES_FS_DIR)
    repo = S3TileRepository(get_minio_client(), TILES_BUCKET)
    repo.ensure_bucket()
    return repo

@lru_cache(maxsize=1)
def get_tile_builder():
    repo = get_tile_repo()
    return TilePyramidBuilder(tile_repo=repo, manifest_repo=repo)
