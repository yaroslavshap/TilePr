# config.py

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # mongo
    # STORAGE_TYPE: str
    # MONGO_USERNAME: str
    # MONGO_PASSWORD: str
    # MONGO_HOST: str
    # MONGO_PORT: int
    # MONGODB_DB: str
    # MONGODB_META_COLLECTION: str
    # MONGODB_DATASETS_COLLECTION: str

    # uvicorn
    UVICORN_SERVER_HOST: str
    UVICORN_SERVER_PORT: int

    # in-memory image repo
    MEM_MAX_BYTES: int = 300 * 1024 * 1024

    # tiles backend
    TILES_BACKEND: str = "s3"  # s3 | fs
    TILES_FS_DIR: str = "./data"
    TILES_BUCKET: str = "palleon-track"

    # tiles cache (TTL)
    TILES_CACHE_TTL: int = 120  # seconds
    TILES_CACHE_MAX_ITEMS: int = 50_000
    TILES_CACHE_MAX_BYTES: int = 512 * 1024 * 1024


    RABBIT_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBIT_QUEUE: str = "tile.build"
    RABBIT_RETRY_QUEUE: str = "tile.build.retry"
    RABBIT_DLQ: str = "tile.build.dlq"

    TILE_BUILD_MAX_RETRIES: int = 5
    TILE_BUILD_RETRY_DELAY_MS: int = 10_000

    MONGO_JOBS_COLLECTION: str = "tile_jobs"
    # сколько хранить jobs в Mongo (секунды)
    MONGO_JOBS_TTL_SECONDS: int = 7 * 24 * 60 * 60  # 7 days


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()


