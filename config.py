# config.py

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # ---------------- Uvicorn ----------------
    UVICORN_SERVER_HOST: str = "0.0.0.0"
    UVICORN_SERVER_PORT: int = 28000

    # ---------------- App data ----------------
    DATA_DIR: str = "./data"

    # ---------------- Mongo ----------------
    MONGO_URL: str = "mongodb://localhost:27017"
    MONGO_DB: str = "images_db"
    MONGO_COLLECTION: str = "image_metadata"

    # jobs repo (Mongo)
    MONGO_JOBS_COLLECTION: str = "tile_jobs"
    MONGO_JOBS_TTL_SECONDS: int = 7 * 24 * 60 * 60  # 7 days

    # ---------------- MinIO / S3 ----------------
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_BUCKET: str = "palleon-track"

    # ---------------- In-memory image repo ----------------
    MEM_MAX_BYTES: int = 300 * 1024 * 1024

    # ---------------- Tiles storage backend ----------------
    TILES_BACKEND: str = "s3"  # s3 | fs
    TILES_FS_DIR: str = "./data"
    TILES_BUCKET: str = "palleon-track"

    # ---------------- Tiles cache ----------------
    TILES_CACHE_TTL: int = 1200000  # seconds
    TILES_CACHE_MAX_ITEMS: int = 50_000
    TILES_CACHE_MAX_BYTES: int = 512 * 1024 * 1024

    # ---------------- RabbitMQ ----------------
    RABBIT_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBIT_QUEUE: str = "tile.build"
    RABBIT_RETRY_QUEUE: str = "tile.build.retry"
    RABBIT_DLQ: str = "tile.build.dlq"

    TILE_BUILD_MAX_RETRIES: int = 5
    TILE_BUILD_RETRY_DELAY_MS: int = 10_000


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()


