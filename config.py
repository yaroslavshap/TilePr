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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()


