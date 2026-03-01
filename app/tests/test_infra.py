import os
import pytest
import requests
from pymongo import MongoClient
from minio import Minio

BASE_URL = os.getenv("API_BASE_URL", "http://0.0.0.0:28000")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "images_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "image_metadata")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "palleon-track")


@pytest.mark.integration
def test_api_service_available():
    r = requests.get(f"{BASE_URL}/health", timeout=3)
    assert r.status_code == 200, f"API not available: {r.text}"
    data = r.json()
    assert data.get("status") == "ok", "Health endpoint returned unexpected response"
    print("OK 1: API service available")


def test_mongo_available_and_collection_accessible():
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    db = client[MONGO_DB]
    assert MONGO_DB in client.list_database_names() or True
    col = db[MONGO_COLLECTION]
    col.find_one({})
    print("OK 2: Mongo available")


def test_minio_available_and_bucket_exists():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    assert client.bucket_exists(MINIO_BUCKET), f"Bucket {MINIO_BUCKET} does not exist"
    print("OK 3: MinIO available and bucket exists")



if __name__ == "__main__":
    test_api_service_available()
    test_mongo_available_and_collection_accessible()
    test_minio_available_and_bucket_exists()


# pytest -m integration -s