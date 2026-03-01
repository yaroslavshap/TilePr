from io import BytesIO

import pytest
from PIL import Image
import sys
import requests

BASE = "http://0.0.0.0:28000"


def fake_jpeg_bytes(width: int = 1000, height: int = 1000, color: tuple[int, int, int] = (200, 50, 50), quality: int = 85,) -> bytes:
    img = Image.new("RGB", (width, height), color)
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=quality)
    return buf.getvalue()

@pytest.mark.end_to_end
def test_main():
    # хэлсчек
    r = requests.get(f"{BASE}/health", timeout=3)
    assert r.status_code == 200, r.text
    print("\nOK 1: Хэлсчек")

    # добавить изображение в хранилище и его метаданные в mongo
    files = {"file": ("t.jpg", fake_jpeg_bytes(), "image/jpeg")}
    r = requests.post(f"{BASE}/ingest/images/s3/ingest", files=files, timeout=10)
    assert r.status_code == 200, r.text
    uuid = r.json()["uuid"]
    print("OK 2: Добавить в s3 + mongo по uuid:", uuid)

    try:
        # получить метаданные оригинала по uuid
        r = requests.get(f"{BASE}/metadata/images/{uuid}/meta", timeout=5)
        assert r.status_code == 200, r.text
        print("OK 3: Получить метаданные оригинала по uuid")

        # получить изображение оригинала по uuid
        r = requests.get(f"{BASE}/metadata/images/{uuid}", timeout=10)
        assert r.status_code == 200, r.text
        print("OK 4: Получить изображение оригинала по uuid")


        # ================================= ТАЙЛЫ ================================================================
        # статус кеша
        r = requests.get(f"{BASE}/tiles/_cache/stats", timeout=15)
        assert r.status_code == 200, r.text
        cache_before = r.json()["cache"]
        print("OK 5: Статус кеша")

        # Построение пирамиды тайлов
        params = {"tile_size": 256, "fmt": "webp", "lossless": "false"}
        r = requests.post(f"{BASE}/tiles/{uuid}/build", params=params, timeout=120)
        assert r.status_code == 200, r.text
        build = r.json()
        assert build["uuid"] == uuid
        assert int(build["tile_size"]) == 256
        assert build["format"] in ("webp", "png")
        print("OK 6: Построение пирамиды тайлов")

        # получить один тайл (0,0,0)
        r = requests.get(f"{BASE}/tiles/{uuid}/0/0/0", timeout=30)
        assert r.status_code == 200, r.text
        ct = r.headers.get("content-type", "")
        assert ct in ("image/webp", "image/png"), ct
        first_tile_bytes = r.content
        assert len(first_tile_bytes) > 10
        print("OK 7: Получить один тайл (0,0,0)")

        # получить этот же тайл еще раз -> должен браться из кеша
        r = requests.get(f"{BASE}/tiles/{uuid}/0/0/0", timeout=30)
        assert r.status_code == 200, r.text
        assert r.content == first_tile_bytes
        print("OK 8: Получить этот же тайл еще раз -> должен браться из кеша")

        # статистика кеша
        r = requests.get(f"{BASE}/tiles/_cache/stats", timeout=5)
        assert r.status_code == 200, r.text
        cache_after = r.json()["cache"]
        assert int(cache_after["items"]) >= int(cache_before["items"])
        print("OK 9: Статистика кеша. Проверка что кеш растет")


        # Удалить один тайл
        r = requests.delete(f"{BASE}/tiles/{uuid}/0/0/0", timeout=20)
        assert r.status_code == 200, r.text
        deleted = r.json()
        assert deleted["uuid"] == uuid
        assert int(deleted["z"]) == 0
        assert int(deleted["y"]) == 0
        assert int(deleted["x"]) == 0
        print("OK 10: Удалить один тайл")

        # проверка, что после удаления тайла нет
        r = requests.get(f"{BASE}/tiles/{uuid}/0/0/0", timeout=30)
        assert r.status_code == 404, r.text
        print("OK 11: Проверка, что после удаления тайла нет")

        # Удалить все тайлы по uuid
        r = requests.delete(f"{BASE}/tiles/{uuid}", timeout=60)
        assert r.status_code == 200, r.text
        bulk = r.json()
        assert bulk["uuid"] == uuid
        assert "stats" in bulk
        print("OK 12: Удалить все тайлы по uuid")

        # ============================================================================================================


        # Удалить оригинал из S3(MinIO) по uuid
        r = requests.delete(f"{BASE}/storage/{uuid}", timeout=10)
        assert r.status_code == 200, r.text
        print("OK 13: Удалить оригинал из S3(MinIO) по uuid")

        # удалить метаданные по uuid
        r = requests.delete(f"{BASE}/metadata/{uuid}", timeout=5)
        assert r.status_code == 200, r.text
        print("OK 14: Удалить метаданные по uuid")

        # Убеждаемся, что метаданные удалены
        r = requests.get(f"{BASE}/metadata/images/{uuid}/meta", timeout=5)
        assert r.status_code == 404, r.text
        print("OK 15: Убеждаемся, что метаданные удалены")

    finally:
        try:
            requests.delete(f"{BASE}/tiles/{uuid}", timeout=20)
        except Exception:
            pass
        try:
            requests.delete(f"{BASE}/storage/{uuid}", timeout=20)
        except Exception:
            pass
        try:
            requests.delete(f"{BASE}/metadata/{uuid}", timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    try:
        test_main()
    except Exception as e:
        print(e)


# pytest -m end_to_end -s



