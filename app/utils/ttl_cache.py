# app/utils/ttl_cache.py
import time
from dataclasses import dataclass
from typing import Dict, Generic, Optional, TypeVar

K = TypeVar("K")
V = TypeVar("V")


@dataclass
class _Entry(Generic[V]):
    value: V
    expires_at: float
    size: int


class InMemoryTTLCache(Generic[K, V]):
    """
    Простой in-memory TTL cache.
    - get() возвращает None, если ключа нет или он протух
    - set() кладёт значение с TTL
    - лимиты: max_items / max_bytes (чтобы не съесть память)
    """

    def __init__(self, *, ttl_seconds: int, max_items: int, max_bytes: int):
        self.ttl_seconds = int(ttl_seconds)
        self.max_items = int(max_items)
        self.max_bytes = int(max_bytes)

        self._data: Dict[K, _Entry[V]] = {}
        self._bytes: int = 0

    def _now(self) -> float:
        return time.time()

    def _delete(self, key: K) -> None:
        e = self._data.pop(key, None)
        if e is not None:
            self._bytes -= e.size
            if self._bytes < 0:
                self._bytes = 0

    def _purge_expired(self) -> None:
        now = self._now()
        expired = [k for k, e in self._data.items() if e.expires_at <= now]
        for k in expired:
            self._delete(k)

    def _evict_if_needed(self) -> None:
        # сначала уберём протухшее
        self._purge_expired()

        if len(self._data) <= self.max_items and self._bytes <= self.max_bytes:
            return

        # мягкая эвакуация: удаляем те, что раньше истекут
        items = sorted(self._data.items(), key=lambda kv: kv[1].expires_at)
        for k, _ in items:
            if len(self._data) <= self.max_items and self._bytes <= self.max_bytes:
                break
            self._delete(k)

    def get(self, key: K) -> Optional[V]:
        e = self._data.get(key)
        if e is None:
            return None
        if e.expires_at <= self._now():
            self._delete(key)
            return None
        return e.value

    def set(self, key: K, value: V, *, size: int) -> None:
        size = int(size)
        if size <= 0:
            return
        if size > self.max_bytes:
            # слишком большой объект — не кешируем
            return

        # replace existing
        if key in self._data:
            self._delete(key)

        self._data[key] = _Entry(
            value=value,
            expires_at=self._now() + self.ttl_seconds,
            size=size,
        )
        self._bytes += size

        self._evict_if_needed()

    def delete(self, key: K) -> None:
        self._delete(key)

    def clear(self) -> None:
        self._data.clear()
        self._bytes = 0

    def stats(self) -> dict:
        self._purge_expired()
        return {
            "ttl_seconds": self.ttl_seconds,
            "items": len(self._data),
            "bytes": self._bytes,
            "max_items": self.max_items,
            "max_bytes": self.max_bytes,
        }