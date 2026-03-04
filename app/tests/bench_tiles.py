# benchmarks/bench_tiles_simple.py
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Tuple

import httpx

from app.api.deps import get_tiles_service, get_tiles_cache
from config import settings


# ==========================
# НАСТРОЙКИ (ПРОСТО ПРАВЬ ТУТ)
# ==========================
UUID = "ac3259cf-a2fe-4f2f-b858-3e6eeec5fba8"
REPEATS = 2               # сколько раз прогоняем весь набор тайлов
SAMPLE_LIMIT = 0        # 0 = все тайлы
RUN_HTTP = True           # True/False
BASE_URL = "http://0.0.0.0:28000"
# ==========================


def now_ns() -> int:
    return time.perf_counter_ns()


def ns_to_ms(ns: int) -> float:
    return ns / 1_000_000.0


def summarize(samples_ms: List[float]) -> Dict[str, float]:
    samples_ms = sorted(samples_ms)
    if not samples_ms:
        return {"n": 0, "min_ms": 0, "p50_ms": 0, "p95_ms": 0, "max_ms": 0, "mean_ms": 0}

    def pct(p: float) -> float:
        k = max(0, min(len(samples_ms) - 1, int(round(p * (len(samples_ms) - 1)))))
        return samples_ms[k]

    return {
        "n": len(samples_ms),
        "min_ms": samples_ms[0],
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "max_ms": samples_ms[-1],
        "mean_ms": sum(samples_ms) / len(samples_ms),
    }


def list_all_tiles(manifest: dict) -> List[Tuple[int, int, int]]:
    tiles: List[Tuple[int, int, int]] = []
    levels = manifest["levels"]  # keys are strings
    for z_str, li in levels.items():
        z = int(z_str)
        tx = int(li["tiles_x"])
        ty = int(li["tiles_y"])
        for y in range(ty):
            for x in range(tx):
                tiles.append((z, y, x))
    return tiles

def count_total_tiles(manifest: dict) -> int:
    total = 0
    levels = manifest["levels"]
    for li in levels.values():
        total += int(li["tiles_x"]) * int(li["tiles_y"])
    return total


# --------------------------
# pretty printing
# --------------------------
def fmt_ms(v: float | None) -> str:
    if v is None:
        return "-"
    # чуть аккуратнее: <1ms показываем с 3 знаками, иначе 2
    return f"{v:.3f}" if v < 1 else f"{v:.2f}"


def fmt_num(v: int | float | None) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


def make_table(title: str, headers: List[str], rows: List[List[str]]) -> str:
    all_rows = [headers] + rows
    widths = [max(len(str(r[i])) for r in all_rows) for i in range(len(headers))]

    def line(sep: str = "-", cross: str = "+") -> str:
        parts = [sep * (w + 2) for w in widths]
        return cross + cross.join(parts) + cross

    def render_row(r: List[str]) -> str:
        cells = []
        for i, cell in enumerate(r):
            s = str(cell)
            cells.append(" " + s.ljust(widths[i]) + " ")
        return "|" + "|".join(cells) + "|"

    out = []
    out.append(title)
    out.append(line("="))
    out.append(render_row(headers))
    out.append(line("-"))
    for r in rows:
        out.append(render_row(r))
    out.append(line("="))
    return "\n".join(out)


def print_summary_block(service_res: Dict[str, Any], http_res: Dict[str, Any] | None) -> None:
    print("\n=== SUMMARY ===")
    print(f"uuid: {service_res.get('uuid')}")
    print(f"backend: {service_res.get('backend')}")

    total_manifest = service_res.get("tiles_total_manifest")
    total_bench = service_res.get("tiles_total_bench")
    repeats = service_res.get("repeats")

    coverage = (total_bench / total_manifest * 100) if total_manifest else 0

    print(f"tiles in image (manifest): {total_manifest}")
    print(f"tiles used in bench: {total_bench}  (coverage: {coverage:.2f}%)")
    print(f"repeats: {repeats}")
    print(f"tiles_total (bench): {service_res.get('tiles_total')}  repeats: {service_res.get('repeats')}")

    sp1 = service_res.get("speedup_p50_storage_to_warm")
    sp2 = service_res.get("speedup_p50_cold_to_warm")
    print(f"speedup p50 storage -> warm: {fmt_num(sp1)}x")
    print(f"speedup p50 cold    -> warm: {fmt_num(sp2)}x")

    warm_cache = service_res.get("cache_stats_after_warm") or {}
    hit_rate = warm_cache.get("hit_rate")
    hits = warm_cache.get("hits")
    misses = warm_cache.get("misses")
    print(f"cache hits/misses (warm): {hits}/{misses}  hit_rate: {fmt_num(hit_rate)}")

    if http_res:
        sp_http = http_res.get("speedup_p50_http")
        print(f"http speedup p50 cold -> warm: {fmt_num(sp_http)}x")
        print(f"http base_url: {http_res.get('base_url')}")


# --------------------------
# bench
# --------------------------
def bench_service_layer(uuid: str, repeats: int, sample_limit: int | None) -> Dict[str, Any]:
    tiles_service = get_tiles_service()
    cache = get_tiles_cache()

    # manifest
    cache.reset_metrics()
    manifest = tiles_service.get_manifest_dict(uuid)
    total_tiles = count_total_tiles(manifest)  # ← ОБЩЕЕ ЧИСЛО
    all_tiles = list_all_tiles(manifest)
    if sample_limit and sample_limit > 0:
        all_tiles = all_tiles[:sample_limit]
    bench_tiles_count = len(all_tiles)

    # A) STORAGE-only: repo.open_tile + read (без кеша)
    storage_samples: List[float] = []
    for _ in range(repeats):
        for (z, y, x) in all_tiles:
            t0 = now_ns()
            _uri, stream = tiles_service.repo.open_tile(uuid, z, y, x, fmt=manifest["format"])
            try:
                _ = stream.read()
            finally:
                try:
                    stream.close()
                    getattr(stream, "release_conn", lambda: None)()
                except Exception:
                    pass
            storage_samples.append(ns_to_ms(now_ns() - t0))

    # B) COLD service: чистим кеш и меряем (почти всё miss)
    cache.clear()
    cache.reset_metrics()
    cold_samples: List[float] = []
    for _ in range(repeats):
        for (z, y, x) in all_tiles:
            t0 = now_ns()
            _ = tiles_service.get_tile_bytes(uuid, z, y, x)
            cold_samples.append(ns_to_ms(now_ns() - t0))
    cold_cache_stats = cache.stats()

    # C) WARM service: второй проход (почти всё hit)
    cache.reset_metrics()
    warm_samples: List[float] = []
    for _ in range(repeats):
        for (z, y, x) in all_tiles:
            t0 = now_ns()
            _ = tiles_service.get_tile_bytes(uuid, z, y, x)
            warm_samples.append(ns_to_ms(now_ns() - t0))
    warm_cache_stats = cache.stats()

    s_storage = summarize(storage_samples)
    s_cold = summarize(cold_samples)
    s_warm = summarize(warm_samples)

    return {
        "uuid": uuid,
        "tiles_total_manifest": total_tiles,     # ← ВСЕГО В ИЗОБРАЖЕНИИ
        "tiles_total_bench": bench_tiles_count,        "repeats": repeats,
        "backend": settings.TILES_BACKEND,
        "storage": s_storage,
        "service_cold": s_cold,
        "service_warm": s_warm,
        "cache_stats_after_cold": cold_cache_stats,
        "cache_stats_after_warm": warm_cache_stats,
        "speedup_p50_storage_to_warm": (s_storage["p50_ms"] / max(1e-9, s_warm["p50_ms"])) if s_warm["n"] else None,
        "speedup_p50_cold_to_warm": (s_cold["p50_ms"] / max(1e-9, s_warm["p50_ms"])) if s_warm["n"] else None,
    }


def bench_http_layer(base_url: str, uuid: str, repeats: int, sample_limit: int | None) -> Dict[str, Any]:
    client = httpx.Client(base_url=base_url, timeout=60.0)

    # manifest
    r = client.get(f"/tiles/{uuid}/manifest")
    r.raise_for_status()
    manifest = r.json()

    levels = {str(k): v for k, v in manifest["levels"].items()}
    all_tiles = list_all_tiles({"levels": levels})
    if sample_limit and sample_limit > 0:
        all_tiles = all_tiles[:sample_limit]

    # cold: очистка кеша
    client.delete("/tiles/cache")

    cold_samples: List[float] = []
    for _ in range(repeats):
        for (z, y, x) in all_tiles:
            t0 = now_ns()
            rr = client.get(f"/tiles/{uuid}/{z}/{y}/{x}")
            rr.raise_for_status()
            _ = rr.content
            cold_samples.append(ns_to_ms(now_ns() - t0))

    warm_samples: List[float] = []
    for _ in range(repeats):
        for (z, y, x) in all_tiles:
            t0 = now_ns()
            rr = client.get(f"/tiles/{uuid}/{z}/{y}/{x}")
            rr.raise_for_status()
            _ = rr.content
            warm_samples.append(ns_to_ms(now_ns() - t0))

    cache_stats = None
    try:
        s = client.get("/tiles/_cache/stats")
        if s.status_code == 200:
            cache_stats = s.json()
    except Exception:
        pass

    client.close()

    s_cold = summarize(cold_samples)
    s_warm = summarize(warm_samples)

    return {
        "base_url": base_url,
        "uuid": uuid,
        "tiles_total": len(all_tiles),
        "repeats": repeats,
        "http_cold": s_cold,
        "http_warm": s_warm,
        "speedup_p50_http": (s_cold["p50_ms"] / max(1e-9, s_warm["p50_ms"])) if s_warm["n"] else None,
        "cache_stats": cache_stats,
    }


def print_tables(service_res: Dict[str, Any], http_res: Dict[str, Any] | None) -> None:
    rows = []
    for name, key in [("STORAGE (repo.read)", "storage"), ("SERVICE COLD", "service_cold"), ("SERVICE WARM", "service_warm")]:
        s = service_res[key]
        rows.append([
            name,
            fmt_num(int(s["n"])),
            fmt_ms(s["min_ms"]),
            fmt_ms(s["p50_ms"]),
            fmt_ms(s["p95_ms"]),
            fmt_ms(s["mean_ms"]),
            fmt_ms(s["max_ms"]),
        ])

    print()
    print(make_table(
        title="SERVICE BENCH (no HTTP, no network) — per-tile timings (ms)",
        headers=["mode", "n", "min", "p50", "p95", "mean", "max"],
        rows=rows
    ))

    # cache stats
    cold_cs = service_res.get("cache_stats_after_cold") or {}
    warm_cs = service_res.get("cache_stats_after_warm") or {}

    rows2 = [
        ["after COLD", fmt_num(cold_cs.get("items")), fmt_num(cold_cs.get("bytes")), fmt_num(cold_cs.get("hits")),
         fmt_num(cold_cs.get("misses")), fmt_num(cold_cs.get("expired")), fmt_num(cold_cs.get("evictions")),
         fmt_num(cold_cs.get("hit_rate"))],
        ["after WARM", fmt_num(warm_cs.get("items")), fmt_num(warm_cs.get("bytes")), fmt_num(warm_cs.get("hits")),
         fmt_num(warm_cs.get("misses")), fmt_num(warm_cs.get("expired")), fmt_num(warm_cs.get("evictions")),
         fmt_num(warm_cs.get("hit_rate"))],
    ]
    print()
    print(make_table(
        title="CACHE METRICS (service-layer cache object)",
        headers=["phase", "items", "bytes", "hits", "misses", "expired", "evictions", "hit_rate"],
        rows=rows2
    ))

    if http_res:
        rows_http = []
        for name, key in [("HTTP COLD", "http_cold"), ("HTTP WARM", "http_warm")]:
            s = http_res[key]
            rows_http.append([
                name,
                fmt_num(int(s["n"])),
                fmt_ms(s["min_ms"]),
                fmt_ms(s["p50_ms"]),
                fmt_ms(s["p95_ms"]),
                fmt_ms(s["mean_ms"]),
                fmt_ms(s["max_ms"]),
            ])
        print()
        print(make_table(
            title="HTTP BENCH (end-to-end) — per-request timings (ms)",
            headers=["mode", "n", "min", "p50", "p95", "mean", "max"],
            rows=rows_http
        ))

        # если API отдало /tiles/_cache/stats — распечатаем кратко hit_rate
        api_cs = (http_res.get("cache_stats") or {}).get("cache") if isinstance(http_res.get("cache_stats"), dict) else None
        if api_cs:
            print()
            print("HTTP cache stats snapshot (/tiles/_cache/stats): "
                  f"hits={api_cs.get('hits')} misses={api_cs.get('misses')} hit_rate={api_cs.get('hit_rate')}")


if __name__ == "__main__":
    if UUID == "PUT_YOUR_UUID_HERE":
        raise SystemExit("Set UUID at the top of the file (UUID = '...')")

    sample_limit = None if (SAMPLE_LIMIT is None or SAMPLE_LIMIT == 0) else int(SAMPLE_LIMIT)

    service_res = bench_service_layer(UUID, int(REPEATS), sample_limit)
    http_res = bench_http_layer(BASE_URL, UUID, int(REPEATS), sample_limit) if RUN_HTTP else None

    print_tables(service_res, http_res)
    print_summary_block(service_res, http_res)

    # если вдруг хочешь ещё и JSON рядом (можно выключить)
    # print("\nRAW JSON:")
    # print(json.dumps({"service": service_res, "http": http_res}, ensure_ascii=False, indent=2))