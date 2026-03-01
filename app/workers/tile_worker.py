
import json
import os
import tempfile
import pika

from config import settings
from app.api.deps import (
    get_original_service,
    get_jobs_repo,
)
from app.api.tiles_routes import _stream_minio  # можно вынести в util, но так тоже ок
from PIL import Image


def _declare_queues(ch: pika.channel.Channel) -> None:
    # Main queue: durable
    ch.queue_declare(queue=settings.RABBIT_QUEUE, durable=True)

    # Retry queue: сообщения живут TTL, потом возвращаются в main
    ch.queue_declare(
        queue=settings.RABBIT_RETRY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": settings.RABBIT_QUEUE,
        },
    )

    # DLQ
    ch.queue_declare(queue=settings.RABBIT_DLQ, durable=True)


def _publish_retry(ch: pika.channel.Channel, payload: dict, delay_ms: int) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    ch.basic_publish(
        exchange="",
        routing_key=settings.RABBIT_RETRY_QUEUE,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            expiration=str(int(delay_ms)),  # per-message TTL
        ),
    )


def _publish_dlq(ch: pika.channel.Channel, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    ch.basic_publish(
        exchange="",
        routing_key=settings.RABBIT_DLQ,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )


def handle_message(ch, method, properties, body: bytes) -> None:
    jobs = get_jobs_repo()
    svc = get_original_service()
    builder = get_tile_builder()

    payload = json.loads(body.decode("utf-8"))
    job_id = payload["job_id"]
    uuid = payload["uuid"]
    tile_size = int(payload["tile_size"])
    fmt = payload["fmt"]
    lossless = bool(payload.get("lossless", False))
    attempt = int(payload.get("attempt", 0))

    # статус running
    jobs.set_status(job_id, status="running", attempt=attempt, error=None)

    tmp_path = None
    try:
        meta, loc, stream = svc.open_original(uuid)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".img") as tmp:
            tmp_path = tmp.name
            if meta.storage == "s3":
                for chunk in _stream_minio(stream):
                    tmp.write(chunk)
            else:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    tmp.write(chunk)
            tmp.flush()

        with Image.open(tmp_path) as im:
            im.load()
            builder.build(uuid=uuid, image=im, tile_size=tile_size, fmt=fmt, lossless=lossless)

        jobs.set_status(job_id, status="done", attempt=attempt, error=None)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    except Exception as e:
        # retry / dlq
        attempt_next = attempt + 1
        max_retries = settings.TILE_BUILD_MAX_RETRIES

        err = str(e)
        jobs.set_status(job_id, status="failed", attempt=attempt_next, error=err)

        payload["attempt"] = attempt_next

        if attempt_next <= max_retries:
            _publish_retry(ch, payload, settings.TILE_BUILD_RETRY_DELAY_MS)
        else:
            _publish_dlq(ch, payload)

        # ack исходное сообщение (мы уже перепоставили его сами)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    finally:
        try:
            stream.close()
            getattr(stream, "release_conn", lambda: None)()
        except Exception:
            pass

        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def main() -> None:
    params = pika.URLParameters(settings.RABBIT_URL)
    connection = pika.BlockingConnection(params)
    ch = connection.channel()

    _declare_queues(ch)

    ch.basic_qos(prefetch_count=1)  # один job на воркер за раз
    ch.basic_consume(queue=settings.RABBIT_QUEUE, on_message_callback=handle_message)

    print("[tile-worker] started, consuming:", settings.RABBIT_QUEUE)
    ch.start_consuming()


if __name__ == "__main__":
    main()



