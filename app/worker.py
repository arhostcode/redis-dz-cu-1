import logging
import signal
import threading
import time

from redis.exceptions import RedisError, ResponseError

from app.config import GROUP, STREAM, STREAM_RETENTION_SECONDS
from app.redis_client import RedisClients, ensure_group

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def trim_notifications(redis) -> int:
    seconds, microseconds = redis.time()
    cutoff_ms = (seconds - STREAM_RETENTION_SECONDS) * 1000 + microseconds // 1000
    return redis.xtrim(STREAM, minid=f"{cutoff_ms}-0", approximate=False)


def main() -> None:
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    clients = RedisClients()
    next_trim = 0.0

    def process(messages):
        for message_id, fields in messages:
            logger.info("notification id=%s fields=%s", message_id, fields)
            clients.master.xack(STREAM, GROUP, message_id)
            logger.info("ack id=%s", message_id)

    try:
        clients.wait_ready()
        while not stopped.is_set():
            try:
                if time.monotonic() >= next_trim:
                    removed = trim_notifications(clients.master)
                    logger.info("retention removed=%s", removed)
                    next_trim = time.monotonic() + 60
                batches = clients.master.xreadgroup(
                    GROUP, "worker-1", {STREAM: ">"}, count=100, block=2000
                )
                for _, messages in batches:
                    process(messages)
            except ResponseError as exc:
                logger.warning("Stream operation failed: %s", exc)
                if "NOGROUP" in str(exc):
                    try:
                        ensure_group(clients.master)
                    except RedisError:
                        logger.warning("Waiting for a writable master to restore the group")
                stopped.wait(1)
            except RedisError as exc:
                logger.warning("Waiting for Redis: %s", exc)
                stopped.wait(1)
    finally:
        clients.close()


if __name__ == "__main__":
    main()
