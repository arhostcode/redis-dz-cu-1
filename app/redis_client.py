import logging
import time
from collections.abc import Callable
from typing import TypeVar

from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError, RedisError, ResponseError, TimeoutError
from redis.retry import Retry
from redis.sentinel import Sentinel

from app.config import GROUP, STREAM, Settings

logger = logging.getLogger(__name__)
T = TypeVar("T")


def ensure_group(client: Redis) -> None:
    try:
        client.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


class RedisClients:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        options = {
            "socket_timeout": self.settings.socket_timeout,
            "socket_connect_timeout": self.settings.connect_timeout,
            "decode_responses": True,
            "health_check_interval": 10,
            # Retrying a write after a lost response can apply INCR twice.
            "retry": Retry(NoBackoff(), 0),
        }
        self.sentinel = Sentinel(
            self.settings.sentinels,
            sentinel_kwargs={
                "socket_timeout": self.settings.connect_timeout,
                "socket_connect_timeout": self.settings.connect_timeout,
                "decode_responses": True,
            },
            **options,
        )
        self.master = self.sentinel.master_for(self.settings.master_name)
        self.replica = self.sentinel.slave_for(self.settings.master_name)

    def wait_ready(self) -> None:
        deadline = time.monotonic() + self.settings.startup_timeout
        while True:
            try:
                self.master.ping()
                ensure_group(self.master)
                return
            except RedisError:
                if time.monotonic() >= deadline:
                    raise
                logger.info("Waiting for Sentinel and a writable master")
                time.sleep(1)

    def read_replica(self, command: Callable[[Redis], T]) -> T:
        for attempt in range(3):
            try:
                return command(self.replica)
            except (ConnectionError, TimeoutError):
                if attempt == 2:
                    raise
                self.replica.connection_pool.disconnect()
                time.sleep(0.1 * (attempt + 1))
        raise AssertionError("unreachable")

    def close(self) -> None:
        for client in (self.master, self.replica, *self.sentinel.sentinels):
            client.close()
            client.connection_pool.disconnect()
