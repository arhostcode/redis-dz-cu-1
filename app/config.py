import os
from dataclasses import dataclass, field

CACHE_TTL = 60
LOGIN_TTL = 86400
STREAM_RETENTION_SECONDS = 7 * 24 * 60 * 60
STREAM = "notifications"
GROUP = "notifications-group"
LEADERBOARD = "tournament:main"


@dataclass(frozen=True)
class Settings:
    sentinels: list[tuple[str, int]] = field(
        default_factory=lambda: [
            (host, int(port))
            for entry in os.getenv(
                "REDIS_SENTINELS", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
            ).split(",")
            for host, port in [entry.strip().rsplit(":", 1)]
        ]
    )
    master_name: str = field(default_factory=lambda: os.getenv("REDIS_MASTER_NAME", "mymaster"))
    socket_timeout: float = field(
        default_factory=lambda: float(os.getenv("REDIS_SOCKET_TIMEOUT", "3"))
    )
    connect_timeout: float = field(
        default_factory=lambda: float(os.getenv("REDIS_CONNECT_TIMEOUT", "1"))
    )
    startup_timeout: float = field(
        default_factory=lambda: float(os.getenv("REDIS_STARTUP_TIMEOUT", "90"))
    )
