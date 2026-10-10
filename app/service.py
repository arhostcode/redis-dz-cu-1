import json
import time
from datetime import UTC, datetime

from fastapi import HTTPException
from redis.exceptions import WatchError

from app.config import CACHE_TTL, LEADERBOARD, LOGIN_TTL, STREAM
from app.models import BatchInput, Player, ProfileInput
from app.redis_client import RedisClients

LOGIN_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return {count, redis.call('TTL', KEYS[1])}
"""


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


def notification(player_id: str, event_type: str, message: str) -> dict[str, str]:
    return {
        "player_id": player_id,
        "type": event_type,
        "message": message,
        "timestamp": timestamp(),
    }


def player_from_hash(player_id: str, data: dict[str, str]) -> Player:
    return Player(id=player_id, **{**data, "level": int(data["level"])})


class GameHub:
    def __init__(self, clients: RedisClients):
        self.clients = clients
        self.redis = clients.master
        self.login_script = self.redis.register_script(LOGIN_SCRIPT)

    def require_player(self, player_id: str) -> None:
        if not self.redis.exists(f"player:{player_id}"):
            raise HTTPException(404, "Player not found")

    def save_player(self, player_id: str, profile: ProfileInput) -> Player:
        key = f"player:{player_id}"
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.hsetnx(key, "created_at", timestamp())
            pipe.hset(key, mapping=profile.model_dump())
            pipe.delete(f"cache:player:{player_id}")
            pipe.xadd(STREAM, notification(player_id, "profile_updated", "Profile saved"))
            pipe.hgetall(key)
            result = pipe.execute()
        return player_from_hash(player_id, result[-1])

    def get_player(self, player_id: str) -> tuple[Player, str]:
        key, cache_key = f"player:{player_id}", f"cache:player:{player_id}"
        for _ in range(10):
            try:
                with self.redis.pipeline() as pipe:
                    # Do not cache a profile changed after HGETALL.
                    pipe.watch(key, cache_key)
                    cached = pipe.get(cache_key)
                    if cached:
                        return Player.model_validate(json.loads(cached)), "HIT"
                    data = pipe.hgetall(key)
                    if not data:
                        raise HTTPException(404, "Player not found")
                    player = player_from_hash(player_id, data)
                    pipe.multi()
                    pipe.set(cache_key, player.model_dump_json(), ex=CACHE_TTL)
                    pipe.execute()
                    return player, "MISS"
            except WatchError:
                continue
        raise HTTPException(409, "Profile is being updated; retry the read")

    def change_level(self, player_id: str, delta: int) -> Player:
        key = f"player:{player_id}"
        for _ in range(20):
            try:
                with self.redis.pipeline() as pipe:
                    pipe.watch(key)
                    old_level = pipe.hget(key, "level")
                    if old_level is None:
                        raise HTTPException(404, "Player not found")
                    if not 1 <= int(old_level) + delta <= 1_000_000:
                        raise HTTPException(422, "Resulting level must be between 1 and 1000000")
                    pipe.multi()
                    pipe.hincrby(key, "level", delta)
                    pipe.delete(f"cache:player:{player_id}")
                    pipe.xadd(
                        STREAM,
                        notification(player_id, "level_changed", f"Level changed by {delta}"),
                    )
                    pipe.hgetall(key)
                    return player_from_hash(player_id, pipe.execute()[-1])
            except WatchError:
                continue
        raise HTTPException(409, "Profile is being updated; retry the request")

    def login(self, player_id: str) -> dict:
        self.require_player(player_id)
        count, ttl = self.login_script(keys=[f"logins:{player_id}"], args=[LOGIN_TTL])
        return {"player_id": player_id, "count": count, "ttl": ttl}

    def batch(self, batch: BatchInput) -> dict:
        started = time.perf_counter()
        created_at = timestamp()
        with self.redis.pipeline(transaction=True) as pipe:
            for profile in batch.players:
                key = f"player:{profile.id}"
                pipe.hsetnx(key, "created_at", created_at)
                pipe.hset(key, mapping=profile.model_dump(exclude={"id"}))
                pipe.delete(f"cache:player:{profile.id}")
                pipe.xadd(
                    STREAM, notification(profile.id, "profile_updated", "Batch profile saved")
                )
            pipe.execute()
        return {
            "count": len(batch.players),
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        }

    def add_score(self, player_id: str, score: float) -> dict:
        self.require_player(player_id)
        total = self.redis.zincrby(LEADERBOARD, score, player_id)
        return {"player_id": player_id, "score": total}

    def top(self, limit: int) -> list[dict]:
        entries = self.clients.read_replica(
            lambda r: r.zrevrange(LEADERBOARD, 0, limit - 1, withscores=True)
        )
        return [
            {"player_id": player_id, "score": score, "rank": rank}
            for rank, (player_id, score) in enumerate(entries, 1)
        ]

    def rank(self, player_id: str) -> dict:
        def read(r):
            with r.pipeline(transaction=True) as pipe:
                pipe.zrevrank(LEADERBOARD, player_id)
                pipe.zrank(LEADERBOARD, player_id)
                pipe.zscore(LEADERBOARD, player_id)
                return pipe.execute()

        rank, redis_rank, score = self.clients.read_replica(read)
        if rank is None:
            raise HTTPException(404, "Player is not on the leaderboard")
        return {"player_id": player_id, "rank": rank + 1, "redis_rank": redis_rank, "score": score}

    def add_achievement(self, player_id: str, name: str) -> dict:
        self.require_player(player_id)
        added = self.redis.sadd(f"achievements:{player_id}", name)
        return {"player_id": player_id, "name": name, "added": bool(added)}

    def has_achievement(self, player_id: str, name: str) -> dict:
        self.require_player(player_id)
        found = self.clients.read_replica(lambda r: r.sismember(f"achievements:{player_id}", name))
        return {"player_id": player_id, "name": name, "has_achievement": bool(found)}

    def common_achievements(self, player_id: str, other_id: str) -> dict:
        self.require_player(player_id)
        self.require_player(other_id)
        names = self.clients.read_replica(
            lambda r: r.sinter(f"achievements:{player_id}", f"achievements:{other_id}")
        )
        return {"player_ids": [player_id, other_id], "achievements": sorted(names)}
