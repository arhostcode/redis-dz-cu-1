import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from app.models import (
    AchievementCheck,
    AchievementInput,
    AchievementResult,
    BatchInput,
    BatchResult,
    CommonAchievements,
    HealthResult,
    LeaderboardEntry,
    LevelInput,
    LoginResult,
    Player,
    PlayerId,
    ProfileInput,
    RankResult,
    ScoreInput,
    ScoreResult,
)
from app.redis_client import RedisClients
from app.service import GameHub

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    clients = RedisClients()
    try:
        clients.wait_ready()
        app.state.gamehub = GameHub(clients)
        yield
    finally:
        clients.close()


app = FastAPI(
    title="GameHub",
    version="1.0.0",
    description="Players, leaderboards and achievements backed by Redis Sentinel.",
    lifespan=lifespan,
    responses={
        404: {"description": "Player or leaderboard entry does not exist"},
        409: {"description": "Concurrent profile updates; transaction was not committed"},
        503: {"description": "Redis unavailable; a write may have completed before disconnection"},
    },
)


def service(request: Request) -> GameHub:
    return request.app.state.gamehub


Service = Annotated[GameHub, Depends(service)]


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError):
    logger.warning("Redis unavailable for %s: %s", request.url.path, exc)
    return JSONResponse(status_code=503, content={"detail": "Redis temporarily unavailable"})


@app.get("/health", response_model=HealthResult, tags=["Infrastructure"])
def health(hub: Service):
    info = hub.redis.info("replication")
    host, port = hub.clients.sentinel.discover_master(hub.clients.settings.master_name)
    return {
        "status": "ok",
        "master": f"{host}:{port}",
        "connected_replicas": info.get("connected_slaves", 0),
    }


@app.post("/api/players/batch", response_model=BatchResult, tags=["Players"])
def batch_players(body: BatchInput, hub: Service):
    return hub.batch(body)


@app.post("/api/players/{player_id}", response_model=Player, tags=["Players"])
def save_player(player_id: PlayerId, body: ProfileInput, hub: Service):
    return hub.save_player(player_id, body)


@app.get("/api/players/{player_id}", response_model=Player, tags=["Players"])
def get_player(player_id: PlayerId, response: Response, hub: Service):
    player, cache_status = hub.get_player(player_id)
    response.headers["X-Cache"] = cache_status
    return player


@app.patch("/api/players/{player_id}/level", response_model=Player, tags=["Players"])
def change_level(player_id: PlayerId, body: LevelInput, hub: Service):
    return hub.change_level(player_id, body.delta)


@app.post("/api/players/{player_id}/login", response_model=LoginResult, tags=["Players"])
def login(player_id: PlayerId, hub: Service):
    return hub.login(player_id)


@app.post("/api/leaderboard/score", response_model=ScoreResult, tags=["Leaderboard"])
def add_score(body: ScoreInput, hub: Service):
    return hub.add_score(body.player_id, body.score)


@app.get("/api/leaderboard/top", response_model=list[LeaderboardEntry], tags=["Leaderboard"])
def top(hub: Service, limit: Annotated[int, Query(ge=1, le=100)] = 10):
    return hub.top(limit)


@app.get("/api/leaderboard/rank/{player_id}", response_model=RankResult, tags=["Leaderboard"])
def rank(player_id: PlayerId, hub: Service):
    return hub.rank(player_id)


@app.post(
    "/api/players/{player_id}/achievements", response_model=AchievementResult, tags=["Achievements"]
)
def add_achievement(player_id: PlayerId, body: AchievementInput, hub: Service):
    return hub.add_achievement(player_id, body.name)


@app.get(
    "/api/players/{player_id}/achievements/common/{other_id}",
    response_model=CommonAchievements,
    tags=["Achievements"],
)
def common_achievements(player_id: PlayerId, other_id: PlayerId, hub: Service):
    return hub.common_achievements(player_id, other_id)


@app.get(
    "/api/players/{player_id}/achievements/{name}",
    response_model=AchievementCheck,
    tags=["Achievements"],
)
def has_achievement(player_id: PlayerId, name: str, hub: Service):
    return hub.has_achievement(player_id, name)
