from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

PlayerId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{1,64}$")]
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
Region = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=32)]
AchievementName = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100, pattern=r"^[^/]+$")
]


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProfileInput(RequestModel):
    name: Name
    level: int = Field(default=1, ge=1, le=1_000_000, strict=True)
    region: Region


class Player(ProfileInput):
    id: PlayerId
    created_at: datetime


class BatchPlayer(ProfileInput):
    id: PlayerId


class BatchInput(RequestModel):
    players: list[BatchPlayer] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_ids(self):
        ids = [player.id for player in self.players]
        if len(ids) != len(set(ids)):
            raise ValueError("Player IDs in a batch must be unique")
        return self


class BatchResult(BaseModel):
    count: int
    elapsed_ms: float


class LevelInput(RequestModel):
    delta: int = Field(ge=-1_000_000, le=1_000_000, strict=True)


class LoginResult(BaseModel):
    player_id: PlayerId
    count: int
    ttl: int


class ScoreInput(RequestModel):
    player_id: PlayerId
    score: float = Field(ge=-1e12, le=1e12, allow_inf_nan=False, strict=True)


class ScoreResult(BaseModel):
    player_id: PlayerId
    score: float


class LeaderboardEntry(ScoreResult):
    rank: int


class RankResult(LeaderboardEntry):
    redis_rank: int = Field(description="Zero-based ascending rank returned by ZRANK")


class AchievementInput(RequestModel):
    name: AchievementName


class AchievementResult(BaseModel):
    player_id: PlayerId
    name: str
    added: bool


class AchievementCheck(BaseModel):
    player_id: PlayerId
    name: str
    has_achievement: bool


class CommonAchievements(BaseModel):
    player_ids: list[PlayerId]
    achievements: list[str]


class HealthResult(BaseModel):
    status: str
    master: str
    connected_replicas: int
