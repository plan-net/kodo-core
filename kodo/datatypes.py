import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, RootModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Option(BaseSettings):

    LOADER: Optional[str] = "kodo.worker.loader:default_loader"
    URL:  Optional[str] = "http://localhost:3366"
    CORS_ORIGINS: Optional[List[str]] = ["*"]
    ORGANIZATION: Optional[str] = None
    CONNECT: Optional[List[str]] = None
    REGISTRY: Optional[bool] = True
    FEED: Optional[bool] = True
    CACHE_DATA: Optional[str] = "./data/cache.json"
    CACHE_RESET: Optional[bool] = False
    SCREEN_LEVEL: Optional[str] = "INFO"
    LOG_FILE: Optional[str] = "./data/kodo.log"
    LOG_FILE_LEVEL: Optional[str] = "DEBUG"
    EXEC_DATA: Optional[str] = "./data/exec"
    
    TIMEOUT: Optional[int] = 30
    RETRY: Optional[int] = 9
    RAY: Optional[bool] = False
    RAY_SERVER: Optional[str] = "localhost:6379"
    RAY_DASHBOARD: Optional[str] = "http://localhost:8265"
    ENV_HOME: Optional[str] = "./data/environ"
    VENV_DIR: Optional[str] = ".venv"
    SYNC: Optional[bool] = True

    @field_validator('URL', mode='before')
    def url_to_str(cls, v):
        if v:
            v = str(v)
            if v.endswith("/"):
                return v[:-1]
            return v

    @field_validator("CONNECT", "CORS_ORIGINS", mode="before")
    def string_to_list(cls, v):
        """Split the env vr environment variable into a list."""
        if isinstance(v, str):
            return [s.strip() for s in v.split(',')]
        return v

    @field_validator("EXEC_DATA", mode="before")
    def make_dir(cls, v):
        if v:
            Path(v).mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("CACHE_DATA", "LOG_FILE", mode="before")
    def make_parent(cls, v):
        if v:
            Path(v).parent.mkdir(parents=True, exist_ok=True)
        return v


class CommandOption(Option):
    RELOAD: Optional[bool] = False


class InternalOption(CommandOption):

    model_config = SettingsConfigDict(
        env_prefix="iKODO_",
        env_file_encoding="utf-8"
    )


class EnvironmentOption(Option):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="KODO_",
        env_file_encoding="utf-8",
        extra="ignore"
    )


class DefaultResponse(BaseModel):
    url: str
    organization: Optional[str] = None
    # connection: Optional[dict[str, Union[datetime.datetime, None]]] = {}
    registry: bool
    feed: bool
    # started_at: datetime.datetime
    idle: bool
    now: datetime.datetime
    message: List[str]
    ray: bool


class Flow(BaseModel):
    url: str
    name: str
    description: Optional[str] = None
    author: Optional[str] = None
    tags: Optional[List[str]] = []
    entry: Optional[str] = None


class NodeInfo(BaseModel):
    url: str
    organization: Optional[str] = None


class Node(NodeInfo):
    created: Optional[datetime.datetime] = None
    modified: Optional[datetime.datetime] = None
    flows: List[Flow]
    heartbeat: Optional[datetime.datetime] = None
    status: Optional[str] = None


class Connect(DefaultResponse):
    nodes: List[Node]


class Disconnect(BaseModel):
    provider: str
    url: List[str]


class Provider(BaseModel):
    url: str
    organization: Optional[str] = None
    feed: bool
    created: Optional[datetime.datetime] = None
    modified: Optional[datetime.datetime] = None
    nodes: List[Node]


class ProviderDump(BaseModel):
    url: str
    organization: Optional[str] = None
    feed: bool
    connection: List[str]
    providers: dict[str, Provider]
    registers: dict[str, Optional[datetime.datetime]]


class ProviderMap(DefaultResponse):
    providers: List[Provider]
    connection: Dict[str, Union[datetime.datetime, None]]
    registers: Dict[str, datetime.datetime]


class IPCinput(BaseModel):
    key: str
    value: Any


class IPCresult(BaseModel):
    content: str
    returncode: int
    stderr: str
    fid: Optional[str] = None
    logging: List[str] = []


class DynamicModel(RootModel[Dict[str, Any]]):
    pass


WorkerMode = Literal[
    "discover",
    "enter",
    "launch",
    "execute"
]


class MODE:
    DISCOVER: WorkerMode = "discover"
    ENTER: WorkerMode = "enter"
    LAUNCH: WorkerMode = "launch"
    EXECUTE: WorkerMode = "execute"


class LaunchResult(BaseModel):
    fid: Optional[str] = None
    payload: Union[str, Dict[str, Any], None] = None
    is_launch: bool
    success: bool
