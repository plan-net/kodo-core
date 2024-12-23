import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from pydantic import BaseModel, field_validator


class InternalEnviron(BaseModel):
    URL: str
    ORGANIZATION: Optional[str] = None
    CONNECT: Optional[List[str]] = []
    NODE: bool
    REGISTRY: bool
    PROVIDER: bool
    EXPLORER: bool
    LOADER: Optional[str] = None
    CACHE: Optional[str] = None
    DEBUG: bool

    @field_validator('URL', mode='before')
    def convert_url_to_str(cls, v):
        return str(v)

    @field_validator('CONNECT', mode='before')
    def convert_connect_elements_to_str(cls, v):
        if v:
            return [str(item) for item in v]
        return None

    class Config:
        env_prefix = 'iKODO_'


class Flow(BaseModel):
    url: str
    name: str
    description: Optional[str] = "missing description"
    tags: Optional[List[str]] = []
    author: Optional[str] = "missing author"


class FlowRecord(Flow):
    registry_url: str
    node_url: str
    created: datetime.datetime
    modified: datetime.datetime
    heartbeat: datetime.datetime


class NodeBase(BaseModel):
    url: str
    organization: Optional[str] = "unknown organization"

class Node(NodeBase):
    flows: Dict[str, Flow]
    created: Optional[datetime.datetime] = None
    modified: Optional[datetime.datetime] = None
    heartbeat: Optional[datetime.datetime] = None
    status: str = "unknown"


class ProviderOffer(NodeBase):
    feed: bool
    nodes: Dict[str, Node]


class Provider(ProviderOffer):
    created: datetime.datetime
    modified: datetime.datetime
    connect: Optional[bool] = False
    heartbeat: datetime.datetime


class ProviderDump(ProviderOffer):
    providers: Dict[str, Provider]


