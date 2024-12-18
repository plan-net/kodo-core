import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from pydantic import BaseModel, field_validator


class InternalEnviron(BaseModel):
    URL: str
    CONNECT: Optional[List[str]] = []
    NODE: bool
    REGISTRY: bool
    PROVIDER: bool
    EXPLORER: bool
    LOADER: Optional[str]
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


class FlowRecord(BaseModel):
    url: str
    name: str
    source: str
    node: str
    created: datetime.datetime
    modified: datetime.datetime


class Node(BaseModel):
    url: str
    flows: Dict[str, Flow]
    created: Optional[datetime.datetime] = None
    modified: Optional[datetime.datetime] = None


class ProviderOffer(BaseModel):
    url: str
    feed: bool
    nodes: Dict[str, Node]


class Provider(BaseModel):
    url: str
    # True, if the provider wants feedback on nodes and providers (is a registry)
    feed: bool
    nodes: Dict[str, Node]
    created: datetime.datetime
    modified: datetime.datetime
    connect: Optional[bool] = False


# class ProviderResponse(BaseModel):
#     url: str
#     nodes: Dict[str, Node]


class ProviderDump(BaseModel):
    url: str
    feed: bool
    nodes: Dict[str, Node]
    providers: Dict[str, Provider]
