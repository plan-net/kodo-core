import logging
from pathlib import Path
from dotenv import load_dotenv
from pydantic import field_validator
from typing import Any, List
from pydantic_settings import BaseSettings


load_dotenv(override=True)

logger = logging.getLogger("kodo")


class Config(BaseSettings):

    RELOAD: bool = True
    SERVER: str = "http://localhost:3366"
    REQUEST_TIMEOUT: int = 30
    REGISTRY: List[str] | None = None

    model_config = {
        "env_file": ".env",
        "env_prefix": "KODO_",
        "env_file_encoding": "utf-8",
        "extra": "allow"
    }

    @field_validator("REGISTRY", mode="before")
    def split_registry(cls, v):
        """Split the REGISTRY environment variable into a list."""
        if isinstance(v, str):
            return [s.strip() for s in v.split(',')]
        return v


setting = Config()
