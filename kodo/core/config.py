import logging
from pathlib import Path
from dotenv import load_dotenv
from pydantic import field_validator
from typing import Any
from pydantic_settings import BaseSettings


load_dotenv(override=True)


class Config(BaseSettings):

    LOG_FILE: str = "./data/logs/kodo.log"
    LOG_FILE_SIZE: int = 1024 * 1024 * 5  # 5mb
    LOG_FILE_COUNT: int = 3
    FILE_LOG_LEVEL: int = logging.DEBUG

    STREAM_VERBOSE_LEVEL: int = logging.INFO  # with --verbose
    STREAM_LOG_LEVEL: int = logging.ERROR  # without --verbose

    #LOG_DETAIL: str = "%(asctime)s %(name)s %(levelname)s: %(message)s <%(pathname)s:%(lineno)d>"
    LOG_DETAIL: str = "%(asctime)s %(name)s %(levelname)s: %(message)s"
    LOG_SHORT: str = "\033[31m%(levelname)-8s\033[0m %(name)s: %(message)s"

    OUTPUT_DIRECTORY: str = "./data/output"
    RUN_DIRECTORY: str = "./data/run"
    RUNS_VACUUM: float = 7

    class Config:
        env_file = ".env"
        env_prefix = "KODO_"
        env_file_encoding = "utf-8"
        extra = "allow"

    def model_post_init(self, *args, **kwargs):
        Path(self.OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)
        Path(self.RUN_DIRECTORY).mkdir(parents=True, exist_ok=True)
        Path(self.LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

    @field_validator("STREAM_LOG_LEVEL", "FILE_LOG_LEVEL", mode="before")
    def validate_log_level(cls, v):
        """Transform a log level string into a logging library integer value."""
        log_levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
            "NONE": logging.CRITICAL + 10
        }
        if isinstance(v, str):
            return log_levels.get(v.upper(), logging.NOTSET)
        return v


setting = Config()
