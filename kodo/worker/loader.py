import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Union
import aiofiles

import yaml
from litestar.datastructures import State

from kodo.datatypes import (MODE, CommandOption, DynamicModel,
                            EnvironmentOption, Flow, InternalOption, IPCresult,
                            ProviderDump, WorkerMode)
from kodo.error import SetupError
from kodo.helper import parse_factory, now
from kodo.log import logger
from kodo.worker.process import FlowInterProcess

class Loader:

    def __init__(self):
        self.option = None
        self.flows = []

    def setup(self, **kwargs) -> None:
        """
        setup command line options as iKODO_* environment variables
        (before application launch)
        """
        # self.init_option(**kwargs)
        self.option = EnvironmentOption()
        # if self.option.URL and isinstance(self.option.CONNECT, list):
        #     if self.option.URL in self.option.CONNECT:
        #         self.option.CONNECT.remove(self.option.URL)
        self.option.LOADER = kwargs.get("loader", None)
        if self.option.LOADER:
            if Path(self.option.LOADER).exists():
                self.load_option_file()
            else:
                try:
                    factory = parse_factory(self.option.LOADER)
                    if callable(factory):
                        yaml_string: str = factory()
                        if yaml_string:
                            self.load_option(yaml.safe_load(yaml_string))
                    else:
                        raise RuntimeError(
                            f"loader not callable: {self.option.LOADER}")
                except:
                    raise RuntimeError(
                        f"loader not found: {self.option.LOADER}")
        self.option = CommandOption(**{
            **{k: v for k, v in self.option.model_dump().items() if v is not None},
            **{k.upper(): v for k, v in kwargs.items() if v is not None}})
        for k, v in self.option.model_dump().items():
            if v is not None:
                if isinstance(v, list):
                    v = json.dumps(v)
                else:
                    v = str(v)
                os.environ[f"iKODO_{k}"] = v

    def init_option(self, **kwargs):
        env = EnvironmentOption()
        self.option = CommandOption(**{
            **{k: v for k, v in env.model_dump().items() if v is not None},
            **{k.upper(): v for k, v in kwargs.items() if v is not None}})

    def load_option_file(self) -> None:
        with open(self.option.LOADER, "r") as file:
            self.load_option(yaml.safe_load(file))

    def load_option(
            self,
            records: List[Dict[str, Any]]) -> None:
        for record in records:
            for key, value in record.items():
                if key.upper() == "FLOWS":
                    if not isinstance(self.flows, list):
                        raise SetupError(f"flows requires list")
                    self.flows = value
                else:
                    setattr(self.option, key.upper(), value)

    def load(self) -> State:
        """
        load state from iKODO_* environment variables and merge with
        KODO_* environment variables, process the loader option
        """
        env = EnvironmentOption()
        self.option = InternalOption(**{
            **{k: v for k, v in env.model_dump().items() if v is not None},
            **InternalOption().model_dump()})
        state = self.default_state()
        for field in self.option.model_fields:
            value = getattr(self.option, field)
            if value is not None:
                setattr(state, field.lower(), value)
        connect = self.option.CONNECT or []
        state.connection = {str(host): None for host in connect or []}
        state.flows = {}
        if not state.cache_reset:
            self.load_from_cache(state)
        # if self.option.LOADER:
        #     # delegate flow discovery to worker subprocess
        #     worker = FlowDiscovery(self.option.LOADER)
        #     flows = worker.enter()
        #     for line in flows.logging:
        #         record = DynamicModel.model_validate_json(line)
        #         level = record.root.get("level")
        #         message = record.root.get("message")
        #         if level and message:
        #             state.log_queue.append((level, message))
        #     for flow in flows.content.split("\n"):
        #         if flow:
        #             f = Flow.model_validate_json(flow)
        #             state.flows[f.url] = f
        return state

    def default_state(self) -> State:
        return State({
            # settings:
            "url": None,
            "organization": None,
            "connection": None,
            "registry": None,
            "feed": None,
            "cache_data": None,
            "cache_reset": None,
            "screen_level": None,
            "log_file": None,
            "log_file_level": None,
            "exec_data": None,
            "timeout": None,
            "retry": None,
            "ray": None,
            # runtime:
            "registers": {},
            "flows": {},
            "providers": {},
            "exit": False,
            # "entry_points": {},
            "event": 0,
            "heartbeat": None,
            "status": None,
            "log_queue": []
        })

    @staticmethod  
    async def save_to_cache(state: State) -> None:
        dump = ProviderDump(
            url=state.url,
            organization=state.organization,
            connection=state.connection.keys(),
            feed=state.feed,
            providers=state.providers,
            registers=state.registers)
        file = Path(state.cache_data)
        async with aiofiles.open(file, "w") as fh:
            await fh.write(dump.model_dump_json())
        logger.debug(f"saved cache {file}")

    def load_from_cache(self, state) -> bool:  # todo: make async
        file = Path(state.cache_data)
        if not file.exists():
            return False
        with file.open("r") as fh:
            data = fh.read()
        dump = ProviderDump.model_validate_json(data)
        for attr in ('url', 'organization', 'feed'):
            cache_value = getattr(dump, attr, None)
            curr_value = getattr(state, attr)
            # if cache_value != curr_value:
            #     logger.warning(f"{attr}: {cache_value} != {curr_value}")
        # connection = sorted(state.connection.keys())
        # if connection != sorted(dump.connection):
        #     logger.warning(f"connection: {connection} != {dump.connection}")
        state.providers = dump.providers
        state.registers = {r: None for r in state.registers.keys()}
        state.log_queue.append((logging.INFO, "loaded from cache"))
        return True

    @staticmethod
    async def load_flows(state) -> None:
        t0 = now()
        if state.loader:
            # delegate flow discovery to worker subprocess
            worker = FlowDiscovery(state.loader)
            flows = await worker.enter()
            for line in flows.logging:
                record = DynamicModel.model_validate_json(line)
                level = record.root.get("level") or logging.INFO
                message = record.root.get("message")
                logger.log(level, message)
            for flow in flows.content.split("\n"):
                if flow:
                    f = Flow.model_validate_json(flow)
                    state.flows[f.url] = f
        logger.info(f"flows: {len(state.flows)} discovered after {now() - t0}")

class FlowDiscovery(FlowInterProcess):

    def communicate(self, mode: Union[WorkerMode, str]) -> None:
        # executed in the subprocess
        if self.factory is None:
            return
        loader = Loader()
        loader.init_option(LOADER=self.factory)
        if Path(self.factory).exists():
            loader.load_option_file()
        else:
            factory = parse_factory(self.factory)
            yaml_string = factory()
            if yaml_string:
                loader.load_option(yaml.safe_load(yaml_string))
        routes = set()
        for data in loader.flows:
            if data.get("entry", None):
                call: Any = None
                try:
                    call = parse_factory(data["entry"])
                except Exception as e:
                    self.log_msg(logging.ERROR, f"skip {data["entry"]}: {str(e)}")
                    continue
                if call:
                    for field in Flow.model_fields:
                        value = getattr(call.fields, field)
                        curr = data.get(field)
                        if curr is None:
                            data[field] = value
            try:
                flow = Flow(**data)
            except Exception as e:
                self.log_msg(logging.ERROR, f"{data}: {str(e)}")
                continue
            if flow.url in routes:
                self.log_msg(
                    logging.ERROR, f"{data["entry"]}: {flow.url} exists")
                continue
            routes.add(flow.url)
            self.log_msg(
                logging.INFO,
                f"found '{flow.name}' at '{flow.url}' ({flow.entry})")
            self.write_msg(flow.model_dump_json())

    async def enter(self, *args, **kwargs) -> IPCresult:
        # is executed on the node parent
        ret = await self.parse_msg(MODE.DISCOVER, None, None)
        return ret


def default_loader():
    return None
