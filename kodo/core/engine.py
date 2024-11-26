import inspect
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
import yaml

import crewai
import crewai.process
from crewai.agents.parser import AgentAction
from crewai.tasks.task_output import TaskOutput

from kodo.core.config import setting
from kodo.core.log import echo, event, load_event, logger

KODO_MIN_VERSION = "0.1"
PROGRESS: dict = {"current": 0, "total": 0}

AGENT_EVENT = "agent"
TASK_EVENT = "task"
META_EVENTS = ("inputs", "version", "config", "crew", "progress", "final", "done")
PROCESS_EVENTS = (TASK_EVENT, "action", "action-error", "result", "result-error")


def find_yaml_files(file):
    scope = []
    file = Path(file)
    if file.is_file():
        scope.append(file)
    else:
        logger.info(f"reading yaml files from {file.absolute()}")
        scope = list(file.rglob('*.yaml'))
    return scope


def step_callback(action: AgentAction):
    """
    The step callback expects an action executed by the agent. A step can be
    a tool usage, as well as internal thoughts, obersvations and decisions.
    """
    # note: for crewai text is the concatenation of
    # 1) .thought (Thought)
    # 2) .tool and .tool_input (Action)
    # 3) and .result (Observation)
    keys = ("thought", "tool", "tool_input", "text", "result")
    try:
        current_frame = inspect.currentframe()
        frame = current_frame.f_back if current_frame else None
        obj = frame.f_locals.get('self', None) if frame else None
        if obj:
            agent = getattr(obj, "agent", None)
            agent_name = getattr(agent, "kodo_name", None) if agent else None
            task = getattr(obj, "task", None)
            task_name = getattr(task, "name", None) if task else None
        else:
            agent_name = None
            task_name = None
        event(
            f"action",
            {
                **{k: getattr(action, k, None) for k in keys},
                **{"agent": agent_name, "task": task_name}
            }
        )
        action_name = getattr(action, "tool", f"no tool {str(action)}")
        logger.info(f"action: '{action_name}' by {agent}/{task}")
    except Exception as e:
        logger.critical(
            f"action-error in {action}, __dict__ {action.__dict__}: {e}")
        event("action-error", _dict=action.__dict__, exception=str(e))


def task_callback(output: TaskOutput, kind="result"):
    """
    The task callback expects an the result of a task. The crew and agent has a
    clearly defined number of tasks.  executed by the agent. A step can be
    a tool usage, as well as internal thoughts, obersvations and decisions.
    """
    keys = ("name", "raw", "pydantic", "json_dict", "agent",
            "description", "expected_output", "summary")
    try:
        event(
            kind,
            {
                **{k: getattr(output, k, None) for k in keys},
                **{"output_format": output.output_format.strip()}
            }
        )
        logger.info(f"{kind}: {output.agent.strip()}/{output.name.strip()}'")
        progress()
    except Exception as e:
        logger.critical(
            f"result-error in {output}, __dict__ {output.__dict__}: {e}")
        event("result-error", _dict=output.__dict__, exception=str(e))


def manager_callback(*args, **kwargs):
    print("X"*300)


def _make_progress():
    global PROGRESS
    event("progress", PROGRESS)
    echo(f"step {PROGRESS['current']}/{PROGRESS['total']}"
         f" - {PROGRESS['current']/PROGRESS['total']:.0%}")


def set_progress(n: int) -> None:
    global PROGRESS
    PROGRESS["total"] = n
    _make_progress()


def progress(n: int = 1) -> None:
    global PROGRESS
    PROGRESS["current"] += n
    _make_progress()


def _validate_parameters(label, config, *params):
    for key in config.keys():
        if key not in params:
            raise KeyError(f"unexpected {label} key: {key}")


def _resolve_object(tool: Union[str, None]) -> Union[List, None]:
    if tool is None:
        return None
    if "(" not in tool:
        raise SyntaxError(f"invalid object arguments: {tool}")
    full_name = tool.split('(', 1)[0]
    *mod_name, cls_name = full_name.split('.')
    mod = __import__(".".join(mod_name).strip())
    cls = getattr(mod, cls_name.strip())
    params = tool[len(full_name):]
    if params.strip() == "":
        raise SyntaxError(f"missing tool arguments: {tool}")
    param_dict = eval(f"dict{params}")
    tool_instance = cls(**param_dict)
    return tool_instance


def _resolve_tools(config):
    tools = []
    for tool in config.get("tools", []):
        tool_instance = _resolve_object(tool)
        tools.append(tool_instance)
        logger.debug(f"initializsed tools: {tool}")
    return tools


def load_agents(name: str, config: dict) -> Tuple[crewai.Agent, dict]:
    """
    Load agent from a dictionary configuration. Arguments:

    - role (str): The role of the agent.
    - goal (str): The objective of the agent.
    - backstory (str): The backstory of the agent.
    - max_iter (int, 25): Maximum number of iterations for an agent to execute a task.
    - max_rpm (int): Maximum number of requests per minute for the agent execution to be respected.
    - memory (bool): Whether the agent should have memory or not.
    - allow_delegation (bool): Whether the agent is allowed to delegate tasks to other agents.
    - tools (list of str, will be resolved to objects): Tools at agents disposal
    - llm (str): The language model that will run the agent.
    - cache (bool, True): Whether the agent should cache the results of the execution.
    - allow_code_execution (bool, False): Enable code execution for the agent. 
    - max_retry_limit (int, 2): Maximum number of retries for an agent to execute a task when an error occurs.
    - respect_context_window (bool, True): Summary strategy to avoid overflowing the context window. 
    - code_execution_mode (str, "safe"): Determines the mode for code execution: "safe" (using Docker) or "unsafe" (direct execution on the host machine).  
    - max_execution_time (int): Maximum time in seconds for the agent to execute a task.

    The following Agent attributes are ignored:

    - system_template: Specifies the system format for the agent.
    - prompt_template: Specifies the prompt format for the agent.
    - response_template: Specifies the response format for the agent.
    - verbose (bool, True): Whether the agent execution should be in verbose mode.
    - use_system_prompt (bool, True): Adds the ability to not use system prompt
    - agent_executor: An instance of the CrewAgentExecutor class.
    - function_calling_llm: The language model that will handle the tool calling for this agent, it overrides the crew function_calling_llm.
    - step_callback: Callback to be executed after each step of the agent execution.    
    - config: Dict representation of agent configuration.
    """
    _validate_parameters(
        "agent", config, "role", "goal", "backstory", "max_iter", "max_rpm",
        "memory", "allow_delegation", "tools", "llm", "cache",
        "allow_code_execution", "max_retry_limit", "respect_context_window",
        "code_execution_mode", "max_execution_time")
    config["tools"] = _resolve_tools(config)
    config["verbose"] = False  # config.get("verbose", False)
    config["name"] = name
    # config["step_callback"] = _agent_step_callback
    config["llm"] = _resolve_object(config.get("llm", None))
    agent = crewai.Agent(**{k: v for k, v in config.items() if k is not None})
    agent.__dict__["kodo_name"] = name.strip()
    return agent, {}


def load_tasks(name: str, config: dict) -> Tuple[crewai.Task, dict]:
    """
    Load task from a dictionary configuration. Arguments:

    - agent (str, resolved to object): Agent responsible for task execution. Represents entity performing task.
    - expected_output (str): Clear definition of expected task outcome.
    - description (str): Descriptive text detailing task's purpose and execution.
    - async_execution (bool, False): Boolean flag indicating asynchronous task execution.
    - context (List of str, resolved to objects): List of Task instances providing task context or input data.
    - tools (List of str, resolved to objects): List of tools/resources limited for task execution.

    The following Task attributes are ignored:

    - config: Dictionary containing task-specific configuration parameters.
    - callback: Function/object executed post task completion for additional actions.
    - output: An instance of TaskOutput, containing the raw, JSON, and Pydantic output plus additional details.
    - output_file: File path for storing task output.
    - human_input (bool, False): Indicates if the task should involve human review at the end, useful for tasks needing human oversight.
    - converter_cls (Converter): A converter class used to export structured output.
    - output_json (BaseModel): Pydantic model for structuring JSON output.
    - output_pydantic (BaseModel): Pydantic model for task output.
    """
    _validate_parameters(
        "task", config, "agent", "expected_output", "description",
        "async_execution", "context", "tools")
    config["tools"] = _resolve_tools(config)
    # config["callback"] = _task_callback
    config["output_file"] = str(
        Path(setting.OUTPUT_DIRECTORY) / f"{name}.output")
    lazyload = {
        "agent": config.get("agent", None),
        "context": config.get("context", None),
    }
    config["agent"] = None
    config["context"] = None
    config["name"] = name.strip()
    task = crewai.Task(**{k: v for k, v in config.items() if k is not None})
    return task, lazyload


def load_crews(name: str, config: dict) -> Tuple[crewai.Crew, dict]:
    """
    Load crew from a dictionary configuration. Arguments:

    - tasks (List of str, resolved): List of tasks assigned to the crew.
    - agents (List of str, resolved): List of agents part of this crew.
    - memory (bool, False): Whether the crew should use memory to store memories of it's execution.
    - cache (bool, True): Whether the crew should use a cache to store the results of the tools execution.
    - process (str, "sequential" or "hierarchical"): The process flow that the crew will follow.
    - max_rpm (int): Maximum number of requests per minute for the crew execution to be respected.
    - planning (bool): Plan the crew execution and add the plan to the crew.
    - manager_llm : The language model that will run manager agent.
    - planning_llm: The language model that will run the planning agent.

    The following Task attributes are ignored:

    - id: A unique identifier for the crew instance.
    - verbose (bool, True): Indicates the verbosity level for logging during execution.
    - manager_agent: Custom agent that will be used as manager.
    - function_calling_llm: The language model that will run the tool calling for all the agents.
    - config: Configuration settings for the crew.
    - prompt_file: Path to the prompt json file to be used for the crew.
    - step_callback: Callback to be executed after each step for every agents execution.
    - task_callback: Callback to be executed after each task for every agents execution.
    - share_crew: Whether you want to share the complete crew information and execution with crewAI to make the library better, and allow us to train models.
    - language: The language in which the crew will operate (dispatches the prompt_file)
    - language_file: Path to the language file to be used for the crew.
    - embedder: Configuration for the embedder to be used by the crew. Mostly used by memory for now.
    - full_output (bool, False): Whether the crew should return the full output with all tasks outputs or just the final output.
    - output_log_file (bool or str): Path to the output log file.
    - manager_callbacks: a list of callback handlers to be executed by the manager agent when a hierarchical process is used.
    - prompt_file: Path to the prompt json file to be used for the crew.
    """
    _validate_parameters(
        "crew", config, "tasks", "agents", "memory", "cache", "process",
        "max_rpm", "planning", "manager_llm", "planning_llm")
    config["tools"] = _resolve_tools(config)
    config["share_crew"] = False
    config["full_output"] = True
    config["verbose"] = False  # config.get("verbose", True)
    config["step_callback"] = step_callback
    config["task_callback"] = task_callback
    config["manager_callbacks"] = [manager_callback]  # todo: BUG! not working
    config["manager_llm"] = _resolve_object(config.get("manager_llm", None))
    config["planning_llm"] = _resolve_object(config.get("planing_llm", None))
    lazyload = {
        "tasks": config.get("tasks", None) or [],
        "agents": config.get("agents", None) or [],
    }
    crew = crewai.Crew(**{k: v for k, v in config.items() if k is not None})
    crew.__dict__["kodo_name"] = name.strip()
    return crew, lazyload


def load_config(yaml_file: Union[str, Path]) -> dict:
    if isinstance(yaml_file, str):
        config = yaml.safe_load(yaml_file) or {}
    else:
        config = yaml.safe_load(yaml_file.open("r")) or {}
    if not (isinstance(config, dict) and "kodo" in config):
        logger.warning(f"skip {str(yaml_file)}")
        return {}
    if isinstance(config["kodo"], dict):
        version = config["kodo"].get("version", "")
    else:
        version = config["kodo"]
    if str(version) != KODO_MIN_VERSION:
        logger.warning(f"kodo version {KODO_MIN_VERSION} required")
        return {}
    logger.info(f"loading {yaml_file}")
    # verify top-level keys
    for key in config.keys():
        if key not in ("kodo", "agents", "tasks", "crews", "config"):
            raise KeyError(f"unexpected key: {key}")
    # verify relevant top-level keys exists and type
    for key in ("agents", "tasks", "crews"):
        if key not in config:
            logger.warning(f"no {key} found in yaml file")
        elif not isinstance(config[key], dict):
            raise ValueError(f"{key} must be a dictionary")
    # instantiate agents and tasks
    obj: Dict[str, Dict] = {"agents": {}, "tasks": {}, "crews": {}}
    lazy: Dict[str, Dict] = {"agents": {}, "tasks": {}, "crews": {}}
    for key in ("agents", "tasks"):
        for name in config.get(key, []):
            meth = globals().get(f"load_{key}", None)
            if not isinstance(config[key][name], dict):
                raise ValueError(f"{name} must be a dictionary")
            logger.debug(f"initialised {key}: {name}")
            obj[key][name], lazy[key][name] = meth(name, config[key][name])
    # resolve tasks (agent and context)
    for task in obj["tasks"]:
        # resolve agent
        agent_name = lazy["tasks"][task]["agent"]
        if agent_name:
            obj["tasks"][task].agent = obj["agents"][agent_name]
        # resolve context
        context = lazy["tasks"][task]["context"]
        if context:
            resolved = []
            for context_task in context:
                resolved.append(obj["tasks"][context_task])
            obj["tasks"][task].context = resolved
    # instantiate crews
    for name in config.get("crews", []):
        if not isinstance(config["crews"][name], dict):
            raise ValueError(f"{name} must be a dictionary")
        # resolve tasks
        tasks = []
        for task_name in config["crews"][name].get("tasks", []):
            tasks.append(obj["tasks"][task_name])
        config["crews"][name]["tasks"] = tasks
        # resolve agents
        agents = []
        for agent_name in config["crews"][name].get("agents", []):
            agents.append(obj["agents"][agent_name])
        config["crews"][name]["agents"] = agents
        obj["crews"][name], _ = load_crews(name, config["crews"][name])
        logger.debug(f"initialised crew: {name}")
    return obj


def glob_config(file: Path):
    scope = find_yaml_files(file)
    instance: dict = {"agents": {}, "tasks": {}, "crews": {}}
    for f in scope:
        logger.info(f"reading {f.relative_to(file)}")
        i = load_config(f)
        for k in instance:
            for name in i.get(k, []):
                if name in instance[k]:
                    raise KeyError(f"{k} {name} already exists")
                instance[k][name] = i[k][name]
    return scope, instance


def launch(crew: crewai.crew.Crew, inputs) -> crewai.crew.CrewOutput:

    def _rstrip(s: str) -> str:
        return s.rstrip() if isinstance(s, str) else s


    def _clean_dict(obj: Any, lookup: tuple):
        return {
            k: _rstrip(v)
            for k in lookup if (v := getattr(obj, k)) is not None
        }

    def _capture_crew(crew: crewai.crew.Crew):
        lu = ("name", "kodo_name")
        event("crew", _clean_dict(crew, lu))

    def _capture_agent(agent: crewai.Agent):
        lu = (
            "role", "goal", "backstory", "kodo_name")
        event("agent", {
            **_clean_dict(agent, lu),
            **{
                "tools": [
                    _clean_dict(t, ("name", "description")) for t in agent.tools
                ]
            }
        })

    def _capture_task(task: crewai.Task):
        lu = ("name", "description", "expected_output")
        event("task", {**_clean_dict(task, lu), **
              {"agent": _rstrip(task.agent.kodo_name)}})

    # header
    _capture_crew(crew)
    for agent in crew.agents:
        _capture_agent(agent)
    for task in crew.tasks:
        _capture_task(task)

    # start processing
    set_progress(len(crew.tasks))
    ret = crew.kickoff(inputs)
    event(
        "final", {
            **{"token_usage": ret.token_usage.dict()},
            **_clean_dict(ret, ("raw", "pydantic", "json_dict"))
        }
    )


def revisit(source) -> dict:

    data: dict = {
        **{k: None for k in META_EVENTS},
        **{"agents": [], "process": []}
    }
    agent_lookup = {}

    def _save_agent(name):
        if name not in agent_lookup:
            data["agents"].append({
                "role": name,
                "kodo_name": name,
                "tasks": [],
                "tools": []
            })
            agent_lookup[name] = data["agents"][-1]
        return agent_lookup[name]

    for line in source:
        line = line.rstrip()
        if line:
            timestamp, kind, payload = load_event(line)
            if kind in META_EVENTS:
                data[kind] = payload
            elif kind == AGENT_EVENT:
                # build
                payload["tasks"] = []
                data["agents"].append(payload)
                agent_lookup[payload["kodo_name"]] = data["agents"][-1]
            elif kind in PROCESS_EVENTS:
                agent_name = payload.get("agent", None)
                agent = _save_agent(agent_name)
                if kind == TASK_EVENT:
                    # pack under agent
                    agent["tasks"].append(payload)
                else:
                    # collect under process
                    data["process"].append({
                        "kind": kind,
                        "payload": payload,
                        "agent": agent
                    })
            else:
                logger.warning(f"unknown kind {timestamp}: {kind}")
    return data
