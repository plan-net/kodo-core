"""
The _kodo_ CLI (command line interface) processes yaml files to execute flows,
crews, agents, tasks and tools. At the moment _kodo_ processes _crewai_
components, only.

You can check and run yaml files to trigger flows. All log files, outputs and
result files persist in `./data/logs`, `./data/output` and `./data/run`.

You can render result files from `./data/run` event files (.ev), for example

    creceo render ./data/run/20210101.ev
"""
import datetime
import re
import shutil
import sys
from collections import OrderedDict
import os
from pathlib import Path

import click
import crewai
from dotenv import load_dotenv
import markdown2
import webbrowser
import jinja2
import tempfile

import kodo.core.engine
import kodo.core.log
from kodo.core.config import setting
from kodo.core.log import echo, event, logger

load_dotenv(override=True)


MARKDOWN_EXTRAS = [
    "fenced-code-blocks", "tables", "sane_lists", "toc",
    "breaks", "cuddled-lists"
]


@click.group()
@click.option('--verbose', '-v', default=False, is_flag=True, help='Verbose output.')
@click.option('--stdout', '-o', default=False, is_flag=True, help='Capture stdout to log file.')
@click.option('--stderr', '-e', default=False, is_flag=True, help='Capture stderr to log file.')
@click.option('--log', type=str, help='Log file base name.', required=False, metavar='LOGFILE')
@click.pass_context
def cli(ctx, verbose, stdout, stderr, log):
    if verbose and (stdout or stderr):
        raise click.UsageError(
            "Options --verbose and --stdout or --stderr are mutually exclusive.")
    events = ctx.invoked_subcommand in ("run", )
    kw = {}
    if log:
        kw["log_basename"] = log
    kodo.core.log.setup_logging(
        verbose=verbose, events=events, stdout=stdout, stderr=stderr, **kw)


@cli.command()
@click.argument('file', default=Path("."), type=click.Path(exists=True))
def check(file):
    """Check yaml file."""
    for f in kodo.core.engine.find_yaml_files(file):
        obj = kodo.core.engine.load_config(f)
        if obj:
            echo(f"checked {f}")


@cli.command()
@click.argument('file', default=Path("."), type=click.Path(exists=True))
@click.argument('args', nargs=-1)
@click.option('--crew', type=str, help='Name of the crew to run.', required=False, metavar='CREW')
@click.option('--dry', '--dry-run', '-n', default=False, is_flag=True, help='Actually do not do anything.')
def run(file, args, crew, dry):
    """Run yaml file."""
    t0 = datetime.datetime.now()
    event("inputs", args)
    event("version", {
        "crewai": crewai.__version__,
        "kodo": kodo.__version__
    })
    inputs = dict(zip(args[::2], args[1::2]))
    scope, instance = kodo.core.engine.glob_config(file)
    if len(instance["crews"]) == 1:
        crew_obj = list(instance["crews"].values())[0]
    elif crew is None:
        raise KeyError("multiple crews, please specify a crew")
    else:
        crew_obj = instance["crews"].get(crew)
    if not crew_obj:
        raise KeyError(f"crew {crew} not found")
    body = []
    for yaml_file in scope:
        with yaml_file.open("r") as f:
            body.append(f"# {str(yaml_file)}\n{f.read()}")
    content = "---\n".join(body)
    event("config", body=content)
    echo(f"running {crew_obj.kodo_name}")
    echo(f"save results to {kodo.core.log.event_logfile}")
    if not dry:
        kodo.core.engine.launch(crew_obj, inputs)
    else:
        logger.info("this is a --dry run")
    t1 = datetime.datetime.now()
    event("done", {
        "runtime": f"{t1-t0}",
        "total_seconds": (t1-t0).total_seconds()
    })
    echo(f"done after {t1 - t0}")


@cli.command()
@click.option('--force', '-f', default=False, is_flag=True, help='Force vacuum.')
@click.option('--dry', '-n', default=False, is_flag=True, help='Actually do not do anything."')
def vacuum(force, dry):
    """Cleanup logs, results and memories."""
    echo(f"vacuuming results older than {setting.RUNS_VACUUM} days")
    now = datetime.datetime.now()
    count = 0
    for file in sorted(Path(setting.RUN_DIRECTORY).glob("*"), reverse=True):
        if not file.suffix in (".log", ".ev"):
            continue
        if not file.with_suffix("").name.isdigit():
            continue
        age = (now - datetime.datetime.fromtimestamp(file.stat().st_mtime))
        outdated = (age.total_seconds() / 86400) > setting.RUNS_VACUUM
        if file.is_file() and (force or outdated):
            logger.info(f"removing {file}, age: {age}")
            if not dry:
                file.unlink()
            count += 1
    echo(f"removed {count} files{' (--dry)' if dry else ''}")


@cli.command()
@click.argument('source', required=False, type=click.File("r"))
@click.option('--target', required=False, type=click.File("w", encoding="utf-8x"))
@click.option('--force', '-f', is_flag=True, help='Overwrite target file.')
@click.option('--template', type=click.File("r"), help='Template file.')
@click.option('--format', type=click.Choice(['md', 'html', 'obsidian']), default='md', help='Output format.')
@click.option('--open', is_flag=True, help='Open target file in default app.')
@click.option('--latest', is_flag=True, help='Render latest flow execution.')
def render(source, target, force, template, format, open, latest):
    """Render yaml file."""

    if source is None:
        if latest:
            run_dir = Path(setting.RUN_DIRECTORY)
            if not run_dir.exists():
                raise FileNotFoundError("run directory does not exist")
            latest_file = max(
                run_dir.glob("*.ev"), 
                key=lambda f: f.stat().st_mtime, 
                default=None
            )
            if latest_file:
                source = latest_file.open("r")
    if source is None:
        raise FileNotFoundError("no source file given")
    echo(f"rendering {source.name}")
    data = kodo.core.engine.revisit(source)

    logger.debug(
        f"found " +
        ", ".join([f"{data[k]} {k}" for k in kodo.core.engine.META_EVENTS if k in data]))
    for agent in data["agents"]:
        echo(f"found {agent['role'].rstrip()} ({agent['kodo_name'].rstrip()}): "
             f"{len(agent['tasks'])} tasks, {len(agent['tools'])} tools")
    echo(f"found {len(data["process"])} events with {
         len(data['agents'])} agents")

    output = []

    def _add(r, eol="\n"): 
        return output.append(r + eol)

    final = data.get("final", None)
    if data.get("crew", None) is None:
        raise RuntimeError("crew not found")
    # header
    _add(f"# {data['crew']['kodo_name']}")
    _add(f"# INPUT")
    for k, v in zip(data["inputs"][0::2], data["inputs"][1::2]):
        _add(f"* `{k}` = `{v}`", eol="")
    _add(f"")
    _add(f"# RUNTIME")
    _add(f"* **progress:** {data["progress"]["current"] / data["progress"]["total"] * 100}%", eol="")
    _add(f"* **runtime:** {data["done"]["runtime"]} ({data["done"]["total_seconds"]}s)", eol="")
    _add(f"* **crewai version:** `{data["version"]["crewai"]}`", eol="")
    _add(f"* **kodo version:** `{data["version"]["kodo"]}`", eol="")
    if final:
        _add(f"* **tokens:**", eol="")
        _add(f"* **tokens:** {final["token_usage"]["total_tokens"]} = " \
             f"{final["token_usage"]["prompt_tokens"]} (prompt) + " \
             f"{final["token_usage"]["completion_tokens"]} (completion)", eol="")
        # _add(f"")
        _add(f"")
        _add(f"# FINAL ANSWER")
        _add(final["raw"])
        _add(f"")
    else:
        _add(f"* no final result found")
        _add(f"")

    def _attr(obj, attr):
        return f"{obj.get(attr, f'{attr} not available').strip()}"

    def cut_string(input_string, length=70):
        addon = ".."
        length -= len(addon)
        if len(input_string) <= length:
            return input_string
        cut_index = input_string.rfind(' ', 0, length)
        if cut_index == -1:
            cut_index = length
        return input_string[:cut_index] + addon

    # agents
    _add(f"# AGENTS")
    for agent in data["agents"]:
        _add(f"## {_attr(agent, 'role')}")
        if agent.get("role", "") != agent.get("kodo_name", ""):
            _add(f"**`{_attr(agent, 'kodo_name')}`**")
        if agent.get("role", None):
            _add(f"### GOAL")
        _add(f"{_attr(agent, 'goal')}")
        if agent.get("role", None):
            _add(f"### BACKSTORY")
        _add(f"{_attr(agent, 'backstory')}")
        # _add(f"")
        # tasks
        _add(f"### TASKS")
        if not agent["tasks"]:
            _add(f"no tasks defined")
        for task in agent["tasks"]:
            _add(f"#### TASK: `{_attr(task, 'name')}`")
            _add(f"##### DESCRIPTION")
            _add(f"{_attr(task, 'description')}")
            _add(f"##### EXPECTED OUTPUT")
            _add(f"{_attr(task, 'expected_output')}")
        # _add(f"")

    # process
    _add(f"# PROCESS")
    process = data["process"]
    if not process:
        _add(f"no process found")
    current_agent = None
    for n, step in enumerate(process):
        kind = step["kind"]
        agent = step.get("agent", None)                
        payload = step["payload"]
        if current_agent is None or agent != current_agent:
            _add(f"## {_attr(agent, 'role')}")
            current_agent = agent
        if kind == "action":
            task = step.get("task", None)
            tool = payload.get("tool", None)
            tool_input = payload.get("tool_input", None)
            _add(f"### {n+1}. {kind.upper()} - {(task or tool or "unknown").lower()}")
            if task is not None:
                _add(f"**{tool}**")
            if tool_input:
                _add(f"**input:**\n\n```\n{tool_input}\n```\n\n")
            # thought = payload.get("thought", "").strip()
            # if thought:
            #     _add(f"##### THOUGHT\n")
            #     _add(f"{thought}\n")
            text = _attr(payload, "text")
            if not text:
                text = _attr(payload, "result")
            # fix invalid markdown
            # _add(f"#### OUTPUT\n")
            _add(f"**output:**")
            _add(f"\n```\n{text}\n```\n")
        elif kind == "result":
            task = payload.get("name", "unknown")
            _add(f"### {n+1}. {kind.upper()} - {task}")
            description = payload.get("description", "no description")
            summary = payload.get("summary", cut_string(description))
            output_format = payload.get("output_format", "no output format")
            raw = payload.get("raw", "")
            pydantic_result = payload.get("pydantic", "no pydantic output")
            json_result = payload.get("json_dict", "not json output")
            _add(f"*{summary}* ({output_format})\n")
            # _add(f"### OUTPUT\n")
            def inc_headline(markdown_text, base=3):
                regex = r"^(#+)\s+(.*?)$"
                def _fix(match):
                    lvl = (len(match.group(1)) + base)
                    if lvl > 6:
                        return f"**{match.group(2)}**"
                    else:
                        return "#" * lvl + " " + match.group(2)
                return re.sub(regex, _fix, markdown_text, flags=re.MULTILINE)
            raw = inc_headline(raw)
            _add(f"{raw}")
        else:
            logger.error(f"unknown process {kind}")
    # config
    _add(f"# YAML CONFIG")
    _add(f"```yaml\n{data['config']['body']}\n```")
    # finally render
    body = re.sub(r"\n{3,}", "\n\n", "\n".join(output))
    # md, then final, else html
    if format == "html":
        tmpl_dir = str(Path(__file__).parent.joinpath("./tmpl"))
        template_loader = jinja2.FileSystemLoader(searchpath=tmpl_dir)
        template_env = jinja2.Environment(loader=template_loader)
        # template_env.globals['static'] = str(tmpl_dir)
        tmpl = template_env.get_template("output.tmpl")
        md = markdown2.Markdown(extras=MARKDOWN_EXTRAS)
        #md = markdown2.markdown(body, extras=MARKDOWN_EXTRAS)
        body = tmpl.render(
            body=md.convert(body),
            toc=md._toc_html
        )        
    vault =os.environ.get("VAULT", None)
    if format == "obsidian":
        if vault is None:
            raise RuntimeError("--format obsidian requires VAULT environment variable")
        if target is None:
            raise RuntimeError("--format obsidian requires --target")
        if target.name != "<stdout>":
            target = Path(vault).joinpath(target.name)
            if target.exists() and not force:
                raise FileExistsError(
                    f"Target file {target} already exists. Use --force to overwrite.")
            target = target.open("w")
        vault_name = Path(vault).name
        fullname = Path(target.name).relative_to(vault)
        url = f"obsidian://open?vault={vault_name}&file={fullname}"
    else:
        if target:
            url = f"file://{Path(target.name).resolve()}"
            if not force and target.name != "<stdout>":
                if Path(target.name).exists():
                    raise FileExistsError(
                        f"Target file {target.name} already exists. Use --force to overwrite.")
        elif force or open:
            raise FileExistsError(f"--force or --open do not make sense without --target")
    if target:
        echo(f"writing to {target.name}")
        target.write(body)
        if open:
            echo(f"opening {url}")
            webbrowser.open(url)
    else:
        print(body)

cli.add_command(check)
cli.add_command(run)


if __name__ == '__main__':
    cli()
