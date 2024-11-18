"""
kodo logging has three modes:
* no setup, no logging
* with setup and default arguments (no verbose, no events, no no-output)
  * log to file kodo.log
  * log to stderr with ERROR level
  * show echo
* with --verbose
  * log to file kodo.log
  * log to stderr with INFO level
  * hide echo
* with --no-output
  * log to file kodo.log
  * no output to stderr, no output to stdout

  default: ERROR to stderr, echo to stdout
  --verbose: INFO to stderr, echo to stderr
  --no-output: no logging and no echo

1) all logging goes to kodo.log, based on FILE_LOG_LEVEL
2) normal ops echos to STDOUT, STREAM_LOG_LEVEL (ERROR) to stderr
3) --verbose with STREAM_VERBOSE_LEVEL (INFO) to stderr
4) --no-echo no echo to STDOUT
5) --quiet no logging to stderr
"""
import datetime
import json
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Any

import click

from kodo.core.config import setting

def _c(r):
    return datetime.timedelta(seconds=r["total_seconds"])

CONVERTER = {
    "runtime": lambda r: _c(r),
    None: lambda r: r
}


class StreamInterceptor:

    def __init__(self, stream, *callback):
        self.stream = stream
        self.callbacks = list(callback) or []

    def add_callback(self, callback):
        self.callbacks.append(callback)

    def write(self, message):
        for callback in self.callbacks:
            callback(self.stream, message)

    def flush(self):
        self.stream.flush()


logger = logging.getLogger("kodo")
logfile = None
event_logger = logging.getLogger("kodo_ev")
event_logfile = None
echo_stream = None


def setup_logging(verbose=False, events=False, stdout=False, stderr=False,
                  log_basename=None):
    global logger, logfile
    global event_logger, event_logfile
    global echo_stream

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # frozen

    logger = logging.getLogger("kodo")  # frozen
    logger.setLevel(logging.DEBUG)  # frozen
    if events:
        if log_basename:
            logfile = Path(log_basename).with_suffix(".log")
        else:
            logfile = Path(setting.RUN_DIRECTORY).joinpath(f"{now}.log")
        file_handler = logging.FileHandler(logfile, mode='w')
        if log_basename:
            event_logfile = Path(log_basename).with_suffix(".ev")
        else:
            event_logfile = Path(setting.RUN_DIRECTORY).joinpath(f"{now}.ev")
        event_logger = logging.getLogger("kodo.ev")
        event_logger.setLevel(logging.INFO)
        event_handler = logging.FileHandler(str(event_logfile), mode='w')
        event_handler.setFormatter(
            logging.Formatter("%(asctime)s|%(message)s"))
        event_logger.addHandler(event_handler)
        event_logger.propagate = False
    else:
        if log_basename:
            logfile = Path(log_basename).with_suffix(".log")
        else:
            logfile = Path(setting.LOG_FILE)
        file_handler = logging.handlers.RotatingFileHandler(
            logfile, maxBytes=setting.LOG_FILE_SIZE,
            backupCount=setting.LOG_FILE_COUNT)

    file_handler.setFormatter(logging.Formatter(setting.LOG_DETAIL))
    file_handler.setLevel(setting.FILE_LOG_LEVEL)
    root.addHandler(file_handler)

    def callback(stream, message):
        if message.strip():
            try:
                stream_text = getattr(stream, 'name', '<unknown>')
            except Exception as e:
                message = e
                stream_text = "<except>"
            logger.info(f"{stream_text}: {message.rstrip()}")


    if verbose:
        level = setting.STREAM_VERBOSE_LEVEL
        # show logging with VERBOSE level on stderr
        console_stream = sys.stderr
        stream_handler = logging.StreamHandler(stream=console_stream)
        stream_handler.setFormatter(logging.Formatter(setting.LOG_SHORT))
        stream_handler.setLevel(level)
        root.addHandler(stream_handler)
        # no echo, which goes through logging here
        echo_stream = None
    else:
        # show logging with LOG LEVEL on stderr
        level = setting.STREAM_LOG_LEVEL
        # echo to stderr
        echo_stream = sys.stderr
        console_stream = sys.stderr

        if stdout or stderr:
            # intercept stdout to capture
            if stdout:
                sys.stdout = StreamInterceptor(sys.stdout, callback)
            # intercept stderr to capture
            if stderr:
                sys.stderr = StreamInterceptor(sys.stderr, callback)
            # direct echo stream
            if stdout and stderr:
                echo_stream = None
                console_stream = None
            elif stdout:
                echo_stream = sys.stderr
                console_stream = sys.stderr
            else: 
                echo_stream = sys.stdout
                console_stream = sys.stdout
        if console_stream:
            stream_handler = logging.StreamHandler(stream=console_stream)
            stream_handler.setFormatter(logging.Formatter(setting.LOG_SHORT))
            stream_handler.setLevel(level)
            root.addHandler(stream_handler)

    logger.debug("logging setup")


def event(kind: str, *args, **kwargs):
    variable = args[0] if args else kwargs
    event_logger.info(f"{kind}|{json.dumps(variable)}")


def load_event(line):
    timestamp, kind, record = line.split("|", 2)[0:3]
    payload = json.loads(record)
    return timestamp, kind, CONVERTER.get(kind, CONVERTER[None])(payload)


def echo(message):
    global echo_stream
    if echo_stream:
        click.echo(click.style(message, fg="green"), file=echo_stream)
    logger.info(message)
