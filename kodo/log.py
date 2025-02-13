import logging

LOG_FORMAT = "%(levelname)-8s " \
             "%(asctime)s %(identifier)s: %(message)s"
LOG_FILE_FORMAT = "%(asctime)s %(levelname)s %(name)s: " \
                  "%(identifier)s - %(message)s"

original_factory = logging.getLogRecordFactory()
identifier = None
logger = logging.getLogger("kodo")  # Set the custom factory



def custom_log_record_factory(*args, **kwargs):
    global original_factory, identifier
    record = original_factory(*args, **kwargs)
    record.identifier = identifier
    return record


logging.setLogRecordFactory(custom_log_record_factory)


def setup_logger(screen_level, log_file, log_file_level):
    global logger
    _log = logging.getLogger()
    _log.setLevel(logging.DEBUG)
    for handler in _log.handlers[:]:
        _log.removeHandler(handler)

    _log = logger
    _log.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, screen_level.upper()))
    ch_formatter = logging.Formatter(LOG_FORMAT)
    ch.setFormatter(ch_formatter)
    _log.addHandler(ch)
#
    fh = logging.FileHandler(log_file, mode="w")
    fh.setLevel(getattr(logging, log_file_level.upper()))
    fh_formatter = logging.Formatter(LOG_FILE_FORMAT)
    fh.setFormatter(fh_formatter)
    _log.addHandler(fh)
#
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.propagate = True

    uvicorn_logger.addHandler(fh)
    uvicorn_logger.addHandler(ch)
    uvicorn_logger.setLevel(logging.WARNING)
