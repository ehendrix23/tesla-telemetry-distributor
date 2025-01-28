from __future__ import annotations

import logging

LOGLEVELS = dict((logging.getLevelName(level), level) for level in [10, 20, 30, 40, 50])


class Loggingfilter(logging.Filter):
    """
    A logging filter that allows for dynamic log level filtering based on logger names.
    Attributes:
        _loglevel (int): The default log level to filter logs.
        _loglevel_entries (dict[str, int] | None): A dictionary mapping specific logger
                names to their log levels.
    Methods:
        __init__(default_loglevel: str, loglevel_entries: list[str] | None):
            Initializes the Loggingfilter with a default log level and optional
                    specific log levels for certain loggers.
        filter(record: logging.LogRecord) -> bool:
            Determines if a log record should be logged based on the default log level
                    and specific log levels for certain loggers.
    """

    def __init__(
        self, default_loglevel: str, loglevel_entries: list[str] | None
    ) -> None:
        super().__init__()
        self._loglevel = LOGLEVELS[default_loglevel]
        self._loglevel_entries: dict[str, int] | None = {}
        if loglevel_entries is not None:
            for logentry in loglevel_entries:
                entry = logentry.split(sep=":", maxsplit=1)
                if (log_item := entry[0].strip()) == "":
                    # Empty entry.
                    continue
                try:
                    self._loglevel_entries.update(
                        {log_item: LOGLEVELS[entry[1].upper()]}
                    )
                except (IndexError, KeyError):
                    # No log level provided or invalid loglevel provided.
                    continue
        if len(self._loglevel_entries) == 0:
            self._loglevel_entries = None

    def filter(self, record: logging.LogRecord) -> bool:
        # If no specific log entries
        if self._loglevel_entries is not None:
            # Check if we have this, we will need to keep on going higher up.
            name = record.name
            while name != "":
                if (loglevel := self._loglevel_entries.get(name)) is not None:
                    # This function is defined.
                    if record.levelno >= loglevel:
                        return True  # Log it
                    return False  # Do not log this.

                # No luck, remove last part and retry.
                new_name = name.rsplit(".", 1)[0]
                name = "" if new_name == name else new_name

        # If we're here then there is no specific log level defined.
        if record.levelno >= self._loglevel:
            return True  # Log it
        return False


def init_logger(loglevel: str, loglevel_entries: list[str], logfile: str) -> None:
    """
    Initializes the logger with the specified log level, log level entries, and log file.
    Args:
        loglevel (str): The default log level for the logger.
        loglevel_entries (list[str]): A list of log level entries to filter logs.
        logfile (str): The path to the log file. If None, logs will be output to the console.
    Returns:
        None
    """

    logging_handler: logging.StreamHandler | logging.FileHandler
    if logfile is None:
        # Console handler
        logging_handler = logging.StreamHandler()
    else:
        logging_handler = logging.FileHandler(filename=logfile, encoding="utf-8")

    # Add the filter to the handlers.
    logging_handler.addFilter(
        Loggingfilter(
            default_loglevel=loglevel,
            loglevel_entries=loglevel_entries,
        )
    )

    # Set to debug to that this handler gets everything
    logging_handler.setLevel("DEBUG")

    # Set the format
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:\t%(name)s\t%(message)s")
    logging_handler.setFormatter(formatter)

    # Add the handlers to the root level.
    logging.getLogger("").addHandler(logging_handler)
    logging.getLogger("").setLevel(logging.DEBUG)

    logger = logging.getLogger("teslemetry")
    logger.setLevel(logging.DEBUG)
    return None
