"""
Custom loggers for external code such as user processes & drivers.

This is necessary because if using the logging module directly, the namespace
is not assigned properly and log levels & log handlers cannot be assigned
correctly.
"""

import logging
import sys
from itertools import chain

from mapchete.registered import drivers, processes

all_mapchete_packages = set(v.value.split(".")[0] for v in chain(drivers, processes))

key_value_replace_patterns = {
    "AWS_ACCESS_KEY_ID": "***",
    "AWS_SECRET_ACCESS_KEY": "***",
}


class KeyValueFilter(logging.Filter):
    """
    This filter looks for dictionaries passed on to log messages and replaces its values
    with a replacement if key matches the pattern.

    Examples
    --------
    >>> stream_handler.addFilter(
    ...     KeyValueFilter(
    ...         key_value_replace={
    ...             "AWS_ACCESS_KEY_ID": "***",
    ...             "AWS_SECRET_ACCESS_KEY": "***",
    ...         }
    ...     )
    ... )
    """

    def __init__(self, key_value_replace=None):
        super(KeyValueFilter, self).__init__()
        self._key_value_replace = key_value_replace or {}

    def filter(self, record):
        record.msg = self.redact(record.msg)
        if isinstance(record.args, dict):
            for k, v in record.args.items():
                record.args[k] = self.redact({k: v})[k]
        else:
            record.args = tuple(self.redact(arg) for arg in record.args)
        return True

    def redact(self, msg):
        if isinstance(msg, dict):
            out_msg = {}
            for k, v in msg.items():
                if isinstance(v, dict):
                    v = self.redact(v)
                else:
                    for k_replace, v_replace in self._key_value_replace.items():
                        v = v_replace if k == k_replace else v
                out_msg[k] = v
        else:
            out_msg = msg

        return out_msg


# lower stream output log level
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.WARNING)
stream_handler.addFilter(KeyValueFilter(key_value_replace=key_value_replace_patterns))
for i in all_mapchete_packages:
    logging.getLogger(i).addHandler(stream_handler)


def add_module_logger(module_name):
    # set loglevel
    logging.getLogger(module_name).setLevel(
        logging.getLogger("mapchete").getEffectiveLevel()
    )
    # add all configured handlers
    for handler in logging.getLogger("mapchete").handlers:
        logging.getLogger(module_name).addHandler(handler)


def set_log_level(loglevel):
    stream_handler.setLevel(loglevel)
    for i in all_mapchete_packages:
        logging.getLogger(i).setLevel(loglevel)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(KeyValueFilter(key_value_replace=key_value_replace_patterns))
    for i in all_mapchete_packages:
        logging.getLogger(i).addHandler(file_handler)
        logging.getLogger(i).setLevel(logging.DEBUG)
