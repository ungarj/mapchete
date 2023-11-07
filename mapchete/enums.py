from enum import Enum


class ProcessingMode(str, Enum):
    CONTINUE = "continue"
    READONLY = "readonly"
    OVERWRITE = "overwrite"
    MEMORY = "memory"


class Concurrency(str, Enum):
    none = "none"
    threads = "threads"
    processes = "processes"
    dask = "dask"
