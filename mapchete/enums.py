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


class DataType(str, Enum):
    raster = "raster"
    vector = "vector"


class OutputType(str, Enum):
    single_file = "single_file"
    tile_directory = "tile_directory"


class InputType(str, Enum):
    single_file = "single_file"
    tile_directory = "tile_directory"
    mapchete = "mapchete"


class Status(str, Enum):
    r"""
    Status describin life cycle of a Job.

           pending
              |
           parsing --> cancelled|failed
              |
    /--> initializing --> cancelled|failed
    |         |
    |      running --> cancelled|failed
     \    /     |
      retrying  post_processing --> cancelled|failed
                   |
                  done
    """

    # just waiting
    pending = "pending"

    # (A) doing something
    # (A.1) parsing configuration
    parsing = "parsing"
    # (A.2) determine job tasks
    initializing = "initializing"
    # (A.3.a) processing has begun
    running = "running"
    # (A.3.b) processing has failed and is being retried; jumping back to (B.2)
    retrying = "retrying"
    # (A.4)
    post_processing = "post_processing"

    # (B) final stage
    # (B.1) job successfully finished
    done = "done"
    # (B.2) job was cancelled
    cancelled = "cancelled"
    # (B.3) job failed
    failed = "failed"
