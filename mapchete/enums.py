from enum import Enum


class ProcessingMode(str, Enum):
    CONTINUE = "continue"
    READONLY = "readonly"
    OVERWRITE = "overwrite"
    MEMORY = "memory"
