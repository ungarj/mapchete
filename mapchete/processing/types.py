from dataclasses import dataclass
from typing import Any, Optional

from mapchete.tile import BufferedTile


@dataclass
class TileProcessInfo:
    tile: BufferedTile
    processed: bool = False
    process_msg: Optional[str] = None
    written: bool = False
    write_msg: Optional[str] = None
    data: Optional[Any] = None


@dataclass
class PreprocessingProcessInfo:
    task_key: str
    processed: bool = False
    process_msg: Optional[str] = None
    written: bool = False
    write_msg: Optional[str] = None
    data: Optional[Any] = None
