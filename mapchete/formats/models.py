from pydantic import BaseModel
from typing import Any, Dict, Optional


class DriverClassMetadata(BaseModel):
    name: str
    data_type: str = None
    file_extensions: list = []
    input_reader: bool = False
    output_reader: bool = False
    output_writer: bool = False


class DriverMetadata(BaseModel):
    name: str
    mode: str
    data_type: Optional[str] = None
    file_extensions: Optional[list] = []
    input_reader_cls: Optional[Any] = None
    output_reader_cls: Optional[Any] = None
    output_writer_cls: Optional[Any] = None
