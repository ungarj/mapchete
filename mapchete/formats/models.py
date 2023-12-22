from typing import List

from pydantic import BaseModel, Field

from mapchete.enums import DataType


class DriverMetadata(BaseModel):
    driver_name: str
    data_type: DataType
    mode: str
    file_extensions: List[str] = Field(default_factory=list)
