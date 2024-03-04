from typing import List

from pydantic import BaseModel, Field, NonNegativeInt

from mapchete.enums import DataType, OutputType
from mapchete.tile import BufferedTilePyramid
from mapchete.types import NodataVal


class DriverMetadata(BaseModel):
    driver_name: str
    data_type: DataType
    output_type: OutputType
    modes: List[str]
    file_extensions: List[str] = Field(default_factory=list)


class BaseInputParams(BaseModel, arbitrary_types_allowed=True):
    pyramid: BufferedTilePyramid
    pixelbuffer: NonNegativeInt = 0


class BaseOutputParams(BaseModel):
    stac: dict = Field(default_factory=dict)
    nodata: NodataVal
