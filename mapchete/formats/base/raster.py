from typing import Any, Iterator, Optional, Tuple

import numpy as np
import numpy.ma as ma
from numpy.typing import DTypeLike
from rasterio.profiles import Profile

from mapchete.errors import (
    MapcheteConfigError,
    MapcheteNodataTile,
    MapcheteProcessOutputError,
)
from mapchete.formats.base._base import InputData, OutputData
from mapchete.io.raster import create_mosaic, extract_from_array, prepare_array
from mapchete.tile import BufferedTile
from mapchete.types import BandIndexes, NodataVal


class RasterInputDriver(InputData):
    count: int
    nodataval: NodataVal
    dtype: DTypeLike
    output_params: dict
    profile: Profile

    def __init__(self, params: dict, **kwargs):
        super().__init__(params, **kwargs)
        self.count = params.get("bands", params.get("count"))
        self.nodataval = params.get("nodata", 0)
        self.dtype = params.get("dtype")

    def bands_count(self, indexes: Optional[BandIndexes] = None) -> int:
        if isinstance(indexes, int):
            return indexes
        elif isinstance(indexes, list):
            return len(indexes)
        else:
            return self.count

    def band_indexes(self, indexes: Optional[BandIndexes] = None) -> BandIndexes:
        return indexes or list(range(1, self.count + 1))

    def extract_subset(
        self,
        input_data_tiles: Iterator[Tuple[BufferedTile, np.ndarray]],
        out_tile: BufferedTile,
    ) -> np.ndarray:
        mosaic = create_mosaic(input_data_tiles)
        return extract_from_array(
            array=prepare_array(
                mosaic.data,
                nodata=self.nodataval,
                dtype=self.dtype,
            ),
            in_affine=mosaic.affine,
            out_grid=out_tile,
        )


class RasterOutputDriver(OutputData):
    count: Optional[int] = None
    nodataval: NodataVal
    dtype: DTypeLike
    output_params: dict
    profile: Profile
    use_stac = True

    def __init__(self, params: dict, **kwargs):
        super().__init__(params, **kwargs)
        count = params.get("bands", params.get("count"))
        if count:
            self.count = count
        else:
            raise MapcheteConfigError(f"cannot determine band count from {params}")
        self.nodataval = params.get("nodata", 0)
        self.dtype = params.get("dtype")

    def bands_count(self, indexes: Optional[BandIndexes] = None) -> int:
        if isinstance(indexes, int):
            return indexes
        elif isinstance(indexes, list):
            return len(indexes)
        elif self.count:
            return self.count
        else:
            raise ValueError(f"cannot determine band count for {self}")

    def band_indexes(self, indexes: Optional[BandIndexes] = None) -> BandIndexes:
        if indexes:
            return indexes
        elif self.count:
            return list(range(1, self.count + 1))
        else:
            raise ValueError(f"cannot determine band count for {self}")

    def output_is_valid(self, process_data: Any) -> bool:
        return is_numpy_or_masked_array(
            process_data
        ) or is_numpy_or_masked_array_with_tags(process_data)

    def output_cleaned(
        self, process_data: Any
    ):  # -> Union[ma.MaskedArray, Tuple[ma.MaskedArray, dict]]:
        if is_numpy_or_masked_array(process_data):
            return prepare_array(
                process_data,
                masked=True,
                nodata=self.nodataval,
                dtype=self.dtype,
            )
        elif is_numpy_or_masked_array_with_tags(process_data):
            data, tags = process_data
            return self.output_cleaned(data), tags

    def streamline_output(
        self, process_data: Any
    ):  # -> Union[ma.MaskedArray, Tuple[ma.MaskedArray, dict]]:
        if isinstance(process_data, str) and process_data == "empty":
            raise MapcheteNodataTile
        elif process_data is None:  # pragma: no cover
            raise MapcheteProcessOutputError("process output is empty")
        elif self.output_is_valid(process_data):
            return self.output_cleaned(process_data)
        else:
            raise MapcheteProcessOutputError(
                "invalid output type: %s" % type(process_data)
            )

    def prepare(self, *args, **kwargs):
        pass


def is_numpy_or_masked_array(data) -> bool:
    return isinstance(data, (np.ndarray, ma.MaskedArray))


def is_numpy_or_masked_array_with_tags(data) -> bool:
    return (
        isinstance(data, tuple)
        and len(data) == 2
        and is_numpy_or_masked_array(data[0])
        and isinstance(data[1], dict)
    )
