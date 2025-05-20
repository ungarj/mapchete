"""Example process file."""

from typing import Optional
from mapchete.errors import MapcheteNodataTile
from mapchete.formats.protocols import RasterInput


def execute(
    required_file: RasterInput,
    optional_file: Optional[RasterInput] = None,
    some_integer_parameter: int = 0,
    some_float_parameter: float = 1.0,
    some_string_parameter: str = "foo",
    some_bool_parameter: bool = False,
):
    """User defined process."""
    # Reading and writing data works like this:
    if optional_file:
        if optional_file.is_empty():
            return "empty"
            # This assures a transparent tile instead of a pink error tile
            # is returned when using mapchete serve.
        dem = optional_file.read(resampling="bilinear")
        return dem
    else:
        return required_file.read(resampling="bilinear")
    raise MapcheteNodataTile
