"""Example process file."""

from typing import List

from mapchete import VectorInput


def execute(
    vector: VectorInput,
) -> List[dict]:
    """User defined process."""
    # Reading and writing data works like this:
    if vector.is_empty():
        # This assures a transparent tile instead of a pink error tile
        # is returned when using mapchete serve.
        return "empty"

    data = vector.read(validity_check=False)
    return data
