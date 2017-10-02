#!/usr/bin/env python
"""Example process file."""

from shapely.geometry import shape


def execute(mp):
    """User defined process."""
    # Reading and writing data works like this:
    with mp.open(mp.params["input"]["file1"]) as vector_file:
        if vector_file.is_empty():
            # This assures a transparent tile instead of a pink error tile
            # is returned when using mapchete serve.
            return "empty"
        return [
            dict(
                geometry=feature["geometry"],
                properties=dict(
                    name=feature["properties"]["NAME_0"],
                    id=feature["properties"]["ID_0"],
                    area=shape(feature["geometry"]).area
                )
            )
            for feature in vector_file.read()
        ]
