#!/usr/bin/env python
"""Example process file."""

from shapely.geometry import shape


def execute(mp):
    """User defined process."""
    # Reading and writing data works like this:
    with mp.open(mp.params["input"]["file1"]) as vector_file:
        return [
            dict(
                geometry=feature["geometry"],
                properties=dict(
                    name=feature["properties"].get("NAME_0", None),
                    id=feature["properties"].get("ID_0", None),
                    area=shape(feature["geometry"]).area
                )
            )
            for feature in vector_file.read()
        ]
