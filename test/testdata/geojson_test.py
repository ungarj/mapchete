"""Example process file."""

from mapchete.errors import MapcheteNodataTile
from shapely.geometry import shape


def execute(mp):
    """User defined process."""
    # Reading and writing data works like this:
    with mp.open("file1") as vector_file:
        if vector_file.is_empty():
            raise MapcheteNodataTile
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
