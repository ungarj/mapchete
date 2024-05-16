import pytest
from shapely.errors import WKTReadingError
from shapely.geometry import mapping, shape

from mapchete.config.parse import bounds_from_opts, guess_geometry
from mapchete.io import fiona_open
from mapchete.types import Bounds


def test_guess_geometry(aoi_br_geojson):
    with fiona_open(aoi_br_geojson) as src:
        area = shape(next(iter(src))["geometry"])

    # WKT
    geom, crs = guess_geometry(area.wkt)
    assert geom.is_valid
    assert crs is None

    # GeoJSON mapping
    geom, crs = guess_geometry(mapping(area))
    assert geom.is_valid
    assert crs is None

    # shapely Geometry
    geom, crs = guess_geometry(area)
    assert geom.is_valid
    assert crs is None

    # path
    geom, crs = guess_geometry(aoi_br_geojson)
    assert geom.is_valid
    assert crs

    # Errors
    # malformed WKT
    with pytest.raises(WKTReadingError):
        guess_geometry(area.wkt.rstrip(")"))
    # non-existent path
    with pytest.raises(FileNotFoundError):
        guess_geometry("/invalid_path.geojson")
    # malformed GeoJSON mapping
    with pytest.raises(AttributeError):
        guess_geometry(dict(mapping(area), type=None))
    # unknown type
    with pytest.raises(TypeError):
        guess_geometry(1)
    # wrong geometry type
    with pytest.raises(TypeError):
        guess_geometry(area.centroid)


def test_bounds_from_opts(example_mapchete, wkt_geom):
    # WKT
    assert isinstance(bounds_from_opts(wkt_geometry=wkt_geom), Bounds)

    # point
    assert isinstance(
        bounds_from_opts(point=(0, 0), raw_conf=example_mapchete.dict), Bounds
    )

    # point from different CRS
    assert isinstance(
        bounds_from_opts(
            point=(0, 0), point_crs="EPSG:3857", raw_conf=example_mapchete.dict
        ),
        Bounds,
    )

    # bounds
    assert isinstance(
        bounds_from_opts(bounds=(1, 2, 3, 4), raw_conf=example_mapchete.dict), Bounds
    )

    # bounds from different CRS
    assert isinstance(
        bounds_from_opts(
            bounds=(1, 2, 3, 4), bounds_crs="EPSG:3857", raw_conf=example_mapchete.dict
        ),
        Bounds,
    )
