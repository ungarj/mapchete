import pytest
from shapely.geometry import box, shape

import mapchete
from mapchete.enums import Concurrency
from mapchete.errors import MapcheteConfigError
from mapchete.io import fiona_open, rasterio_open

# TODO: these down here should be moved elsewhere


def test_area_and_bounds(cleantopo_br_tiledir, sample_geojson):
    outside_bounds = (-1.7578125, 54.931640625, -1.73583984375, 54.95361328125)
    with mapchete.open(
        dict(cleantopo_br_tiledir.dict, area=sample_geojson), bounds=outside_bounds
    ) as mp:
        assert len(list(mp.get_process_tiles())) == 0


def test_aoi(aoi_br, aoi_br_geojson, cleantopo_br_tif):
    zoom = 7

    # read geojson geometry
    with fiona_open(aoi_br_geojson) as src:
        area = shape(next(iter(src))["geometry"])
    # read input tiff bounds
    with rasterio_open(cleantopo_br_tif) as src:
        raster = box(*src.bounds)
    aoi = area.intersection(raster)

    # area as path in mapchete config
    with mapchete.open(aoi_br.dict) as mp:
        aoi_tiles = list(mp.config.process_pyramid.tiles_from_geom(aoi, zoom))
        process_tiles = list(mp.get_process_tiles(zoom=zoom))
        assert len(aoi_tiles) == len(process_tiles)
        assert set(aoi_tiles) == set(process_tiles)

    # area as WKT in mapchete config
    with mapchete.open(
        dict(aoi_br.dict, area=area.wkt),
    ) as mp:
        process_tiles = list(mp.get_process_tiles(zoom=zoom))
        assert len(aoi_tiles) == len(process_tiles)
        assert set(aoi_tiles) == set(process_tiles)

    # area as path in mapchete.open
    with mapchete.open(dict(aoi_br.dict, area=None), area=aoi_br_geojson) as mp:
        process_tiles = list(mp.get_process_tiles(zoom=zoom))
        assert len(aoi_tiles) == len(process_tiles)
        assert set(aoi_tiles) == set(process_tiles)

    # errors
    # non-existent path
    with pytest.raises(MapcheteConfigError):
        mapchete.open(dict(aoi_br.dict, area=None), area="/invalid_path.geojson")


def test_custom_process(example_custom_process_mapchete):
    with mapchete.open(example_custom_process_mapchete.dict) as mp:
        tile = example_custom_process_mapchete.first_process_tile()
        assert mp.execute_tile(tile) is not None


def test_typed_raster_input(typed_raster_input):
    with mapchete.open(typed_raster_input.path) as mp:
        list(mp.execute(concurrency=Concurrency.none))
