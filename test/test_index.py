import fiona
import numpy as np
import os
import pytest
import rasterio

import mapchete
from mapchete.index import zoom_index_gen


@pytest.mark.remote
def test_remote_indexes(gtiff_s3):

    zoom = 7
    gtiff_s3.dict.update(zoom_levels=zoom)

    def gen_indexes_and_check():
        # generate indexes
        list(
            zoom_index_gen(
                mp=mp,
                zoom=zoom,
                out_dir=mp.config.output.path,
                geojson=True,
                txt=True,
                vrt=True,
            )
        )

        # assert GeoJSON exists
        path = mp.config.output.path / zoom + ".geojson"
        with path.fio_env():
            with fiona.open(str(path)) as src:
                assert len(src) == 2

        # assert TXT exists
        txt_index = mp.config.output.path / zoom + ".txt"
        with txt_index.open() as src:
            assert len(list(src.readlines())) == 2

        # assert VRT exists
        path = mp.config.output.path / zoom + ".vrt"
        with path.rio_env():
            with rasterio.open(path) as src:
                assert src.read().any()

    with mapchete.open(gtiff_s3.dict) as mp:
        # write output data
        mp.batch_process(zoom=zoom)

        # generate indexes and check
        gen_indexes_and_check()

        # generate indexes again and assert nothing has changes
        gen_indexes_and_check()


def test_vrt(mp_tmpdir, cleantopo_br):
    zoom = 8
    with mapchete.open(
        dict(cleantopo_br.dict, zoom_levels=dict(min=0, max=zoom))
    ) as mp:
        # generate output
        mp.batch_process(zoom=zoom)

        # generate index
        list(
            zoom_index_gen(
                mp=mp,
                zoom=zoom,
                out_dir=mp.config.output.path,
                vrt=True,
            )
        )
        output_tiles = list(
            mp.config.output_pyramid.tiles_from_bounds(
                mp.config.bounds_at_zoom(zoom=zoom), zoom=zoom
            )
        )
        bounds = (
            min([t.left for t in output_tiles]),
            min([t.bottom for t in output_tiles]),
            max([t.right for t in output_tiles]),
            max([t.top for t in output_tiles]),
        )
        # bounds = mp.config.effective_bounds

    vrt_index = mp.config.output.path / zoom + ".vrt"

    with vrt_index.rio_env():
        with rasterio.open(vrt_index) as vrt:
            assert vrt.driver == "VRT"
            assert vrt.dtypes[0] == "uint16"
            assert vrt.meta["dtype"] == "uint16"
            assert vrt.count == 1
            assert vrt.nodata == 0
            assert vrt.bounds == bounds
            vrt_data = vrt.read()
            assert vrt_data.any()

    # generate a VRT using GDAL and compare
    temp_vrt = mp.config.output.path / zoom + "_gdal.vrt"
    gdalbuildvrt = f"gdalbuildvrt {str(temp_vrt)} {str(mp.config.output.path)}/{str(zoom)}/*/*.tif > /dev/null"
    os.system(gdalbuildvrt)
    with temp_vrt.rio_env():
        with rasterio.open(temp_vrt, "r") as gdal_vrt:
            assert gdal_vrt.dtypes[0] == "uint16"
            assert gdal_vrt.meta["dtype"] == "uint16"
            assert gdal_vrt.count == 1
            assert gdal_vrt.nodata == 0
            assert gdal_vrt.bounds == bounds
            gdal_vrt_data = gdal_vrt.read()
            assert np.array_equal(vrt_data, gdal_vrt_data)

    # make sure handling an existing VRT works
    with mapchete.open(
        dict(cleantopo_br.dict, zoom_levels=dict(min=0, max=zoom))
    ) as mp:
        # generate output
        mp.batch_process(zoom=zoom)

        # generate index
        list(
            zoom_index_gen(
                mp=mp,
                zoom=zoom,
                out_dir=mp.config.output.path,
                vrt=True,
            )
        )


def test_vrt_mercator(cleantopo_br_mercator):
    zoom = 8
    with mapchete.open(
        dict(cleantopo_br_mercator.dict, zoom_levels=dict(min=0, max=zoom))
    ) as mp:
        # generate output
        mp.batch_process(zoom=zoom)

        # generate index
        list(
            zoom_index_gen(
                mp=mp,
                zoom=zoom,
                out_dir=mp.config.output.path,
                vrt=True,
            )
        )
        output_tiles = list(
            mp.config.output_pyramid.tiles_from_bounds(
                mp.config.bounds_at_zoom(zoom=zoom), zoom=zoom
            )
        )
        bounds = (
            min([t.left for t in output_tiles]),
            min([t.bottom for t in output_tiles]),
            max([t.right for t in output_tiles]),
            max([t.top for t in output_tiles]),
        )
        # bounds = mp.config.effective_bounds

    vrt_index = mp.config.output.path / zoom + ".vrt"

    with rasterio.open(vrt_index) as vrt:
        assert vrt.driver == "VRT"
        assert vrt.dtypes[0] == "uint16"
        assert vrt.meta["dtype"] == "uint16"
        assert vrt.count == 1
        assert vrt.nodata == 0
        for vrt_b, b in zip(vrt.bounds, bounds):
            assert round(vrt_b, 6) == round(b, 6)
        vrt_data = vrt.read()
        assert vrt_data.any()

    # generate a VRT using GDAL and compare
    temp_vrt = mp.config.output.path / zoom + "_gdal.vrt"
    gdalbuildvrt = f"gdalbuildvrt {str(temp_vrt)} {str(mp.config.output.path)}/{str(zoom)}/*/*.tif > /dev/null"
    os.system(gdalbuildvrt)
    with temp_vrt.rio_env():
        with rasterio.open(temp_vrt, "r") as gdal_vrt:
            assert gdal_vrt.dtypes[0] == "uint16"
            assert gdal_vrt.meta["dtype"] == "uint16"
            assert gdal_vrt.count == 1
            assert gdal_vrt.nodata == 0
            for vrt_b, b in zip(vrt.bounds, bounds):
                assert round(vrt_b, 6) == round(b, 6)
            gdal_vrt_data = gdal_vrt.read()
            assert np.array_equal(vrt_data, gdal_vrt_data)
            assert gdal_vrt_data.any()

    # make sure handling an existing VRT works
    with mapchete.open(
        dict(cleantopo_br_mercator.dict, zoom_levels=dict(min=0, max=zoom))
    ) as mp:
        # generate output
        mp.batch_process(zoom=zoom)

        # generate index
        list(
            zoom_index_gen(
                mp=mp,
                zoom=zoom,
                out_dir=mp.config.output.path,
                vrt=True,
            )
        )
