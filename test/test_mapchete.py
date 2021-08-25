"""Test Mapchete main module and processing."""

from itertools import chain
import pytest
import os
import shutil
import rasterio
from rasterio import windows
import numpy as np
import numpy.ma as ma
import pkg_resources

try:
    from cPickle import dumps as pickle_dumps
except ImportError:
    from pickle import dumps as pickle_dumps
from shapely.geometry import box, shape
from shapely.ops import unary_union

import mapchete
from mapchete.io.raster import create_mosaic, _shift_required
from mapchete.errors import MapcheteProcessOutputError
from mapchete.tile import BufferedTilePyramid, count_tiles


def test_empty_execute(mp_tmpdir, cleantopo_br):
    """Execute process outside of defined zoom levels."""
    with mapchete.open(cleantopo_br.path) as mp:
        assert mp.execute((6, 0, 0)).mask.all()


def test_read_existing_output(mp_tmpdir, cleantopo_tl):
    """Read existing process output."""
    # raster data
    with mapchete.open(cleantopo_tl.path) as mp:
        tile = mp.config.process_pyramid.tile(5, 0, 0)
        # process and save
        mp.get_raw_output(tile)
        data = mp.config.output.read(tile)
        assert data.any()
        assert isinstance(data, ma.masked_array)
        assert not data.mask.all()
        # read data from Mapchete class
        data = mp.config.output.read(tile)
        assert data.any()
        assert isinstance(data, ma.masked_array)
        assert not data.mask.all()


def test_read_existing_output_buffer(mp_tmpdir, cleantopo_tl):
    """Read existing process output with process buffer."""
    # raster data process buffer > output buffer
    config = cleantopo_tl.dict
    config["output"].update(pixelbuffer=0)
    with mapchete.open(config) as mp:
        tile = next(mp.get_process_tiles(5))
        # process and save
        mp.get_raw_output(tile)
        data = mp.config.output.read(tile)
        assert data.any()
        assert isinstance(data, ma.masked_array)
        assert not data.mask.all()


def test_read_existing_output_vector(mp_tmpdir, geojson):
    """Read existing process output with process buffer."""
    with mapchete.open(geojson.path) as mp:
        tile = next(mp.get_process_tiles(4))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        data = list(
            chain(
                *[
                    mp.config.output.read(t)
                    for t in mp.config.output.pyramid.intersecting(tile)
                ]
            )
        )
        assert data
        for feature in data:
            assert shape(feature["geometry"]).is_valid


def test_open_data_error(cleantopo_tl):
    """Try to open data not specified as input."""
    with mapchete.open(cleantopo_tl.path) as mp:
        tile = mp.config.process_pyramid.tile(5, 0, 0)
        # read written data from within MapcheteProcess object
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            config=mp.config,
        )
        with pytest.raises(ValueError):
            user_process.open("invaild_input_id")


def test_get_raw_output_outside(mp_tmpdir, cleantopo_br):
    """Get raw process output outside of zoom levels."""
    with mapchete.open(cleantopo_br.path) as mp:
        assert mp.get_raw_output((6, 0, 0)).mask.all()


def test_get_raw_output_memory(mp_tmpdir, cleantopo_tl):
    """Get raw process output using memory flag."""
    with mapchete.open(cleantopo_tl.path, mode="memory") as mp:
        assert mp.config.mode == "memory"
        assert not mp.get_raw_output((5, 0, 0)).mask.all()


def test_get_raw_output_readonly(mp_tmpdir, cleantopo_tl):
    """Get raw process output using readonly flag."""
    tile = (5, 0, 0)
    readonly_mp = mapchete.open(cleantopo_tl.path, mode="readonly")
    write_mp = mapchete.open(cleantopo_tl.path, mode="continue")

    # read non-existing data (returns empty)
    assert readonly_mp.get_raw_output(tile).mask.all()

    # try to process and save empty data
    with pytest.raises(ValueError):
        readonly_mp.write(tile, readonly_mp.get_raw_output(tile))

    # actually process and save
    write_mp.write(tile, write_mp.get_raw_output(tile))

    # read written output
    assert not readonly_mp.get_raw_output(tile).mask.all()


def test_get_raw_output_continue_raster(mp_tmpdir, cleantopo_tl):
    """Get raw process output using continue flag."""
    with mapchete.open(cleantopo_tl.path) as mp:
        assert mp.config.mode == "continue"
        tile = (5, 0, 0)
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data
        assert not mp.get_raw_output(tile).mask.all()


def test_get_raw_output_continue_vector(mp_tmpdir, geojson):
    """Get raw process output using continue flag."""
    with mapchete.open(geojson.path) as mp:
        assert mp.config.mode == "continue"
        tile = next(mp.get_process_tiles(4))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data
        assert mp.get_raw_output(tile)


# def test_get_raw_output_reproject(mp_tmpdir, cleantopo_tl):
#     """Get process output from a different CRS."""
#     with pytest.raises(NotImplementedError):
#         with mapchete.open(cleantopo_tl.path) as mp:
#             assert mp.config.mode == "continue"
#             # TODO implement function
#             print(mp.get_raw_output((5, 0, 0)))


def test_baselevels(mp_tmpdir, baselevels):
    """Baselevel interpolation."""
    with mapchete.open(baselevels.path, mode="continue") as mp:
        # process data before getting baselevels
        mp.batch_process()

        # get tile from lower zoom level
        for tile in mp.get_process_tiles(4):
            data = mp.get_raw_output(tile)
            assert not data.mask.all()
            # write for next zoom level
            mp.write(tile, data)
            assert not mp.get_raw_output(tile.get_parent()).mask.all()

        # get tile from higher zoom level
        tile = next(mp.get_process_tiles(6))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert any(
            [
                not mp.get_raw_output(upper_tile).mask.all()
                for upper_tile in tile.get_children()
            ]
        )


def test_baselevels_custom_nodata(mp_tmpdir, baselevels_custom_nodata):
    """Baselevel interpolation."""
    fill_value = -32768.0
    with mapchete.open(baselevels_custom_nodata.path, mode="continue") as mp:
        # process data before getting baselevels
        mp.batch_process()

        # get tile from lower zoom level
        for tile in mp.get_process_tiles(4):
            lower_tile = mp.get_raw_output(tile)
            assert not lower_tile.mask.all()
            # assert fill_value is set and all data are not 0
            assert lower_tile.fill_value == fill_value
            assert lower_tile.data.all()
            # write for next zoom level
            mp.write(tile, lower_tile)
            parent_tile = mp.get_raw_output(tile.get_parent())
            assert not parent_tile.mask.all()
            # assert fill_value is set and all data are not 0
            assert parent_tile.fill_value == fill_value
            assert parent_tile.data.all()

        # get tile from higher zoom level
        tile = next(mp.get_process_tiles(6))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert any(
            [
                not mp.get_raw_output(upper_tile).mask.all()
                for upper_tile in tile.get_children()
            ]
        )
        # assert fill_value is set and all data are not 0
        assert all(
            [
                mp.get_raw_output(upper_tile).fill_value == fill_value
                for upper_tile in tile.get_children()
            ]
        )
        assert all(
            [
                mp.get_raw_output(upper_tile).data.all()
                for upper_tile in tile.get_children()
            ]
        )


def test_update_baselevels(mp_tmpdir, baselevels):
    """Baselevel interpolation."""
    conf = dict(baselevels.dict)
    conf.update(zoom_levels=[7, 8], baselevels=dict(min=8, max=8))
    baselevel_tile = (8, 125, 260)
    overview_tile = (7, 62, 130)
    with mapchete.open(conf, mode="continue") as mp:
        tile_bounds = mp.config.output_pyramid.tile(*baselevel_tile).bounds

    # process using bounds of just one baselevel tile
    with mapchete.open(conf, mode="continue", bounds=tile_bounds) as mp:
        mp.batch_process()
        with rasterio.open(
            mp.config.output.get_path(mp.config.output_pyramid.tile(*overview_tile))
        ) as src:
            overview_before = src.read()
            assert overview_before.any()

    # process full area which leaves out overview tile for baselevel tile above
    with mapchete.open(conf, mode="continue") as mp:
        mp.batch_process()

    # delete baselevel tile
    written_tile = (
        os.path.join(
            *[
                baselevels.dict["config_dir"],
                baselevels.dict["output"]["path"],
                *map(str, baselevel_tile),
            ]
        )
        + ".tif"
    )
    os.remove(written_tile)
    assert not os.path.exists(written_tile)

    # run again in continue mode. this processes the missing tile on zoom 5 but overwrites
    # the tile in zoom 4
    with mapchete.open(conf, mode="continue") as mp:
        # process data before getting baselevels
        mp.batch_process()
        with rasterio.open(
            mp.config.output.get_path(mp.config.output_pyramid.tile(*overview_tile))
        ) as src:
            overview_after = src.read()
            assert overview_after.any()

    assert not np.array_equal(overview_before, overview_after)


def test_baselevels_buffer(mp_tmpdir, baselevels):
    """Baselevel interpolation using buffers."""
    config = baselevels.dict
    config["pyramid"].update(pixelbuffer=10)
    with mapchete.open(config, mode="continue") as mp:
        # get tile from lower zoom level
        lower_tile = next(mp.get_process_tiles(4))
        # process and save
        for tile in lower_tile.get_children():
            mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert not mp.get_raw_output(lower_tile).mask.all()

        # get tile from higher zoom level
        tile = next(mp.get_process_tiles(6))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert any(
            [
                not mp.get_raw_output(upper_tile).mask.all()
                for upper_tile in tile.get_children()
            ]
        )


def test_baselevels_output_buffer(mp_tmpdir, baselevels_output_buffer):
    # it should not contain masked values within bounds
    # (171.46155, -87.27184, 174.45159, -84.31281)
    with mapchete.open(baselevels_output_buffer.dict) as mp:
        # process all
        mp.batch_process()
        # read tile 6/62/125.tif
        with rasterio.open(
            os.path.join(mp.config.output.output_params["path"], "6/62/125.tif")
        ) as src:
            window = windows.from_bounds(
                171.46155, -87.27184, 174.45159, -84.31281, transform=src.transform
            )
            subset = src.read(window=window, masked=True)
            print(subset.shape)
            assert not subset.mask.any()
            pass


def test_baselevels_buffer_antimeridian(mp_tmpdir, baselevels):
    """Baselevel interpolation using buffers."""
    config = baselevels.dict
    config.update(input=None)
    config["pyramid"].update(pixelbuffer=10)
    zoom = 5
    row = 0
    with mapchete.open(config) as mp:
        # write data left and right of antimeridian
        west = mp.config.process_pyramid.tile(zoom, row, 0)
        shape = (3,) + west.shape
        mp.write(west, np.ones(shape) * 0)
        east = mp.config.process_pyramid.tile(
            zoom, row, mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        mp.write(east, np.ones(shape) * 10)
        # use baselevel generation to interpolate tile and somehow
        # assert no data from across the antimeridian is read.
        lower_tile = mp.get_raw_output(west.get_parent())
        assert np.where(lower_tile != 10, True, False).all()


def test_processing(mp_tmpdir, cleantopo_br, cleantopo_tl):
    """Test correct processing (read and write) outputs."""
    for cleantopo_process in [cleantopo_br.path, cleantopo_tl.path]:
        with mapchete.open(cleantopo_process) as mp:
            for zoom in range(6):
                tiles = []
                for tile in mp.get_process_tiles(zoom):
                    output = mp.execute(tile)
                    tiles.append((tile, output))
                    assert isinstance(output, ma.MaskedArray)
                    assert output.shape == output.shape
                    assert not ma.all(output.mask)
                    mp.write(tile, output)
                mosaic = create_mosaic(tiles)
                try:
                    temp_vrt = os.path.join(mp_tmpdir, str(zoom) + ".vrt")
                    gdalbuildvrt = "gdalbuildvrt %s %s/%s/*/*.tif > /dev/null" % (
                        temp_vrt,
                        mp.config.output.path,
                        zoom,
                    )
                    os.system(gdalbuildvrt)
                    with rasterio.open(temp_vrt, "r") as testfile:
                        for file_item, mosaic_item in zip(
                            testfile.meta["transform"], mosaic.affine
                        ):
                            assert file_item == mosaic_item
                        band = testfile.read(1, masked=True)
                        assert band.shape == mosaic.data.shape
                        assert ma.allclose(band, mosaic.data)
                        assert ma.allclose(band.mask, mosaic.data.mask)
                finally:
                    shutil.rmtree(mp_tmpdir, ignore_errors=True)


def test_pickleability(mp_tmpdir, cleantopo_tl):
    """Test parallel tile processing."""
    with mapchete.open(cleantopo_tl.path) as mp:
        assert pickle_dumps(mp)
        assert pickle_dumps(mp.config)
        assert pickle_dumps(mp.config.output)
        for tile in mp.get_process_tiles():
            assert pickle_dumps(tile)


def _worker(mp, tile):
    """Multiprocessing worker processing a tile."""
    return tile, mp.execute(tile)


def test_write_empty(mp_tmpdir, cleantopo_tl):
    """Test write function when passing an empty process_tile."""
    with mapchete.open(cleantopo_tl.path) as mp:
        # process and save
        mp.write(mp.config.process_pyramid.tile(5, 0, 0), None)


def test_process_template(dummy1_tif, mp_tmpdir):
    """Template used to create an empty process."""
    process_template = pkg_resources.resource_filename(
        "mapchete.static", "process_template.py"
    )
    mp = mapchete.open(
        dict(
            process=process_template,
            pyramid=dict(grid="geodetic"),
            input=dict(file1=dummy1_tif),
            output=dict(format="GTiff", path=mp_tmpdir, bands=1, dtype="uint8"),
            config_dir=mp_tmpdir,
            zoom_levels=4,
        )
    )
    process_tile = next(mp.get_process_tiles(zoom=4))
    # Mapchete throws a RuntimeError if process output is empty
    with pytest.raises(MapcheteProcessOutputError):
        mp.execute(process_tile)


def test_count_tiles(zoom_mapchete):
    """Count tiles function."""
    maxzoom = 13
    conf = zoom_mapchete.dict
    conf.update(
        zoom_levels=dict(max=maxzoom),
        bounds=[14.0625, 47.8125, 16.875, 50.625],
        input=None,
    )
    conf["pyramid"].update(metatiling=8, pixelbuffer=5)
    for minzoom in range(0, 14):
        conf["zoom_levels"].update(min=minzoom)
        with mapchete.open(conf) as mp:
            assert len(list(mp.get_process_tiles())) == mapchete.count_tiles(
                mp.config.area_at_zoom(), mp.config.process_pyramid, minzoom, maxzoom
            )


def test_count_tiles_mercator():
    for metatiling in [1, 2, 4, 8, 16]:
        tp = BufferedTilePyramid("mercator", metatiling=metatiling)
        for zoom in range(13):
            count_by_geom = count_tiles(box(*tp.bounds), tp, zoom, zoom)
            count_by_tp = tp.matrix_width(zoom) * tp.matrix_height(zoom)
            assert count_by_geom == count_by_tp


def test_batch_process(mp_tmpdir, cleantopo_tl):
    """Test batch_process function."""
    with mapchete.open(cleantopo_tl.path) as mp:
        # invalid parameters errors
        with pytest.raises(ValueError):
            mp.batch_process(zoom=1, tile=(1, 0, 0))
        # process single tile
        mp.batch_process(tile=(2, 0, 0))
        # process using multiprocessing
        mp.batch_process(zoom=2, multi=2)
        # process without multiprocessing
        mp.batch_process(zoom=2, multi=1)


def test_skip_tiles(mp_tmpdir, cleantopo_tl):
    """Test batch_process function."""
    zoom = 2
    with mapchete.open(cleantopo_tl.path, mode="continue") as mp:
        mp.batch_process(zoom=zoom)
        for tile, skip in mp.skip_tiles(tiles=mp.get_process_tiles(zoom=zoom)):
            assert skip

    with mapchete.open(cleantopo_tl.path, mode="overwrite") as mp:
        for tile, skip in mp.skip_tiles(tiles=mp.get_process_tiles(zoom=zoom)):
            assert not skip


def test_custom_grid(mp_tmpdir, custom_grid):
    """Cutom grid processing."""
    # process and save
    with mapchete.open(custom_grid.path) as mp:
        mp.batch_process()
    # read written output
    with mapchete.open(custom_grid.path) as mp:
        for tile in mp.get_process_tiles(5):
            data = mp.config.output.read(tile)
            assert data.any()
            assert isinstance(data, ma.masked_array)
            assert not data.mask.all()


def test_execute_kwargs(example_mapchete, execute_kwargs_py):
    config = example_mapchete.dict
    config.update(process=execute_kwargs_py)
    with mapchete.open(config) as mp:
        mp.execute((7, 61, 129))


def test_snap_bounds_to_zoom():
    bounds = (-180, -90, -60, -30)
    for pixelbuffer in [0, 5, 10]:
        for metatiling in [1, 2, 4]:
            pyramid = BufferedTilePyramid(
                "geodetic", pixelbuffer=pixelbuffer, metatiling=metatiling
            )
            for zoom in range(3, 5):
                snapped_bounds = mapchete.config.snap_bounds(
                    bounds=bounds, pyramid=pyramid, zoom=zoom
                )
                control_bounds = unary_union(
                    [t.bbox for t in pyramid.tiles_from_bounds(bounds, zoom)]
                ).bounds
                assert snapped_bounds == control_bounds


def test_snap_bounds_errors():
    bounds = (-180, -90, -60, -30)
    with pytest.raises(TypeError):
        mapchete.config.snap_bounds(bounds="invalid")
    with pytest.raises(ValueError):
        mapchete.config.snap_bounds(
            bounds=(
                0,
                1,
            )
        )
    with pytest.raises(TypeError):
        mapchete.config.snap_bounds(bounds=bounds, pyramid="invalid")


def test_execute_params(cleantopo_br, execute_params_error_py):
    """Assert execute() without parameters passes."""
    config = cleantopo_br.dict
    config.update(process=execute_params_error_py)
    mapchete.open(config)


def test_shift_required():
    zoom = 11
    row = 711
    tp = BufferedTilePyramid("mercator")
    tiles = [(tp.tile(zoom, row, i), None) for i in range(1, 5)]

    # all tiles are connected without passing the Antimeridian, so no shift is required
    assert not _shift_required(tiles)

    # add one tile connected on the other side of the Antimeridian and a shift is required
    tiles.append((tp.tile(zoom, row, tp.matrix_width(zoom) - 1), None))
    assert _shift_required(tiles)

    # leave one column and add one tile
    tiles = [(tp.tile(zoom, row, i), None) for i in range(2, 5)]
    tiles.append((tp.tile(zoom, row, 6), None))
    tiles.append((tp.tile(zoom, row, 8), None))
    tiles.append((tp.tile(zoom, row, 9), None))
    assert not _shift_required(tiles)


def test_bufferedtiles():
    tp = BufferedTilePyramid("geodetic")
    a = tp.tile(5, 5, 5)
    b = tp.tile(5, 5, 5)
    c = tp.tile(5, 5, 6)
    assert a == b
    assert a != c
    assert b != c
    assert a != "invalid type"
    assert len(set([a, b, c])) == 2

    tp_buffered = BufferedTilePyramid("geodetic", pixelbuffer=10)
    assert a != tp_buffered.tile(5, 5, 5)

    assert a.get_neighbors() != a.get_neighbors(connectedness=4)
