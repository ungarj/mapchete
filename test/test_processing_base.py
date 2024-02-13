"""Test Mapchete main module and processing."""

import json
import os
import shutil
from itertools import chain

import numpy as np
import numpy.ma as ma
import pkg_resources
import pytest
from rasterio import windows

try:
    from cPickle import dumps as pickle_dumps
except ImportError:
    from pickle import dumps as pickle_dumps

from shapely.geometry import box, shape
from shapely.ops import unary_union

import mapchete
from mapchete.config import DaskSettings
from mapchete.errors import MapcheteProcessOutputError
from mapchete.io import fs_from_path, rasterio_open
from mapchete.io.raster.mosaic import _shift_required, create_mosaic
from mapchete.processing.types import TaskInfo
from mapchete.tile import BufferedTilePyramid, count_tiles


def test_empty_execute(cleantopo_br):
    """Execute process outside of defined zoom levels."""
    with mapchete.open(cleantopo_br.dict) as mp:
        assert mp.execute_tile((6, 0, 0)).mask.all()


def test_read_existing_output(cleantopo_tl):
    """Read existing process output."""
    # raster data
    with mapchete.open(cleantopo_tl.dict) as mp:
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


def test_read_existing_output_buffer(cleantopo_tl):
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


def test_read_existing_output_vector(geojson):
    """Read existing process output with process buffer."""
    with mapchete.open(geojson.dict) as mp:
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
    with mapchete.open(cleantopo_tl.dict) as mp:
        tile = mp.config.process_pyramid.tile(5, 0, 0)
        # read written data from within MapcheteProcess object
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            config=mp.config,
        )
        with pytest.raises(ValueError):
            user_process.open("invaild_input_id")


def test_get_raw_output_outside(cleantopo_br):
    """Get raw process output outside of zoom levels."""
    with mapchete.open(cleantopo_br.dict) as mp:
        assert mp.get_raw_output((6, 0, 0)).mask.all()


def test_get_raw_output_memory(cleantopo_tl):
    """Get raw process output using memory flag."""
    with mapchete.open(cleantopo_tl.dict, mode="memory") as mp:
        assert mp.config.mode == "memory"
        assert not mp.get_raw_output((5, 0, 0)).mask.all()


def test_get_raw_output_readonly(cleantopo_tl):
    """Get raw process output using readonly flag."""
    tile = (5, 0, 0)
    readonly_mp = mapchete.open(cleantopo_tl.dict, mode="readonly")
    write_mp = mapchete.open(cleantopo_tl.dict, mode="continue")

    # read non-existing data (returns empty)
    assert readonly_mp.get_raw_output(tile).mask.all()

    # try to process and save empty data
    with pytest.raises(ValueError):
        readonly_mp.write(tile, readonly_mp.get_raw_output(tile))

    # actually process and save
    write_mp.write(tile, write_mp.get_raw_output(tile))

    # read written output
    assert not readonly_mp.get_raw_output(tile).mask.all()


def test_get_raw_output_continue_raster(cleantopo_tl):
    """Get raw process output using continue flag."""
    with mapchete.open(cleantopo_tl.dict) as mp:
        assert mp.config.mode == "continue"
        tile = (5, 0, 0)
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data
        assert not mp.get_raw_output(tile).mask.all()


def test_get_raw_output_continue_vector(geojson):
    """Get raw process output using continue flag."""
    with mapchete.open(geojson.dict) as mp:
        assert mp.config.mode == "continue"
        tile = next(mp.get_process_tiles(4))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data
        assert mp.get_raw_output(tile)


def test_baselevels(baselevels):
    """Baselevel interpolation."""
    with mapchete.open(baselevels.dict, mode="continue") as mp:
        # process data before getting baselevels
        list(mp.execute(concurrency=None))

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


def test_baselevels_dask(baselevels, dask_executor):
    """Baselevel interpolation."""
    with mapchete.open(baselevels.dict, mode="continue") as mp:
        # process data before getting baselevels
        list(mp.execute(executor=dask_executor))

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


def test_baselevels_custom_nodata(baselevels_custom_nodata):
    """Baselevel interpolation."""
    fill_value = -32768.0
    with mapchete.open(baselevels_custom_nodata.dict, mode="continue") as mp:
        # process data before getting baselevels
        list(mp.execute())

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


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        ("threads", None),
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_update_overviews(overviews, concurrency, process_graph, dask_executor):
    """Baselevel interpolation."""
    if concurrency == "dask":
        execute_kwargs = dict(
            executor=dask_executor,
            dask_settings=DaskSettings(process_graph=process_graph),
        )
    else:
        execute_kwargs = dict(concurrency=concurrency)
    # process everything and make sure output was written
    with overviews.mp() as mp:
        baselevel_tile = overviews.first_process_tile()
        overview_tile = baselevel_tile.get_parent()
        baselevel_tile_path = mp.config.output.get_path(
            mp.config.output_pyramid.tile(*baselevel_tile)
        )
        overview_tile_path = mp.config.output.get_path(
            mp.config.output_pyramid.tile(*overview_tile)
        )

        # process baselevel (zoom 7)
        list(mp.execute(**execute_kwargs, zoom=7))

    # make sure baselevel tile has content
    with rasterio_open(baselevel_tile_path) as src:
        assert src.read().any()

    # remove baselevel_tile
    baselevel_tile_path.rm()
    assert not baselevel_tile_path.exists()

    with overviews.mp() as mp:
        # process overviews
        list(mp.execute(**execute_kwargs, zoom=[0, 6]))
    assert not baselevel_tile_path.exists()

    # read overview tile which is half empty
    with rasterio_open(overview_tile_path) as src:
        overview_before = src.read()
        assert overview_before.any()

    # run again in continue mode. this processes the missing tile on baselevel but overwrites
    # the overview tiles
    with overviews.mp() as mp:
        # process data before getting baselevels
        list(mp.execute(concurrency=concurrency))

    assert baselevel_tile_path.exists()
    with rasterio_open(
        mp.config.output.get_path(mp.config.output_pyramid.tile(*overview_tile))
    ) as src:
        overview_after = src.read()
        assert overview_after.any()

    # now make sure the overview tile was updated
    assert not np.array_equal(overview_before, overview_after)


def test_larger_process_tiles_than_output_tiles(cleantopo_br):
    config = cleantopo_br.dict.copy()
    config["output"].update(metatiling=1)
    config["pyramid"].update(metatiling=2)
    zoom = 5
    with mapchete.open(config) as mp:
        # there are tasks to be done
        assert mp.tasks(zoom=zoom)

        # execute tasks
        list(mp.execute(zoom=zoom))

        # there are no tasks
        assert not mp.tasks(zoom=zoom)

        # remove one output tile
        (cleantopo_br.output_path / 5).ls()[0].ls()[0].rm()

        # now there are tasks to be done again
        assert mp.tasks(zoom=zoom)


def test_baselevels_buffer(baselevels):
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


def test_baselevels_output_buffer(baselevels_output_buffer):
    # it should not contain masked values within bounds
    # (171.46155, -87.27184, 174.45159, -84.31281)
    with mapchete.open(baselevels_output_buffer.dict) as mp:
        # process all
        list(mp.execute())
        # read tile 6/62/125.tif
        with rasterio_open(
            mp.config.output.output_params["path"] / 6 / 62 / 125 + ".tif"
        ) as src:
            window = windows.from_bounds(
                171.46155, -87.27184, 174.45159, -84.31281, transform=src.transform
            )
            subset = src.read(window=window, masked=True)
            assert not subset.mask.any()


def test_baselevels_buffer_antimeridian(baselevels):
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

        # use baselevel generation to interpolate tile and
        # assert data from across the antimeridian is read.
        lower_tile = mp.get_raw_output(west.get_parent())
        assert np.where(lower_tile != 10, True, False).all()


def test_processing(mp_tmpdir, cleantopo_br, cleantopo_tl):
    """Test correct processing (read and write) outputs."""
    for cleantopo_process in [cleantopo_br.dict, cleantopo_tl.dict]:
        with mapchete.open(cleantopo_process) as mp:
            for zoom in range(6):
                tiles = []
                for tile in mp.get_process_tiles(zoom):
                    output = mp.execute_tile(tile)
                    tiles.append((tile, output))
                    assert isinstance(output, ma.MaskedArray)
                    assert output.shape == output.shape
                    assert not ma.all(output.mask)
                    mp.write(tile, output)
                mosaic = create_mosaic(tiles)
                try:
                    temp_vrt = mp_tmpdir / zoom + ".vrt"
                    temp_vrt.parent.makedirs()
                    gdalbuildvrt = "gdalbuildvrt %s %s/%s/*/*.tif > /dev/null" % (
                        temp_vrt,
                        mp.config.output.path,
                        zoom,
                    )
                    os.system(gdalbuildvrt)
                    with rasterio_open(temp_vrt, "r") as testfile:
                        for file_item, mosaic_item in zip(
                            testfile.meta["transform"], mosaic.affine
                        ):
                            assert file_item == mosaic_item
                        band = testfile.read(masked=True)
                        assert band.shape == mosaic.data.shape
                        assert ma.allclose(band, mosaic.data)
                        assert ma.allclose(band.mask, mosaic.data.mask)
                finally:
                    shutil.rmtree(mp_tmpdir, ignore_errors=True)


def test_pickleability(cleantopo_tl):
    """Test parallel tile processing."""
    with mapchete.open(cleantopo_tl.dict) as mp:
        assert pickle_dumps(mp)
        assert pickle_dumps(mp.config)
        assert pickle_dumps(mp.config.output)
        for tile in mp.get_process_tiles():
            assert pickle_dumps(tile)


def _worker(mp, tile):
    """Multiprocessing worker processing a tile."""
    return tile, mp.execute_tile(tile)


def test_write_empty(cleantopo_tl):
    """Test write function when passing an empty process_tile."""
    with mapchete.open(cleantopo_tl.dict) as mp:
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
        mp.execute_tile(process_tile)


@pytest.mark.parametrize("minzoom", range(0, 14))
def test_count_tiles(zoom_mapchete, minzoom):
    """Count tiles function."""
    maxzoom = 13
    conf = zoom_mapchete.dict
    conf.update(
        zoom_levels=dict(max=maxzoom),
        bounds=[14.0625, 47.8125, 16.875, 50.625],
        input=None,
    )
    conf["pyramid"].update(metatiling=8, pixelbuffer=5)
    conf["zoom_levels"].update(min=minzoom)
    with mapchete.open(conf) as mp:
        assert len(list(mp.get_process_tiles())) == mapchete.count_tiles(
            mp.config.area_at_zoom(), mp.config.process_pyramid, minzoom, maxzoom
        )


@pytest.mark.parametrize("metatiling", [1, 2, 4, 8, 16])
@pytest.mark.parametrize("zoom", range(15))
def test_count_tiles_mercator(metatiling, zoom):
    tp = BufferedTilePyramid("mercator", metatiling=metatiling)
    count_by_geom = count_tiles(box(*tp.bounds), tp, zoom, zoom)
    count_by_tp = tp.matrix_width(zoom) * tp.matrix_height(zoom)
    assert count_by_geom == count_by_tp


# This test only works until zoom 14. After this the results between the count_tiles()
# algorithms (rasterized & tile-based) start to differ from the actual TilePyramid.tiles_from_geom()
# implementation. Please also note that TilePyramid.tiles_from_geom(exact=True) ast to be activated
# in order to pass
@pytest.mark.parametrize("zoom", range(14))
def test_count_tiles_large_init_zoom(geometrycollection, zoom):
    tp = BufferedTilePyramid(grid="geodetic")
    raster_count = count_tiles(
        geometrycollection, tp, zoom, zoom, rasterize_threshold=0
    )
    tile_count = count_tiles(
        geometrycollection, tp, zoom, zoom, rasterize_threshold=100_000_000
    )
    tiles = len(
        list(tp.tiles_from_geom(geometry=geometrycollection, zoom=zoom, exact=True))
    )
    assert raster_count == tile_count == tiles


def test_skip_tiles(cleantopo_tl):
    """Test batch_process function."""
    zoom = 2
    with mapchete.open(cleantopo_tl.dict, mode="continue") as mp:
        list(mp.execute(zoom=zoom))
        for tile, skip in mp.skip_tiles(tiles=mp.get_process_tiles(zoom=zoom)):
            assert skip

    with mapchete.open(cleantopo_tl.dict, mode="overwrite") as mp:
        for tile, skip in mp.skip_tiles(tiles=mp.get_process_tiles(zoom=zoom)):
            assert not skip


def test_custom_grid(custom_grid):
    """Cutom grid processing."""
    # process and save
    with mapchete.open(custom_grid.dict) as mp:
        list(mp.execute())
    # read written output
    with mapchete.open(custom_grid.dict) as mp:
        for tile in mp.get_process_tiles(5):
            data = mp.config.output.read(tile)
            assert data.any()
            assert isinstance(data, ma.masked_array)
            assert not data.mask.all()


def test_execute_kwargs(example_mapchete, execute_kwargs_py):
    config = example_mapchete.dict
    config.update(process=execute_kwargs_py)
    with mapchete.open(config) as mp:
        mp.execute_tile((7, 61, 129))


@pytest.mark.parametrize("pixelbuffer", [0, 5, 10])
@pytest.mark.parametrize("metatiling", [1, 2, 4])
@pytest.mark.parametrize("zoom", range(3, 5))
def test_snap_bounds_to_zoom(pixelbuffer, metatiling, zoom):
    bounds = (-180, -90, -60, -30)
    pyramid = BufferedTilePyramid(
        "geodetic", pixelbuffer=pixelbuffer, metatiling=metatiling
    )
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


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        ("threads", None),
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_execute(preprocess_cache_memory, concurrency, process_graph, dask_executor):
    if concurrency == "dask":
        execute_kwargs = dict(
            executor=dask_executor,
            dask_settings=DaskSettings(process_graph=process_graph),
        )
    else:
        execute_kwargs = dict(concurrency=concurrency)

    with preprocess_cache_memory.mp(batch_preprocess=False) as mp:
        preprocessing_tasks = 0
        tile_tasks = 0
        for task_info in mp.execute(**execute_kwargs):
            assert isinstance(task_info, TaskInfo)
            if task_info.tile is None:
                preprocessing_tasks += 1
            else:
                tile_tasks += 1
                assert task_info.output is None
    assert tile_tasks == 20
    assert preprocessing_tasks == 2


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        ("threads", None),
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_execute_continue(
    red_raster, green_raster, dask_executor, concurrency, process_graph
):
    if concurrency == "dask":
        execute_kwargs = dict(
            executor=dask_executor,
            dask_settings=DaskSettings(process_graph=process_graph),
        )
    else:
        execute_kwargs = dict(concurrency=concurrency)

    zoom = 3

    # run red_raster on tile 1, 0, 0
    with red_raster.mp() as mp_red:
        list(mp_red.execute(tile=(zoom, 0, 0), **execute_kwargs))
    fs_red = fs_from_path(mp_red.config.output.path)
    assert len(fs_red.glob(f"{mp_red.config.output.path}/*/*/*.tif")) == 1
    with rasterio_open(f"{mp_red.config.output.path}/{zoom}/0/0.tif") as src:
        assert np.array_equal(
            src.read(),
            np.stack([np.full((256, 256), c, dtype=np.uint8) for c in (255, 1, 1)]),
        )

    # copy red_raster output to green_raster output
    with green_raster.mp() as mp_green:
        fs_green = fs_from_path(mp_green.config.output.path)
        fs_green.mkdir(mp_green.config.output.path / f"{zoom}/0", create_parents=True)
        fs_green.copy(
            str(mp_red.config.output.path / f"{zoom}/0/0.tif"),
            str(mp_green.config.output.path / f"{zoom}/0/0.tif"),
        )
        # run green_raster on zoom 1
        list(mp_green.execute(zoom=[0, zoom], **execute_kwargs))

    # assert red tile is still there and other tiles were written and are green
    assert len(
        fs_green.glob(f"{mp_green.config.output.path}/*/*/*.tif")
    ) == mp_green.count_tiles(minzoom=0, maxzoom=zoom)
    tp = mp_green.config.process_pyramid
    red_tile = tp.tile(zoom, 0, 0)
    overview_tiles = [
        tp.tile_from_xy(red_tile.bbox.centroid.x, red_tile.bbox.centroid.y, z)
        for z in range(0, zoom)
    ]
    for path in fs_green.glob(f"{mp_green.config.output.path}/*/*/*.tif"):
        zoom, row, col = [int(p.rstrip(".tif")) for p in path.split("/")[-3:]]
        tile = tp.tile(zoom, row, col)
        # make sure red tile still is red
        if tile == red_tile:
            with rasterio_open(path) as src:
                assert np.array_equal(
                    src.read(),
                    np.stack(
                        [np.full((256, 256), c, dtype=np.uint8) for c in (255, 1, 1)]
                    ),
                )

        # make sure overview tiles from red tile contain both red and green values
        elif tile in overview_tiles:
            with rasterio_open(path) as src:
                for band in src.read([1, 2], masked=True):
                    assert 1 in band
                    assert 255 in band

        # make sure all other tiles are green
        else:
            with rasterio_open(path) as src:
                assert np.array_equal(
                    src.read(),
                    np.stack(
                        [np.full((256, 256), c, dtype=np.uint8) for c in (1, 255, 1)]
                    ),
                )


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        ("threads", None),
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_execute_without_results(baselevels, dask_executor, concurrency, process_graph):
    if concurrency == "dask":
        execute_kwargs = dict(
            executor=dask_executor,
            dask_settings=DaskSettings(process_graph=process_graph),
        )
    else:
        execute_kwargs = dict(concurrency=concurrency)

    # make sure task results are appended to tasks
    with baselevels.mp() as mp:
        tile_tasks = 0
        for task_info in mp.execute(**execute_kwargs, propagate_results=True):
            assert task_info.output is not None
            tile_tasks += 1
    assert tile_tasks == 6

    # make sure task results are None
    with baselevels.mp() as mp:
        tile_tasks = 0
        for task_info in mp.execute(**execute_kwargs, propagate_results=False):
            assert task_info.output is None
            tile_tasks += 1
    assert tile_tasks == 6


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        ("threads", None),
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_execute_single_file(
    preprocess_cache_memory_single_file, concurrency, process_graph, dask_executor
):
    """Baselevel interpolation."""
    if concurrency == "dask":
        execute_kwargs = dict(
            executor=dask_executor,
            dask_settings=DaskSettings(process_graph=process_graph),
        )
    else:
        execute_kwargs = dict(concurrency=concurrency)

    with preprocess_cache_memory_single_file.mp(batch_preprocess=False) as mp:
        preprocessing_tasks = 0
        tile_tasks = 0
        for task_info in mp.execute(**execute_kwargs):
            assert isinstance(task_info, TaskInfo)
            if task_info.tile is None:
                assert task_info.output is not None
                preprocessing_tasks += 1
            else:
                tile_tasks += 1
                assert task_info.output is None
    assert tile_tasks == 12
    assert preprocessing_tasks == 2
    with rasterio_open(mp.config.output.path) as src:
        assert not src.read(masked=True).mask.all()


def test_write_stac(stac_metadata):
    mp = stac_metadata.mp()
    out_path = stac_metadata.mp().config.output.stac_path
    with pytest.raises(FileNotFoundError):
        fs_from_path(out_path).open(out_path, "r")

    mp.write_stac()

    with fs_from_path(out_path).open(out_path, "r") as src:
        item = json.loads(src.read())

    assert item


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        # ("threads", None),  --> request count does not work on threads
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_execute_request_count(
    preprocess_cache_memory, concurrency, process_graph, dask_executor
):
    if concurrency == "dask":
        execute_kwargs = dict(
            executor=dask_executor,
            dask_settings=DaskSettings(process_graph=process_graph),
        )
    else:
        execute_kwargs = dict(concurrency=concurrency)
    with preprocess_cache_memory.mp(batch_preprocess=False) as mp:
        preprocessing_tasks = 0
        tile_tasks = 0
        for task_info in mp.execute(**execute_kwargs):
            if task_info.tile is None:
                preprocessing_tasks += 1
            else:
                tile_tasks += 1
                assert task_info.output is None
    assert tile_tasks == 20
    assert preprocessing_tasks == 2


def test_execute_no_tasks(preprocess_cache_memory):
    """If a process is initialized out of bounds, there should be no tasks."""
    some_bounds_outside_process = (-180, 80, -170, 90)
    with preprocess_cache_memory.mp(
        batch_preprocess=False, bounds=some_bounds_outside_process, zoom=5
    ) as mp:
        process_tiles = [tile for tile in mp.get_process_tiles(zoom=5)]
        tasks = mp.tasks()
        assert tasks.tile_tasks_count == len(process_tiles) == 0
        assert tasks.preprocessing_tasks_count == 0
        assert len(tasks) == 0
