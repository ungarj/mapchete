import os
import warnings

import geobuf
import pytest
from rio_cogeo.cogeo import cog_validate
from shapely.geometry import box, shape
from tilematrix import TilePyramid

import mapchete
from mapchete.commands import convert, cp, execute, index, rm
from mapchete.config import DaskSettings
from mapchete.enums import Status
from mapchete.errors import JobCancelledError
from mapchete.io import fiona_open, rasterio_open
from mapchete.processing.types import TaskInfo
from mapchete.protocols import ObserverProtocol
from mapchete.tile import BufferedTilePyramid

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


class TaskCounter(ObserverProtocol):
    tasks = 0

    def update(self, *args, progress=None, **kwargs):
        if progress:
            self.tasks = progress.current


def test_cp(mp_tmpdir, cleantopo_br, wkt_geom):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.dict, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        list(mp.execute(zoom=5))
    out_path = os.path.join(TESTDATA_DIR, cleantopo_br.dict["output"]["path"])
    # copy tiles and subset by bounds
    task_counter = TaskCounter()
    cp(
        out_path,
        os.path.join(mp_tmpdir, "bounds"),
        zoom=5,
        bounds=[169.19251592399996, -90, 180, -80.18582802550002],
        observers=[task_counter],
    )
    assert task_counter.tasks

    # copy all tiles
    task_counter = TaskCounter()
    cp(out_path, os.path.join(mp_tmpdir, "all"), zoom=5, observers=[task_counter])
    assert task_counter.tasks

    # copy tiles and subset by area
    task_counter = TaskCounter()
    cp(
        out_path,
        os.path.join(mp_tmpdir, "area"),
        zoom=5,
        area=wkt_geom,
        observers=[task_counter],
    )
    assert task_counter.tasks

    # copy local tiles without using threads
    task_counter = TaskCounter()
    cp(
        out_path,
        os.path.join(mp_tmpdir, "nothreads"),
        zoom=5,
        multi=1,
        observers=[task_counter],
    )
    assert task_counter.tasks


@pytest.mark.integration
def test_cp_http(mp_tmpdir, http_tiledir):
    # copy tiles and subset by bounds
    task_counter = TaskCounter()
    cp(
        http_tiledir,
        os.path.join(mp_tmpdir, "http"),
        zoom=1,
        bounds=[3, 1, 4, 2],
        observers=[task_counter],
    )
    assert task_counter.tasks


def test_rm(cleantopo_br):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.dict, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        list(mp.execute(zoom=5))
    out_path = os.path.join(TESTDATA_DIR, cleantopo_br.dict["output"]["path"])

    # remove tiles
    task_counter = TaskCounter()
    rm(out_path, zoom=5, observers=[task_counter])
    assert task_counter.tasks

    # remove tiles but this time they should already have been removed
    task_counter = TaskCounter()
    rm(out_path, zoom=5, observers=[task_counter])
    assert task_counter.tasks == 0


def test_rm_path_list(mp_tmpdir):
    out_path = mp_tmpdir / "some_file.txt"
    with out_path.open("w") as dst:
        dst.write("foo")

    assert out_path.exists()
    rm(paths=[out_path])
    assert not out_path.exists()


@pytest.mark.integration
def test_rm_path_list_s3(s3_testdata_dir):
    out_path = s3_testdata_dir / "some_file.txt"
    with out_path.open("w") as dst:
        dst.write("foo")

    assert out_path.exists()
    rm(paths=[out_path])
    assert not out_path.exists()


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
def test_execute(
    cleantopo_br_metatiling_1, cleantopo_br_tif, concurrency, process_graph
):
    execute_kwargs = dict(concurrency=concurrency)
    if concurrency == "dask":
        execute_kwargs.update(dask_settings=DaskSettings(process_graph=process_graph))

    zoom = 5
    tp = TilePyramid("geodetic")
    with rasterio_open(cleantopo_br_tif) as src:
        tiles = list(tp.tiles_from_bounds(src.bounds, zoom))
    execute(cleantopo_br_metatiling_1.dict, zoom=zoom, **execute_kwargs)
    mp = cleantopo_br_metatiling_1.mp()
    for t in tiles:
        with rasterio_open(mp.config.output.get_path(t)) as src:
            assert not src.read(masked=True).mask.all()


def test_execute_retry(example_mapchete):
    zoom = 10
    retries = 2

    class ExceptionRaiser:
        """Makes the job fail during progress."""

        def update(*args, progress=None, **kwargs):
            if progress and progress.current > 2:
                raise RuntimeError("This job just raised an exception!")

    class RetryCounter:
        """Count retry attempts."""

        retries = 0

        def update(self, *args, status=None, **kwargs):
            if status and status == Status.retrying:
                self.retries += 1

    exception_raiser = ExceptionRaiser()
    retry_counter = RetryCounter()

    # this job should fail
    with pytest.raises(RuntimeError):
        execute(
            example_mapchete.dict,
            zoom=zoom,
            retries=retries,
            observers=[exception_raiser, retry_counter],
            concurrency=None,
        )

    # make sure job has been retried
    assert retry_counter.retries == retries


def test_execute_cancel(cleantopo_br_metatiling_1):
    zoom = 5

    class CancelObserver:
        """Cancels job when running."""

        def update(*args, progress=None, **kwargs):
            if progress and progress.current > 0:
                raise JobCancelledError

    class StatusObserver:
        """Observes job state."""

        status = None

        def update(self, *args, status=None, **kwargs):
            if status:
                self.status = status

    state_observer = StatusObserver()
    execute(
        cleantopo_br_metatiling_1.dict,
        zoom=zoom,
        observers=[CancelObserver(), state_observer],
        concurrency=None,
    )
    assert state_observer.status == Status.cancelled


def test_execute_tile(mp_tmpdir, cleantopo_br_metatiling_1):
    tile = (5, 30, 63)

    task_counter = TaskCounter()
    execute(cleantopo_br_metatiling_1.dict, tile=tile, observers=[task_counter])

    assert task_counter.tasks == 1

    mp = cleantopo_br_metatiling_1.mp()
    with rasterio_open(
        mp.config.output.get_path(mp.config.output_pyramid.tile(*tile))
    ) as src:
        assert not src.read(masked=True).mask.all()


def test_execute_point(mp_tmpdir, example_mapchete, dummy2_tif):
    """Using bounds from WKT."""
    with rasterio_open(dummy2_tif) as src:
        g = box(*src.bounds)

    task_counter = TaskCounter()
    execute(
        example_mapchete.dict,
        point=[g.centroid.x, g.centroid.y],
        zoom=10,
        observers=[task_counter],
    )
    assert task_counter.tasks == 1


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
def test_execute_preprocessing_tasks(
    concurrency, preprocess_cache_raster_vector, process_graph
):
    execute_kwargs = dict(concurrency=concurrency)
    if concurrency == "dask":
        execute_kwargs.update(dask_settings=DaskSettings(process_graph=process_graph))

    task_counter = TaskCounter()
    execute(
        preprocess_cache_raster_vector.path, observers=[task_counter], **execute_kwargs
    )
    assert task_counter.tasks


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        # ("threads", False),  # profiling does not work with threads
        ("dask", False),
        ("dask", True),
        ("processes", False),
        (None, False),
    ],
)
def test_execute_profiling(cleantopo_br_metatiling_1, concurrency, process_graph):
    execute_kwargs = dict(concurrency=concurrency)
    if concurrency == "dask":
        execute_kwargs.update(dask_settings=DaskSettings(process_graph=process_graph))

    zoom = 5

    class TaskResultObserver(ObserverProtocol):
        def update(self, *args, task_result=None, **kwargs):
            if task_result:
                assert isinstance(task_result, TaskInfo)
                assert task_result.profiling
                for profiler in ["time", "memory"]:
                    assert profiler in task_result.profiling

                assert task_result.profiling["time"].elapsed > 0

                assert task_result.profiling["memory"].max_allocated > 0
                assert task_result.profiling["memory"].total_allocated > 0
                assert task_result.profiling["memory"].allocations > 0

    execute(
        cleantopo_br_metatiling_1.dict,
        zoom=zoom,
        profiling=True,
        observers=[TaskResultObserver()],
        **execute_kwargs
    )


def test_convert_geodetic(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="geodetic")
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_mercator(cleantopo_br_tif, mp_tmpdir):
    """Automatic mercator tile pyramid creation of raster files."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator")
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_custom_grid(s2_band, mp_tmpdir, custom_grid_json):
    """Automatic mercator tile pyramid creation of raster files."""
    convert(s2_band, mp_tmpdir, output_pyramid=custom_grid_json)
    for zoom, row, col in [(0, 5298, 631)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_png(cleantopo_br_tif, mp_tmpdir):
    """Automatic PNG tile pyramid creation of raster files."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", output_format="PNG")

    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".png"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with rasterio_open(out_file, "r") as src:
                assert src.meta["driver"] == "PNG"
                assert src.meta["dtype"] == "uint8"
                data = src.read(masked=True)
                assert data.mask.any()


def test_convert_bidx(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out_bidx.tif")
    convert(cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=3, bidx=1)
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    convert(cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=3)
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff_cog(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out_cog.tif")
    convert(cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=5, cog=True)
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert not data.mask.all()
    assert cog_validate(single_gtiff, strict=True)


def test_convert_single_gtiff_cog_dask(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out_cog.tif")
    convert(
        cleantopo_br_tif,
        single_gtiff,
        output_pyramid="geodetic",
        zoom=5,
        cog=True,
        concurrency="dask",
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert not data.mask.all()
    assert cog_validate(single_gtiff, strict=True)


def test_convert_single_gtiff_overviews(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    convert(
        cleantopo_br_tif,
        single_gtiff,
        output_pyramid="geodetic",
        zoom=7,
        overviews=True,
        overviews_resampling_method="bilinear",
        concurrency=None,
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert src.overviews(1)


@pytest.mark.integration
def test_convert_remote_single_gtiff(http_raster, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    convert(
        http_raster, single_gtiff, output_pyramid="geodetic", zoom=1, concurrency=None
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.any()


def test_convert_dtype(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using dtype scale."""
    convert(
        cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", output_dtype="uint8"
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_scale_ratio(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="mercator",
        output_dtype="uint8",
        scale_ratio=0.003,
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_convert_scale_offset(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="mercator",
        output_dtype="uint8",
        scale_offset=1,
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_convert_clip(cleantopo_br_tif, mp_tmpdir, landpoly):
    """Automatic tile pyramid creation cropping data."""
    task_counter = TaskCounter()
    convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="geodetic",
        clip_geometry=landpoly,
        observers=[task_counter],
    )
    assert task_counter.tasks == 0


def test_convert_zoom(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using a specific zoom."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=3)
    for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_zoom_minmax(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using min max zoom."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=[3, 4])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_zoom_maxmin(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using max min zoom."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=[4, 3])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_mapchete(cleantopo_br, mp_tmpdir):
    # prepare data
    execute(cleantopo_br.path, zoom=[1, 3])

    convert(
        cleantopo_br.path,
        mp_tmpdir,
        output_pyramid="geodetic",
        output_metatiling=1,
        zoom=[1, 3],
    )
    for zoom, row, col in [(3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_tiledir(cleantopo_br, mp_tmpdir):
    bounds = BufferedTilePyramid("geodetic").tile(4, 15, 31).bounds
    # prepare data
    with mapchete.open(cleantopo_br.dict) as mp:
        list(mp.execute(zoom=[1, 4]))
    convert(
        os.path.join(
            cleantopo_br.dict["config_dir"], cleantopo_br.dict["output"]["path"]
        ),
        mp_tmpdir,
        output_pyramid="geodetic",
        output_metatiling=1,
        zoom=[1, 4],
        bounds=bounds,
    )
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_gcps(gcps_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    out_file = os.path.join(mp_tmpdir, "gcps_out.tif")
    convert(gcps_tif, out_file, output_pyramid="geodetic", zoom=8)
    with rasterio_open(out_file, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()


def test_convert_geojson(landpoly, mp_tmpdir):
    convert(landpoly, mp_tmpdir, output_pyramid="geodetic", zoom=4)
    for (zoom, row, col), control in zip([(4, 0, 7), (4, 1, 7)], [9, 32]):
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col) + ".geojson"]
        )
        with fiona_open(out_file, "r") as src:
            assert len(src) == control
            for f in src:
                assert shape(f["geometry"]).is_valid


def test_convert_geobuf(landpoly, mp_tmpdir):
    # convert to geobuf
    geobuf_outdir = os.path.join(mp_tmpdir, "geobuf")
    convert(
        landpoly,
        geobuf_outdir,
        output_pyramid="geodetic",
        zoom=4,
        output_format="Geobuf",
    )
    for (zoom, row, col), control in zip([(4, 0, 7), (4, 1, 7)], [9, 32]):
        out_file = os.path.join(
            *[geobuf_outdir, str(zoom), str(row), str(col) + ".pbf"]
        )
        with open(out_file, "rb") as src:
            features = geobuf.decode(src.read())["features"]
            assert len(features) == control
            for f in features:
                assert f["geometry"]["type"] == "Polygon"
                assert shape(f["geometry"]).area


def test_convert_errors(s2_band_jp2, mp_tmpdir, s2_band, cleantopo_br, landpoly):
    # output format required
    with pytest.raises(ValueError):
        convert(s2_band_jp2, mp_tmpdir, output_pyramid="geodetic")

    # output pyramid reqired
    with pytest.raises(ValueError):
        convert(s2_band, mp_tmpdir)

    # prepare data for tiledir input
    with mapchete.open(cleantopo_br.dict) as mp:
        list(mp.execute(zoom=[1, 4]))
    tiledir_path = os.path.join(
        cleantopo_br.dict["config_dir"], cleantopo_br.dict["output"]["path"]
    )

    # zoom level required
    with pytest.raises(ValueError):
        convert(tiledir_path, mp_tmpdir, output_pyramid="geodetic")

    # incompatible formats
    with pytest.raises(ValueError):
        convert(
            tiledir_path,
            mp_tmpdir,
            output_pyramid="geodetic",
            zoom=5,
            output_format="GeoJSON",
        )

    # unsupported output format extension
    with pytest.raises(ValueError):
        convert(s2_band_jp2, "output.jp2", output_pyramid="geodetic", zoom=5)

    # malformed band index
    with pytest.raises(ValueError):
        convert(s2_band_jp2, "output.tif", bidx="invalid")


def test_index_geojson(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    # generate index for zoom 3
    index(cleantopo_br.dict, zoom=3, geojson=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 4
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_geojson_fieldname(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    # index and rename "location" to "new_fieldname"
    index(
        cleantopo_br.dict,
        zoom=3,
        geojson=True,
        fieldname="new_fieldname",
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert "new_fieldname" in f["properties"]
        assert len(list(src)) == 1


def test_index_geojson_basepath(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    index(cleantopo_br.dict, zoom=3, geojson=True, basepath=basepath)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert f["properties"]["location"].startswith(basepath)
        assert len(list(src)) == 1


def test_index_geojson_for_gdal(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    index(cleantopo_br.dict, zoom=3, geojson=True, basepath=basepath, for_gdal=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert f["properties"]["location"].startswith("/vsicurl/" + basepath)
        assert len(list(src)) == 1


def test_index_geojson_tile(mp_tmpdir, cleantopo_tl):
    # execute process at zoom 3
    execute(cleantopo_tl.dict, zoom=3)

    # generate index
    index(cleantopo_tl.dict, tile=(3, 0, 0), geojson=True)

    with mapchete.open(cleantopo_tl.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 4
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        assert len(list(src)) == 1


def test_index_geojson_wkt_area(mp_tmpdir, cleantopo_tl, wkt_geom_tl):
    # execute process at zoom 3
    execute(cleantopo_tl.dict, area=wkt_geom_tl)

    # generate index for zoom 3
    index(cleantopo_tl.dict, geojson=True, area=wkt_geom_tl)

    with mapchete.open(cleantopo_tl.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 14
        assert "3.geojson" in files


def test_index_gpkg(mp_tmpdir, cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, gpkg=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, gpkg=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_shp(mp_tmpdir, cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, shp=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona_open(mp.config.output.path / "5.shp") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, shp=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona_open(mp.config.output.path / "5.shp") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_fgb(mp_tmpdir, cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, fgb=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.fgb" in files
    with fiona_open(mp.config.output.path / "5.fgb") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, fgb=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.fgb" in files
    with fiona_open(mp.config.output.path / "5.fgb") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_text(cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, txt=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with open(os.path.join(mp.config.output.path, "5.txt")) as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, txt=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with open(os.path.join(mp.config.output.path, "5.txt")) as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")


def test_index_tiledir(cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.output_path, zoom=5, gpkg=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, txt=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_errors(mp_tmpdir, cleantopo_br):
    with pytest.raises(ValueError):
        index(cleantopo_br.dict, zoom=5)


def test_convert_empty_gpkg(empty_gpkg, mp_tmpdir):
    convert(
        empty_gpkg,
        mp_tmpdir,
        output_pyramid="geodetic",
        zoom=5,
        output_format="GeoJSON",
    )
