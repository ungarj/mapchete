"""All pytest fixtures."""

from collections import namedtuple
import datetime
import os
import pytest
import rasterio
from shapely import wkt
from shapely.geometry import box
import shutil
import uuid

from tilematrix import Bounds, GridDefinition

from mapchete.cli.default.serve import create_app
from mapchete._executor import DaskExecutor
from mapchete.io import fs_from_path
from mapchete.io.vector import reproject_geometry
from mapchete.testing import ProcessFixture
from mapchete.tile import BufferedTilePyramid


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPT_DIR, "testdata")
TEMP_DIR = os.path.join(TESTDATA_DIR, "tmp")
S3_TEMP_DIR = "s3://mapchete-test/tmp/" + uuid.uuid4().hex


# flask test app for mapchete serve
@pytest.fixture
def app(dem_to_hillshade, cleantopo_br, geobuf, geojson, mp_tmpdir):
    """Dummy Flask app."""
    return create_app(
        mapchete_files=[
            dem_to_hillshade.path,
            cleantopo_br.path,
            geojson.path,
            geobuf.path,
        ],
        zoom=None,
        bounds=None,
        single_input_file=None,
        mode="overwrite",
        debug=True,
    )


# temporary directory for I/O tests
@pytest.fixture(autouse=True)
def mp_tmpdir():
    """Setup and teardown temporary directory."""
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    os.makedirs(TEMP_DIR)
    yield TEMP_DIR
    shutil.rmtree(TEMP_DIR, ignore_errors=True)


# temporary directory for I/O tests
@pytest.fixture
def mp_s3_tmpdir():
    """Setup and teardown temporary directory."""
    fs = fs_from_path(S3_TEMP_DIR)

    def _cleanup():
        try:
            fs.rm(S3_TEMP_DIR, recursive=True)
        except FileNotFoundError:
            pass

    _cleanup()
    yield S3_TEMP_DIR
    _cleanup()


@pytest.fixture
def wkt_geom():
    """Example WKT geometry."""
    return "Polygon ((2.8125 11.25, 2.8125 14.0625, 0 14.0625, 0 11.25, 2.8125 11.25))"


@pytest.fixture
def wkt_geom_tl():
    """Example WKT geometry."""
    return "Polygon ((-176.04949 85.59671, -174.57652 73.86651, -159.98073 74.58961, -161.74829 83.05249, -176.04949 85.59671))"


# example files
@pytest.fixture
def http_raster():
    """Fixture for HTTP raster."""
    return "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/1/0/0.tif"


@pytest.fixture
def http_tiledir():
    """Fixture for HTTP TileDirectory."""
    return "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/"


@pytest.fixture
def s2_band():
    """
    Fixture for Sentinel-2 raster band.

    Original file:
    s3://sentinel-s2-l1c/tiles/33/T/WN/2016/4/3/0/B02.jp2
    """
    return os.path.join(TESTDATA_DIR, "s2_band.tif")


@pytest.fixture
def s2_band_tile():
    tp = BufferedTilePyramid("geodetic")
    with rasterio.open(os.path.join(TESTDATA_DIR, "s2_band.tif")) as src:
        rr_center = reproject_geometry(
            geometry=box(*src.bounds), src_crs=src.crs, dst_crs=tp.crs
        ).centroid
        return next(tp.tiles_from_geom(rr_center, 13))


@pytest.fixture
def s2_band_jp2():
    """
    Fixture for Sentinel-2 raster band.

    Original file:
    s3://sentinel-s2-l1c/tiles/33/T/WN/2016/4/3/0/B02.jp2
    """
    return os.path.join(TESTDATA_DIR, "s2_band.jp2")


@pytest.fixture
def s2_band_remote():
    """
    Fixture for remote file on S3 bucket.
    """
    return "s3://mapchete-test/4band_test.tif"


@pytest.fixture
def empty_gpkg():
    """Fixture for HTTP raster."""
    return os.path.join(TESTDATA_DIR, "empty.gpkg")


@pytest.fixture
def s3_metadata_json():
    """
    Fixture for s3://mapchete-test/metadata.json.
    """
    return "s3://mapchete-test/metadata.json"


@pytest.fixture
def http_metadata_json():
    """
    Fixture for https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/metadata.json.
    """
    return "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/metadata.json"


@pytest.fixture
def old_style_metadata_json():
    """
    Fixture for old_style_metadata.json.
    """
    return os.path.join(TESTDATA_DIR, "old_style_metadata.json")


@pytest.fixture
def old_geodetic_shape_metadata_json():
    """
    Fixture for old_geodetic_shape_metadata.json.
    """
    return os.path.join(TESTDATA_DIR, "old_geodetic_shape_metadata.json")


@pytest.fixture
def driver_metadata_dict():
    """Content of a metadata.json file as dictionary."""
    return {
        "driver": {
            "bands": 1,
            "compress": "deflate",
            "delimiters": {
                "bounds": [-180.0, -90.0, 180.0, 90.0],
                "effective_bounds": [-180.17578125, -90.0, 180.17578125, 90.0],
                "process_bounds": [-180.0, -90.0, 180.0, 90.0],
                "zoom": [0, 1, 2, 3, 4, 5, 6, 7, 8],
            },
            "dtype": "float32",
            "format": "GTiff",
            "mode": "continue",
            "nodata": -32768.0,
            "predictor": 3,
        },
        "pyramid": {
            "grid": {
                "bounds": [-180.0, -90.0, 180.0, 90.0],
                "is_global": True,
                "shape": [1, 2],
                "srs": {
                    "wkt": 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
                },
                "type": "geodetic",
            },
            "metatiling": 8,
            "pixelbuffer": 64,
            "tile_size": 256,
        },
    }


@pytest.fixture
def driver_output_params_dict():
    return {
        "format": "xarray",
        "path": "/foo/bar",
        "dtype": "uint16",
        "bands": 3,
        "chunksize": 256,
        "time": {
            "start": datetime.date(2022, 6, 1),
            "end": datetime.date(2022, 6, 11),
            "steps": [
                datetime.date(2022, 6, 1),
                datetime.date(2022, 6, 4),
                datetime.date(2022, 6, 6),
                datetime.date(2022, 6, 9),
                datetime.date(2022, 6, 11),
            ],
        },
        "grid": GridDefinition("geodetic"),
        "pixelbuffer": 0,
        "metatiling": 1,
        "delimiters": {
            "zoom": [5],
            "bounds": Bounds(left=15.0064, bottom=47.7604, right=16.4863, top=48.7485),
            "process_bounds": Bounds(
                left=15.0064, bottom=47.7604, right=16.4863, top=48.7485
            ),
            "effective_bounds": Bounds(
                left=11.25, bottom=45.0, right=16.875, top=50.625
            ),
        },
        "mode": "continue",
    }


@pytest.fixture
def landpoly():
    """Fixture for landpoly.geojson."""
    return os.path.join(TESTDATA_DIR, "landpoly.geojson")


@pytest.fixture
def landpoly_3857():
    """Fixture for landpoly_3857.geojson"""
    return os.path.join(TESTDATA_DIR, "landpoly_3857.geojson")


@pytest.fixture
def aoi_br_geojson():
    """Fixture for aoi_br.geojson"""
    return os.path.join(TESTDATA_DIR, "aoi_br.geojson")


@pytest.fixture
def sample_geojson():
    """Fixture for sample.geojson"""
    return os.path.join(TESTDATA_DIR, "sample.geojson")


@pytest.fixture
def geometrycollection():
    """Fixture for geometrycollection.geojson"""
    return wkt.loads(
        "GEOMETRYCOLLECTION (LINESTRING (-100.9423828125 78.75, -100.8984375 78.75), LINESTRING (-100.2392578125 78.75, -99.9755859375 78.75), POLYGON ((-101.25 78.9697265625, -101.25 79.013671875, -101.2060546875 79.013671875, -101.2060546875 78.9697265625, -100.986328125 78.9697265625, -100.986328125 78.92578125, -101.0302734375 78.92578125, -101.0302734375 78.8818359375, -101.07421875 78.8818359375, -101.1181640625 78.8818359375, -101.1181640625 78.837890625, -101.162109375 78.837890625, -101.2060546875 78.837890625, -101.2060546875 78.7939453125, -100.9423828125 78.7939453125, -100.9423828125 78.75, -101.25 78.75, -101.25 78.9697265625)), POLYGON ((-100.8984375 78.75, -100.8984375 78.7939453125, -100.5908203125 78.7939453125, -100.546875 78.7939453125, -100.546875 78.837890625, -100.3271484375 78.837890625, -100.3271484375 78.7939453125, -100.2392578125 78.7939453125, -100.2392578125 78.75, -100.8984375 78.75)))"
    )


@pytest.fixture
def cleantopo_br_tif():
    """Fixture for cleantopo_br.tif"""
    return os.path.join(TESTDATA_DIR, "cleantopo_br.tif")


@pytest.fixture
def cleantopo_tl_tif():
    """Fixture for cleantopo_tl.tif"""
    return os.path.join(TESTDATA_DIR, "cleantopo_tl.tif")


@pytest.fixture
def dummy1_3857_tif():
    """Fixture for dummy1_3857.tif"""
    return os.path.join(TESTDATA_DIR, "dummy1_3857.tif")


@pytest.fixture
def dummy1_tif():
    """Fixture for dummy1.tif"""
    return os.path.join(TESTDATA_DIR, "dummy1.tif")


@pytest.fixture
def dummy2_tif():
    """Fixture for dummy2.tif"""
    return os.path.join(TESTDATA_DIR, "dummy2.tif")


@pytest.fixture
def invalid_tif():
    """Fixture for invalid.tif"""
    return os.path.join(TESTDATA_DIR, "invalid.tif")


@pytest.fixture
def gcps_tif():
    """Fixture for gcps.tif"""
    return os.path.join(TESTDATA_DIR, "gcps.tif")


@pytest.fixture
def invalid_geojson():
    """Fixture for invalid.geojson"""
    return os.path.join(TESTDATA_DIR, "invalid.geojson")


@pytest.fixture
def execute_kwargs_py():
    """Fixture for execute_kwargs.py"""
    return os.path.join(TESTDATA_DIR, "execute_kwargs.py")


@pytest.fixture
def write_rasterfile_tags_py():
    """Fixture for write_rasterfile_tags.py"""
    return os.path.join(TESTDATA_DIR, "write_rasterfile_tags.py")


@pytest.fixture
def import_error_py():
    """Fixture for import_error.py"""
    return os.path.join(TESTDATA_DIR, "import_error.py")


@pytest.fixture
def malformed_py():
    """Fixture for malformed.py"""
    return os.path.join(TESTDATA_DIR, "malformed.py")


@pytest.fixture
def syntax_error_py():
    """Fixture for syntax_error.py"""
    return os.path.join(TESTDATA_DIR, "syntax_error.py")


@pytest.fixture
def execute_params_error_py():
    """Fixture for execute_params_error.py"""
    return os.path.join(TESTDATA_DIR, "execute_params_error.py")


@pytest.fixture
def process_error_py():
    """Fixture for process_error.py"""
    return os.path.join(TESTDATA_DIR, "process_error.py")


@pytest.fixture
def output_error_py():
    """Fixture for output_error.py"""
    return os.path.join(TESTDATA_DIR, "output_error.py")


@pytest.fixture
def old_style_process_py():
    """Fixture for old_style_process.py"""
    return os.path.join(TESTDATA_DIR, "old_style_process.py")


@pytest.fixture
def custom_grid_json():
    return os.path.join(TESTDATA_DIR, "custom_grid.json")


# example mapchete configurations
@pytest.fixture
def custom_grid():
    """Fixture for custom_grid.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "custom_grid.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def deprecated_params():
    """Fixture for deprecated_params.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "deprecated_params.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def files_zooms():
    """Fixture for files_zooms.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "files_zooms.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def file_groups():
    """Fixture for file_groups.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "file_groups.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def baselevels():
    """Fixture for baselevels.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "baselevels.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def baselevels_output_buffer():
    """Fixture for baselevels_output_buffer.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "baselevels_output_buffer.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def baselevels_custom_nodata():
    """Fixture for baselevels_custom_nodata.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "baselevels_custom_nodata.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def mapchete_input():
    """Fixture for mapchete_input.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "mapchete_input.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def dem_to_hillshade():
    """Fixture for dem_to_hillshade.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "dem_to_hillshade.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def files_bounds():
    """Fixture for files_bounds.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "files_bounds.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def example_mapchete():
    """Fixture for example.mapchete."""
    with ProcessFixture(
        os.path.join(SCRIPT_DIR, "example.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def example_custom_process_mapchete():
    """Fixture for example.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "example_custom_process.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def zoom_mapchete():
    """Fixture for zoom.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "zoom.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def minmax_zoom():
    """Fixture for minmax_zoom.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "minmax_zoom.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def cleantopo_tl():
    """Fixture for cleantopo_tl.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "cleantopo_tl.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br():
    """Fixture for cleantopo_br.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "cleantopo_br.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_metatiling_1():
    """Fixture for cleantopo_br.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "cleantopo_br_metatiling_1.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def cleantopo_remote():
    """Fixture for cleantopo_remote.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "cleantopo_remote.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_tiledir():
    """Fixture for cleantopo_br_tiledir.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "cleantopo_br_tiledir.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_tiledir_mercator():
    """Fixture for cleantopo_br_tiledir_mercator.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "cleantopo_br_tiledir_mercator.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_mercator():
    """Fixture for cleantopo_br_mercator.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "cleantopo_br_mercator.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def geojson():
    """Fixture for geojson.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "geojson.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def geojson_s3():
    """Fixture for geojson.mapchete with updated output path."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "geojson.mapchete"),
        output_tempdir=S3_TEMP_DIR,
    ) as example:
        yield example


@pytest.fixture
def geobuf():
    """Fixture for geobuf.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "geobuf.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def geobuf_s3():
    """Fixture for geobuf.mapchete with updated output path."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "geobuf.mapchete"),
        output_tempdir=S3_TEMP_DIR,
    ) as example:
        yield example


@pytest.fixture
def flatgeobuf():
    """Fixture for flatgeobuf.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "flatgeobuf.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def flatgeobuf_s3():
    """Fixture for flatgeobuf.mapchete with updated output path."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "flatgeobuf.mapchete"),
        output_tempdir=S3_TEMP_DIR,
    ) as example:
        yield example


@pytest.fixture
def geojson_tiledir():
    """Fixture for geojson_tiledir.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "geojson_tiledir.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def process_module():
    """Fixture for process_module.mapchete"""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "process_module.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def gtiff_s3():
    """Fixture for gtiff_s3.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "gtiff_s3.mapchete"),
        output_tempdir=S3_TEMP_DIR,
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff():
    """Fixture for output_single_gtiff.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "output_single_gtiff.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff_s3():
    """Fixture for output_single_gtiff.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "output_single_gtiff.mapchete"),
        output_tempdir=S3_TEMP_DIR,
        output_suffix=".tif",
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff_cog():
    """Fixture for output_single_gtiff_cog.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "output_single_gtiff_cog.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff_cog_s3():
    """Fixture for output_single_gtiff_cog.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "output_single_gtiff_cog.mapchete"),
        output_tempdir=S3_TEMP_DIR,
        output_suffix=".tif",
    ) as example:
        yield example


@pytest.fixture
def aoi_br():
    """Fixture for aoi_br.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "aoi_br.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def preprocess_cache_raster_vector():
    """Fixture for preprocess_cache_raster_vector.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "preprocess_cache_raster_vector.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def preprocess_cache_memory():
    """Fixture for preprocess_cache_memory.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "preprocess_cache_memory.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def preprocess_cache_memory_single_file():
    """Fixture for preprocess_cache_memory_single_file.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "preprocess_cache_memory_single_file.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def custom_grid_points():
    """Fixture for custom_grid_points.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "custom_grid_points.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def stac_metadata():
    """Fixture for stac_metadata.mapchete."""
    with ProcessFixture(
        os.path.join(TESTDATA_DIR, "stac_metadata.mapchete"),
    ) as example:
        yield example


@pytest.fixture
def s3_example_tile(gtiff_s3):
    """Example tile for fixture."""
    return (5, 15, 32)


@pytest.fixture(scope="package")
def dask_executor():
    """DaskExecutor()"""
    with DaskExecutor() as executor:
        yield executor
