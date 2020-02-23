"""All pytest fixtures."""

import boto3
from collections import namedtuple
import os
import pytest
import shutil
import uuid
import yaml

from mapchete.cli.default.serve import create_app

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPT_DIR, "testdata")
TEMP_DIR = os.path.join(TESTDATA_DIR, "tmp")
S3_TEMP_DIR = "s3://mapchete-test/tmp/" + uuid.uuid4().hex


ExampleConfig = namedtuple("ExampleConfig", ("path", "dict"))


# flask test app for mapchete serve
@pytest.fixture
def app(dem_to_hillshade, cleantopo_br, geojson, mp_tmpdir):
    """Dummy Flask app."""
    return create_app(
        mapchete_files=[dem_to_hillshade.path, cleantopo_br.path, geojson.path],
        zoom=None, bounds=None, single_input_file=None, mode="overwrite",
        debug=True)


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

    def _cleanup():
        for obj in boto3.resource('s3').Bucket(S3_TEMP_DIR.split("/")[2]).objects.filter(
            Prefix="/".join(S3_TEMP_DIR.split("/")[-2:])
        ):
            obj.delete()

    _cleanup()
    yield S3_TEMP_DIR
    _cleanup()


@pytest.fixture
def wkt_geom():
    """Example WKT geometry."""
    return "Polygon ((2.8125 11.25, 2.8125 14.0625, 0 14.0625, 0 11.25, 2.8125 11.25))"


# example files
@pytest.fixture
def http_raster():
    """Fixture for HTTP raster."""
    return "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/1/0/0.tif"


@pytest.fixture
def s2_band():
    """
    Fixture for Sentinel-2 raster band.

    Original file:
    s3://sentinel-s2-l1c/tiles/33/T/WN/2016/4/3/0/B02.jp2
    """
    return os.path.join(TESTDATA_DIR, "s2_band.tif")


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
def landpoly():
    """Fixture for landpoly.geojson."""
    return os.path.join(TESTDATA_DIR, "landpoly.geojson")


@pytest.fixture
def landpoly_3857():
    """Fixture for landpoly_3857.geojson"""
    return os.path.join(TESTDATA_DIR, "landpoly_3857.geojson")


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


# example mapchete configurations
@pytest.fixture
def custom_grid():
    """Fixture for custom_grid.mapchete."""
    path = os.path.join(TESTDATA_DIR, "custom_grid.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def deprecated_params():
    """Fixture for deprecated_params.mapchete."""
    path = os.path.join(TESTDATA_DIR, "deprecated_params.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def abstract_input():
    """Fixture for abstract_input.mapchete."""
    path = os.path.join(TESTDATA_DIR, "abstract_input.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def files_zooms():
    """Fixture for files_zooms.mapchete."""
    path = os.path.join(TESTDATA_DIR, "files_zooms.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def file_groups():
    """Fixture for file_groups.mapchete."""
    path = os.path.join(TESTDATA_DIR, "file_groups.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def baselevels():
    """Fixture for baselevels.mapchete."""
    path = os.path.join(TESTDATA_DIR, "baselevels.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def baselevels_output_buffer():
    """Fixture for baselevels_output_buffer.mapchete."""
    path = os.path.join(TESTDATA_DIR, "baselevels_output_buffer.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def baselevels_custom_nodata():
    """Fixture for baselevels_custom_nodata.mapchete."""
    path = os.path.join(TESTDATA_DIR, "baselevels_custom_nodata.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def mapchete_input():
    """Fixture for mapchete_input.mapchete."""
    path = os.path.join(TESTDATA_DIR, "mapchete_input.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def dem_to_hillshade():
    """Fixture for dem_to_hillshade.mapchete."""
    path = os.path.join(TESTDATA_DIR, "dem_to_hillshade.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def files_bounds():
    """Fixture for files_bounds.mapchete."""
    path = os.path.join(TESTDATA_DIR, "files_bounds.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def example_mapchete():
    """Fixture for example.mapchete."""
    path = os.path.join(SCRIPT_DIR, "example.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def zoom_mapchete():
    """Fixture for zoom.mapchete."""
    path = os.path.join(TESTDATA_DIR, "zoom.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def minmax_zoom():
    """Fixture for minmax_zoom.mapchete."""
    path = os.path.join(TESTDATA_DIR, "minmax_zoom.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_tl():
    """Fixture for cleantopo_tl.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_tl.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_br():
    """Fixture for cleantopo_br.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_br.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_remote():
    """Fixture for cleantopo_remote.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_remote.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_br_tiledir():
    """Fixture for cleantopo_br_tiledir.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_br_tiledir.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_br_tiledir_mercator():
    """Fixture for cleantopo_br_tiledir_mercator.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_br_tiledir_mercator.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_br_mercator():
    """Fixture for cleantopo_br_mercator.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_br_mercator.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def geojson():
    """Fixture for geojson.mapchete."""
    path = os.path.join(TESTDATA_DIR, "geojson.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def geojson_s3():
    """Fixture for geojson.mapchete with updated output path."""
    path = os.path.join(TESTDATA_DIR, "geojson.mapchete")
    config = _dict_from_mapchete(path)
    config["output"].update(path=S3_TEMP_DIR)
    return ExampleConfig(path=None, dict=config)


@pytest.fixture
def geojson_tiledir():
    """Fixture for geojson_tiledir.mapchete."""
    path = os.path.join(TESTDATA_DIR, "geojson_tiledir.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def process_module():
    """Fixture for process_module.mapchete"""
    path = os.path.join(TESTDATA_DIR, "process_module.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def gtiff_s3():
    """Fixture for gtiff_s3.mapchete."""
    path = os.path.join(TESTDATA_DIR, "gtiff_s3.mapchete")
    config = _dict_from_mapchete(path)
    config["output"].update(path=S3_TEMP_DIR)
    return ExampleConfig(path=None, dict=config)


@pytest.fixture
def output_single_gtiff():
    """Fixture for output_single_gtiff.mapchete."""
    path = os.path.join(TESTDATA_DIR, "output_single_gtiff.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def output_single_gtiff_cog():
    """Fixture for output_single_gtiff_cog.mapchete."""
    path = os.path.join(TESTDATA_DIR, "output_single_gtiff_cog.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def s3_example_tile(gtiff_s3):
    """Example tile for fixture."""
    return (5, 15, 32)


# helper functions
def _dict_from_mapchete(path):
    config = yaml.load(open(path).read())
    config.update(config_dir=os.path.dirname(path))
    return config
