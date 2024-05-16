"""All pytest fixtures."""

import datetime
import os
import uuid
from tempfile import TemporaryDirectory
from typing import Generator, List

import pytest
from aiohttp.client_exceptions import ClientConnectorError
from minio import Minio
from shapely import wkt
from shapely.geometry import box
from tilematrix import Bounds, GridDefinition

from mapchete.cli.default.serve import create_app
from mapchete.executor import (
    ConcurrentFuturesExecutor,
    DaskExecutor,
    SequentialExecutor,
)
from mapchete.io import MPath, copy, rasterio_open
from mapchete.io.vector import reproject_geometry
from mapchete.testing import ProcessFixture
from mapchete.tile import BufferedTile, BufferedTilePyramid

MINIO_TESTDATA_BUCKET = "testdata"
HTTP_USERNAME = "client"
HTTP_PASSWORD = "password2022"
S3_KEY = "eecang7G"
S3_SECRET = "Eashei2a"
S3_ENDPOINT_URL = "localhost:9000"
TESTDATA_DIR = "testdata"
HTTP_TESTDATA_DIR = MPath("http://localhost/open/")
SECURE_HTTP_TESTDATA_DIR = MPath(
    "http://localhost/secure/",
    storage_options=dict(username=HTTP_USERNAME, password=HTTP_PASSWORD),
)
AWS_S3_TESTDATA_DIR = MPath("s3://mapchete-test/")


def prepare_s3_testfile(bucket, testfile) -> MPath:
    dst = bucket / testfile
    if not dst.exists():
        copy(
            MPath(os.path.dirname(os.path.realpath(__file__)))
            / TESTDATA_DIR
            / testfile,
            dst,
        )
    return dst


@pytest.fixture()
def script_dir() -> MPath:
    return MPath(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture()
def examples_dir(script_dir) -> MPath:
    return script_dir.parent / "examples"


@pytest.fixture()
def testdata_dir(script_dir) -> MPath:
    return script_dir / TESTDATA_DIR


@pytest.fixture(scope="session")
def http_testdata_dir() -> MPath:
    try:
        HTTP_TESTDATA_DIR.ls()
    except ClientConnectorError:
        raise ConnectionError(
            "HTTP test endpoint is not available, please run "
            "'docker-compose -f test/docker-compose.yml up --remove-orphans' "
            "before running tests"
        )
    return HTTP_TESTDATA_DIR


@pytest.fixture(scope="session")
def secure_http_testdata_dir() -> MPath:
    try:
        SECURE_HTTP_TESTDATA_DIR.ls()
    except ClientConnectorError:
        raise ConnectionError(
            "HTTP test endpoint is not available, please run "
            "'docker-compose -f test/docker-compose.yml up --remove-orphans' "
            "before running tests"
        )
    return SECURE_HTTP_TESTDATA_DIR


@pytest.fixture(scope="session")
def s3_testdata_dir(minio_testdata_bucket) -> MPath:
    bucket_path = minio_testdata_bucket / MINIO_TESTDATA_BUCKET
    # prepare at least one file for testing
    prepare_s3_testfile(bucket_path, "cleantopo/cleantopo.json")
    return bucket_path


@pytest.fixture(scope="session")
def aws_s3_testdata_dir() -> MPath:
    from botocore.exceptions import NoCredentialsError

    try:
        AWS_S3_TESTDATA_DIR.ls()
    except (NoCredentialsError, PermissionError):
        raise PermissionError("credentials for s3://mapchete-test are not set")
    return AWS_S3_TESTDATA_DIR


@pytest.fixture(scope="session")
def minio_testdata_bucket() -> MPath:
    from urllib3.exceptions import MaxRetryError

    minio = Minio(
        S3_ENDPOINT_URL, access_key=S3_KEY, secret_key=S3_SECRET, secure=False
    )
    try:
        if not minio.bucket_exists(MINIO_TESTDATA_BUCKET):
            minio.make_bucket(MINIO_TESTDATA_BUCKET)
    except MaxRetryError:
        raise ConnectionError(
            "minio S3 test endpoint is not available, please run "
            "'docker-compose -f test/docker-compose.yml up --remove-orphans' "
            "before running tests"
        )
    s3_testdata = MPath(
        f"s3://{MINIO_TESTDATA_BUCKET}/",
        fs_options=dict(
            endpoint_url=f"http://{S3_ENDPOINT_URL}", key=S3_KEY, secret=S3_SECRET
        ),
    )
    return s3_testdata


# flask test app for mapchete serve
@pytest.fixture
def app(dem_to_hillshade, cleantopo_br, geojson):
    """Dummy Flask app."""
    return create_app(
        mapchete_files=[
            dem_to_hillshade.path,
            cleantopo_br.path,
            geojson.path,
        ],
        zoom=None,
        bounds=None,
        single_input_file=None,
        mode="overwrite",
        debug=True,
    )


# temporary directory for I/O tests
@pytest.fixture(autouse=True)
def mp_tmpdir() -> Generator[MPath, None, None]:
    """Setup and teardown temporary directory."""
    with TemporaryDirectory() as tempdir_path:
        tempdir = MPath(tempdir_path)
        tempdir.makedirs()
        yield tempdir


# temporary directory for I/O tests
@pytest.fixture
def mp_s3_tmpdir(minio_testdata_bucket) -> Generator[MPath, None, None]:
    """Setup and teardown temporary directory."""
    tempdir = minio_testdata_bucket / "tmp" / uuid.uuid4().hex

    def _cleanup():
        tempdir.rm(recursive=True, ignore_errors=True)

    _cleanup()
    yield tempdir
    _cleanup()


@pytest.fixture
def wkt_geom() -> str:
    """Example WKT geometry."""
    return "Polygon ((2.8125 11.25, 2.8125 14.0625, 0 14.0625, 0 11.25, 2.8125 11.25))"


@pytest.fixture
def wkt_geom_tl() -> str:
    """Example WKT geometry."""
    return "Polygon ((-176.04949 85.59671, -174.57652 73.86651, -159.98073 74.58961, -161.74829 83.05249, -176.04949 85.59671))"


# example files
@pytest.fixture
def local_raster(testdata_dir) -> MPath:
    """Fixture for HTTP raster."""
    return testdata_dir / "cleantopo/1/0/0.tif"


@pytest.fixture
def http_raster(http_testdata_dir) -> MPath:
    """Fixture for HTTP raster."""
    return http_testdata_dir / "cleantopo/1/0/0.tif"


@pytest.fixture
def secure_http_raster(secure_http_testdata_dir) -> MPath:
    """Fixture for HTTP raster."""
    return secure_http_testdata_dir / "cleantopo/1/0/0.tif"


@pytest.fixture
def http_tiledir(http_testdata_dir) -> MPath:
    """Fixture for HTTP TileDirectory."""
    return http_testdata_dir / "cleantopo"


@pytest.fixture
def secure_http_tiledir(secure_http_testdata_dir) -> MPath:
    """Fixture for HTTP TileDirectory."""
    return secure_http_testdata_dir / "cleantopo"


@pytest.fixture
def stacta(testdata_dir) -> MPath:
    """Fixture for STACTA."""
    return testdata_dir / "cleantopo" / "cleantopo.json"


@pytest.fixture
def http_stacta(http_testdata_dir) -> MPath:
    """Fixture for HTTP STACTA."""
    return http_testdata_dir / "cleantopo" / "cleantopo.json"


@pytest.fixture
def secure_http_stacta(secure_http_testdata_dir) -> MPath:
    """Fixture for secure HTTP STACTA."""
    return secure_http_testdata_dir / "cleantopo" / "cleantopo.json"


@pytest.fixture
def s3_stacta(minio_testdata_bucket) -> MPath:
    """Fixture for S3 STACTA."""
    return prepare_s3_testfile(minio_testdata_bucket, "cleantopo/cleantopo.json")


@pytest.fixture
def aws_s3_stacta(aws_s3_testdata_dir) -> MPath:
    """Fixture for AWS S3 STACTA."""
    return aws_s3_testdata_dir / "cleantopo" / "cleantopo.json"


@pytest.fixture
def s2_band(testdata_dir) -> MPath:
    """
    Fixture for Sentinel-2 raster band.

    Original file:
    s3://sentinel-s2-l1c/tiles/33/T/WN/2016/4/3/0/B02.jp2
    """
    return testdata_dir / "s2_band.tif"


@pytest.fixture
def s2_band_tile(testdata_dir):
    tp = BufferedTilePyramid("geodetic")
    with rasterio_open(testdata_dir / "s2_band.tif") as src:
        rr_center = reproject_geometry(
            geometry=box(*src.bounds), src_crs=src.crs, dst_crs=tp.crs
        ).centroid
        return next(tp.tiles_from_geom(rr_center, 13))


@pytest.fixture
def s2_band_jp2(testdata_dir) -> MPath:
    """
    Fixture for Sentinel-2 raster band.

    Original file:
    s3://sentinel-s2-l1c/tiles/33/T/WN/2016/4/3/0/B02.jp2
    """
    return testdata_dir / "s2_band.jp2"


@pytest.fixture
def raster_4band_tile() -> BufferedTile:
    """
    A tile intersecting with 4band_test.tif.
    """
    return BufferedTilePyramid("geodetic").tile(13, 2209, 8569)


@pytest.fixture
def raster_4band(testdata_dir) -> MPath:
    """
    Fixture for 4band_test.tif.
    """
    return testdata_dir / "4band_test.tif"


@pytest.fixture
def raster_4band_s3(minio_testdata_bucket) -> MPath:
    """
    Fixture for remote file on S3 bucket.
    """
    return prepare_s3_testfile(minio_testdata_bucket, "4band_test.tif")


@pytest.fixture
def raster_4band_http(http_testdata_dir) -> MPath:
    """
    Fixture for 4band_test.tif.
    """
    return http_testdata_dir / "4band_test.tif"


@pytest.fixture
def raster_4band_secure_http(secure_http_testdata_dir) -> MPath:
    """
    Fixture for 4band_test.tif.
    """
    return secure_http_testdata_dir / "4band_test.tif"


@pytest.fixture
def raster_4band_aws_s3(aws_s3_testdata_dir) -> MPath:
    """
    Fixture for remote file on S3 bucket.
    """
    return aws_s3_testdata_dir / "4band_test.tif"


@pytest.fixture
def empty_gpkg(testdata_dir) -> MPath:
    """Fixture for HTTP raster."""
    return testdata_dir / "empty.gpkg"


@pytest.fixture
def metadata_json(testdata_dir) -> MPath:
    """
    Fixture for metadata.json.
    """
    return testdata_dir / "cleantopo" / "metadata.json"


@pytest.fixture
def s3_metadata_json(minio_testdata_bucket) -> MPath:
    """
    Fixture for s3://mapchete-test/metadata.json.
    """
    return prepare_s3_testfile(minio_testdata_bucket, "metadata.json")


@pytest.fixture
def http_metadata_json(http_testdata_dir) -> MPath:
    """
    Fixture for http://localhost/cleantopo/metadata.json.
    """
    return http_testdata_dir / "cleantopo" / "metadata.json"


@pytest.fixture
def secure_http_metadata_json(secure_http_testdata_dir) -> MPath:
    """
    Fixture for http://localhost/cleantopo/metadata.json.
    """
    return secure_http_testdata_dir / "cleantopo" / "metadata.json"


@pytest.fixture
def vsicurl_metadata_json(http_testdata_dir) -> MPath:
    """
    Fixture for http://localhost/cleantopo/metadata.json.
    """
    return http_testdata_dir.new(
        f"/vsicurl/{http_testdata_dir / 'cleantopo' / 'metadata.json'}"
    )


@pytest.fixture
def old_style_metadata_json(testdata_dir) -> MPath:
    """
    Fixture for old_style_metadata.json.
    """
    return testdata_dir / "old_style_metadata.json"


@pytest.fixture
def old_geodetic_shape_metadata_json(testdata_dir) -> MPath:
    """
    Fixture for old_geodetic_shape_metadata.json.
    """
    return testdata_dir / "old_geodetic_shape_metadata.json"


@pytest.fixture
def driver_metadata_dict() -> dict:
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
def driver_output_params_dict() -> dict:
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
def landpoly(testdata_dir) -> MPath:
    """Fixture for landpoly.geojson."""
    return testdata_dir / "landpoly.geojson"


@pytest.fixture
def landpoly_s3(minio_testdata_bucket) -> MPath:
    """Fixture for landpoly.geojson."""
    return prepare_s3_testfile(minio_testdata_bucket, "landpoly.geojson")


@pytest.fixture
def landpoly_http(http_testdata_dir) -> MPath:
    """Fixture for landpoly.geojson."""
    return http_testdata_dir / "landpoly.geojson"


@pytest.fixture
def landpoly_secure_http(secure_http_testdata_dir) -> MPath:
    """Fixture for landpoly.geojson."""
    return secure_http_testdata_dir / "landpoly.geojson"


@pytest.fixture
def landpoly_3857(testdata_dir) -> MPath:
    """Fixture for landpoly_3857.geojson"""
    return testdata_dir / "landpoly_3857.geojson"


@pytest.fixture
def aoi_br_geojson(testdata_dir) -> MPath:
    """Fixture for aoi_br.geojson"""
    return testdata_dir / "aoi_br.geojson"


@pytest.fixture
def sample_geojson(testdata_dir) -> MPath:
    """Fixture for sample.geojson"""
    return testdata_dir / "sample.geojson"


@pytest.fixture
def geometrycollection() -> str:
    """Fixture for geometrycollection.geojson"""
    return wkt.loads(
        "GEOMETRYCOLLECTION (LINESTRING (-100.9423828125 78.75, -100.8984375 78.75), LINESTRING (-100.2392578125 78.75, -99.9755859375 78.75), POLYGON ((-101.25 78.9697265625, -101.25 79.013671875, -101.2060546875 79.013671875, -101.2060546875 78.9697265625, -100.986328125 78.9697265625, -100.986328125 78.92578125, -101.0302734375 78.92578125, -101.0302734375 78.8818359375, -101.07421875 78.8818359375, -101.1181640625 78.8818359375, -101.1181640625 78.837890625, -101.162109375 78.837890625, -101.2060546875 78.837890625, -101.2060546875 78.7939453125, -100.9423828125 78.7939453125, -100.9423828125 78.75, -101.25 78.75, -101.25 78.9697265625)), POLYGON ((-100.8984375 78.75, -100.8984375 78.7939453125, -100.5908203125 78.7939453125, -100.546875 78.7939453125, -100.546875 78.837890625, -100.3271484375 78.837890625, -100.3271484375 78.7939453125, -100.2392578125 78.7939453125, -100.2392578125 78.75, -100.8984375 78.75)))"
    )


@pytest.fixture
def cleantopo_br_tif(testdata_dir) -> MPath:
    """Fixture for cleantopo_br.tif"""
    return testdata_dir / "cleantopo_br.tif"


@pytest.fixture
def cleantopo_br_tif_s3(minio_testdata_bucket) -> MPath:
    """Fixture for cleantopo_br.tif"""
    return prepare_s3_testfile(minio_testdata_bucket, "cleantopo_br.tif")


@pytest.fixture
def dummy1_3857_tif(testdata_dir) -> MPath:
    """Fixture for dummy1_3857.tif"""
    return testdata_dir / "dummy1_3857.tif"


@pytest.fixture
def dummy1_tif(testdata_dir) -> MPath:
    """Fixture for dummy1.tif"""
    return testdata_dir / "dummy1.tif"


@pytest.fixture
def dummy2_tif(testdata_dir) -> MPath:
    """Fixture for dummy2.tif"""
    return testdata_dir / "dummy2.tif"


@pytest.fixture
def invalid_tif(testdata_dir) -> MPath:
    """Fixture for invalid.tif"""
    return testdata_dir / "invalid.tif"


@pytest.fixture
def gcps_tif(testdata_dir) -> MPath:
    """Fixture for gcps.tif"""
    return testdata_dir / "gcps.tif"


@pytest.fixture
def invalid_geojson(testdata_dir) -> MPath:
    """Fixture for invalid.geojson"""
    return testdata_dir / "invalid.geojson"


@pytest.fixture
def example_process_text(script_dir) -> List[str]:
    return (script_dir / "example_process.py").read_text().split("\n")


@pytest.fixture
def example_process_module() -> str:
    return "mapchete.processes.examples.example_process"


@pytest.fixture
def example_process_py(script_dir) -> MPath:
    return script_dir / "example_process.py"


@pytest.fixture
def execute_kwargs_py(testdata_dir) -> MPath:
    """Fixture for execute_kwargs.py"""
    return testdata_dir / "execute_kwargs.py"


@pytest.fixture
def write_rasterfile_tags_py(testdata_dir) -> MPath:
    """Fixture for write_rasterfile_tags.py"""
    return testdata_dir / "write_rasterfile_tags.py"


@pytest.fixture
def import_error_py(testdata_dir) -> MPath:
    """Fixture for import_error.py"""
    return testdata_dir / "import_error.py"


@pytest.fixture
def malformed_py(testdata_dir) -> MPath:
    """Fixture for malformed.py"""
    return testdata_dir / "malformed.py"


@pytest.fixture
def syntax_error_py(testdata_dir) -> MPath:
    """Fixture for syntax_error.py"""
    return testdata_dir / "syntax_error.py"


@pytest.fixture
def execute_params_error_py(testdata_dir) -> MPath:
    """Fixture for execute_params_error.py"""
    return testdata_dir / "execute_params_error.py"


@pytest.fixture
def process_error_py(testdata_dir) -> MPath:
    """Fixture for process_error.py"""
    return testdata_dir / "process_error.py"


@pytest.fixture
def output_error_py(testdata_dir) -> MPath:
    """Fixture for output_error.py"""
    return testdata_dir / "output_error.py"


@pytest.fixture
def old_style_process_py(testdata_dir) -> MPath:
    """Fixture for old_style_process.py"""
    return testdata_dir / "old_style_process.py"


@pytest.fixture
def custom_grid_json(testdata_dir) -> MPath:
    return testdata_dir / "custom_grid.json"


# example mapchete configurations
@pytest.fixture
def typed_raster_input(mp_tmpdir, testdata_dir):
    """Fixture for typed_raster_input.mapchete."""
    with ProcessFixture(
        testdata_dir / "typed_raster_input.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def custom_grid(mp_tmpdir, testdata_dir):
    """Fixture for custom_grid.mapchete."""
    with ProcessFixture(
        testdata_dir / "custom_grid.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def deprecated_params(mp_tmpdir, testdata_dir):
    """Fixture for deprecated_params.mapchete."""
    with ProcessFixture(
        testdata_dir / "deprecated_params.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def files_zooms(mp_tmpdir, testdata_dir):
    """Fixture for files_zooms.mapchete."""
    with ProcessFixture(
        testdata_dir / "files_zooms.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def file_groups(mp_tmpdir, testdata_dir):
    """Fixture for file_groups.mapchete."""
    with ProcessFixture(
        testdata_dir / "file_groups.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def overviews(mp_tmpdir, testdata_dir):
    """Fixture for overviews.mapchete."""
    with ProcessFixture(
        testdata_dir / "overviews.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def baselevels(mp_tmpdir, testdata_dir):
    """Fixture for baselevels.mapchete."""
    with ProcessFixture(
        testdata_dir / "baselevels.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def baselevels_output_buffer(mp_tmpdir, testdata_dir):
    """Fixture for baselevels_output_buffer.mapchete."""
    with ProcessFixture(
        testdata_dir / "baselevels_output_buffer.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def baselevels_custom_nodata(mp_tmpdir, testdata_dir):
    """Fixture for baselevels_custom_nodata.mapchete."""
    with ProcessFixture(
        testdata_dir / "baselevels_custom_nodata.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def mapchete_input(mp_tmpdir, testdata_dir):
    """Fixture for mapchete_input.mapchete."""
    with ProcessFixture(
        testdata_dir / "mapchete_input.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def dem_to_hillshade(mp_tmpdir, testdata_dir):
    """Fixture for dem_to_hillshade.mapchete."""
    with ProcessFixture(
        testdata_dir / "dem_to_hillshade.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def files_bounds(mp_tmpdir, testdata_dir):
    """Fixture for files_bounds.mapchete."""
    with ProcessFixture(
        testdata_dir / "files_bounds.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def example_mapchete(mp_tmpdir, script_dir):
    """Fixture for example.mapchete."""
    with ProcessFixture(
        script_dir / "example.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def env_storage_options_mapchete(mp_tmpdir, testdata_dir):
    """Fixture for env_storage_options.mapchete."""
    with ProcessFixture(
        testdata_dir / "env_storage_options.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def env_input_path_mapchete(mp_tmpdir, testdata_dir):
    """Fixture for env_input_path.mapchete."""
    with ProcessFixture(
        testdata_dir / "env_input_path.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def example_custom_process_mapchete(mp_tmpdir, testdata_dir):
    """Fixture for example.mapchete."""
    with ProcessFixture(
        testdata_dir / "example_custom_process.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def zoom_mapchete(mp_tmpdir, testdata_dir):
    """Fixture for zoom.mapchete."""
    with ProcessFixture(
        testdata_dir / "zoom.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def minmax_zoom(mp_tmpdir, testdata_dir):
    """Fixture for minmax_zoom.mapchete."""
    with ProcessFixture(
        testdata_dir / "minmax_zoom.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def cleantopo_tl(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_tl.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_tl.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_br.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_br.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_metatiling_1(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_br.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_br_metatiling_1.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def cleantopo_remote(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_remote.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_remote.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_tiledir(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_br_tiledir.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_br_tiledir.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_tiledir_mercator(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_br_tiledir_mercator.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_br_tiledir_mercator.mapchete",
        output_tempdir=mp_tmpdir,
    ) as example:
        yield example


@pytest.fixture
def cleantopo_br_mercator(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_br_mercator.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_br_mercator.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def cleantopo_read_lower_zoom(mp_tmpdir, testdata_dir):
    """Fixture for cleantopo_read_lower_zoom.mapchete."""
    with ProcessFixture(
        testdata_dir / "cleantopo_read_lower_zoom.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def geojson(mp_tmpdir, testdata_dir):
    """Fixture for geojson.mapchete."""
    with ProcessFixture(
        testdata_dir / "geojson.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def geojson_s3(mp_s3_tmpdir, testdata_dir):
    """Fixture for geojson.mapchete with updated output path."""
    with ProcessFixture(
        testdata_dir / "geojson.mapchete",
        output_tempdir=mp_s3_tmpdir,
    ) as example:
        yield example


@pytest.fixture
def flatgeobuf(mp_tmpdir, testdata_dir):
    """Fixture for flatgeobuf.mapchete."""
    with ProcessFixture(
        testdata_dir / "flatgeobuf.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def flatgeobuf_s3(mp_s3_tmpdir, testdata_dir):
    """Fixture for flatgeobuf.mapchete with updated output path."""
    with ProcessFixture(
        testdata_dir / "flatgeobuf.mapchete",
        output_tempdir=mp_s3_tmpdir,
    ) as example:
        yield example


@pytest.fixture
def geojson_tiledir(mp_tmpdir, testdata_dir):
    """Fixture for geojson_tiledir.mapchete."""
    with ProcessFixture(
        testdata_dir / "geojson_tiledir.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def process_module(mp_tmpdir, testdata_dir):
    """Fixture for process_module.mapchete"""
    with ProcessFixture(
        testdata_dir / "process_module.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def gtiff_s3(mp_s3_tmpdir, testdata_dir):
    """Fixture for gtiff_s3.mapchete."""
    with ProcessFixture(
        testdata_dir / "gtiff_s3.mapchete",
        output_tempdir=mp_s3_tmpdir,
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff(mp_tmpdir, testdata_dir):
    """Fixture for output_single_gtiff.mapchete."""
    with ProcessFixture(
        testdata_dir / "output_single_gtiff.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture(scope="function")
def output_s3_single_gtiff_error(mp_s3_tmpdir, testdata_dir):
    """Fixture for output_s3_single_gtiff_error.mapchete."""
    with ProcessFixture(
        testdata_dir / "output_s3_single_gtiff_error.mapchete",
        output_tempdir=mp_s3_tmpdir,
        output_suffix=".tif",
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff_s3(mp_s3_tmpdir, testdata_dir):
    """Fixture for output_single_gtiff.mapchete."""
    with ProcessFixture(
        testdata_dir / "output_single_gtiff.mapchete",
        output_tempdir=mp_s3_tmpdir,
        output_suffix=".tif",
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff_cog(mp_tmpdir, testdata_dir):
    """Fixture for output_single_gtiff_cog.mapchete."""
    with ProcessFixture(
        testdata_dir / "output_single_gtiff_cog.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def output_single_gtiff_cog_s3(mp_s3_tmpdir, testdata_dir):
    """Fixture for output_single_gtiff_cog.mapchete."""
    with ProcessFixture(
        testdata_dir / "output_single_gtiff_cog.mapchete",
        output_tempdir=mp_s3_tmpdir,
        output_suffix=".tif",
    ) as example:
        yield example


@pytest.fixture
def aoi_br(mp_tmpdir, testdata_dir):
    """Fixture for aoi_br.mapchete."""
    with ProcessFixture(
        testdata_dir / "aoi_br.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def preprocess_cache_raster_vector(mp_tmpdir, testdata_dir):
    """Fixture for preprocess_cache_raster_vector.mapchete."""
    with ProcessFixture(
        testdata_dir / "preprocess_cache_raster_vector.mapchete",
        output_tempdir=mp_tmpdir,
        inp_cache_tempdir=mp_tmpdir,
    ) as example:
        yield example


@pytest.fixture
def preprocess_cache_memory(mp_tmpdir, testdata_dir):
    """Fixture for preprocess_cache_memory.mapchete."""
    with ProcessFixture(
        testdata_dir / "preprocess_cache_memory.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def preprocess_cache_memory_single_file(mp_tmpdir, testdata_dir):
    """Fixture for preprocess_cache_memory_single_file.mapchete."""
    with ProcessFixture(
        testdata_dir / "preprocess_cache_memory_single_file.mapchete",
        output_tempdir=mp_tmpdir,
    ) as example:
        yield example


@pytest.fixture
def custom_grid_points(mp_tmpdir, testdata_dir):
    """Fixture for custom_grid_points.mapchete."""
    with ProcessFixture(
        testdata_dir / "custom_grid_points.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def stac_metadata(mp_tmpdir, testdata_dir):
    """Fixture for stac_metadata.mapchete."""
    with ProcessFixture(
        testdata_dir / "stac_metadata.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def red_raster(mp_tmpdir, testdata_dir):
    """Fixture for red_raster.mapchete."""
    with ProcessFixture(
        testdata_dir / "red_raster.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def green_raster(mp_tmpdir, testdata_dir):
    """Fixture for green_raster.mapchete."""
    with ProcessFixture(
        testdata_dir / "green_raster.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def dask_specs(mp_tmpdir, testdata_dir):
    """Fixture for dask_specs.mapchete."""
    with ProcessFixture(
        testdata_dir / "dask_specs.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def tile_path_schema(mp_tmpdir, testdata_dir):
    """Fixture for tile_path_schema.mapchete."""
    with ProcessFixture(
        testdata_dir / "tile_path_schema.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def s3_example_tile():
    """Example tile for fixture."""
    return (5, 15, 32)


@pytest.fixture(scope="package")
def sequential_executor():
    """SequentialExecutor()"""
    with SequentialExecutor() as executor:
        yield executor


@pytest.fixture(scope="package")
def dask_executor():
    """DaskExecutor()"""
    with DaskExecutor() as executor:
        yield executor


@pytest.fixture(scope="package")
def processes_executor():
    """ConcurrentFuturesExecutor()"""
    with ConcurrentFuturesExecutor(concurrency="processes") as executor:
        yield executor


@pytest.fixture(scope="package")
def threads_executor():
    """ConcurrentFuturesExecutor()"""
    with ConcurrentFuturesExecutor(concurrency="threads") as executor:
        yield executor


@pytest.fixture
def example_clip(mp_tmpdir, examples_dir):
    """Fixture for examples/clip/clip.mapchete."""
    with ProcessFixture(
        examples_dir / "clip/clip.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def example_contours(mp_tmpdir, examples_dir):
    """Fixture for examples/contours/contours.mapchete."""
    with ProcessFixture(
        examples_dir / "contours/contours.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def example_custom_grid(mp_tmpdir, examples_dir):
    """Fixture for examples/custom_grid/custom_grid.mapchete."""
    with ProcessFixture(
        examples_dir / "custom_grid/custom_grid.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def example_file_groups(mp_tmpdir, examples_dir):
    """Fixture for examples/file_groups/file_groups.mapchete."""
    with ProcessFixture(
        examples_dir / "file_groups/file_groups.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example


@pytest.fixture
def example_hillshade(mp_tmpdir, examples_dir):
    """Fixture for examples/hillshade/hillshade.mapchete."""
    with ProcessFixture(
        examples_dir / "hillshade/hillshade.mapchete", output_tempdir=mp_tmpdir
    ) as example:
        yield example
