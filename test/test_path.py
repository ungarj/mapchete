import pickle
from datetime import datetime

import pytest
from pytest_lazyfixture import lazy_fixture

from mapchete.config import get_hash
from mapchete.io.raster.referenced_raster import ReferencedRaster
from mapchete.path import MPath, batch_sort_property


@pytest.mark.parametrize(
    "path_str",
    [
        "s3://foo/bar.json",
        "http://foo/bar.json",
        "https://foo/bar.json",
        "/vsicurl/https://foo/bar.json",
        "/foo/bar.json",
        "foo/bar.json",
    ],
)
def test_parse(path_str):
    path = MPath(path_str)
    assert path.fs


def test_parse_error():
    with pytest.raises(TypeError):
        MPath(None)  # type: ignore


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_metadata_json"),
        lazy_fixture("secure_http_metadata_json"),
        lazy_fixture("s3_metadata_json"),
        lazy_fixture("vsicurl_metadata_json"),
    ],
)
def test_remote_existing_paths(path):
    assert path.exists()
    assert path.is_remote()
    assert path.name == "metadata.json"
    assert path.stem == "metadata"
    assert path.suffix == ".json"


def test_absolute_path():
    path = MPath("/foo/bar.json")
    assert not path.exists()
    assert not path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_relative_path():
    path = MPath("foo/bar.json")
    assert not path.exists()
    assert not path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_absolute_from_relative_path():
    path = MPath("foo/bar.json")
    abspath = path.absolute_path()
    assert abspath.is_absolute()
    assert abspath.endswith("foo/bar.json")


@pytest.mark.parametrize("path_str", ["s3://foo/bar", "s3://foo/bar/"])
def test_s3_dirpath(path_str):
    path = MPath(path_str)
    assert path.is_remote()
    assert path.name == "bar"
    assert path.stem == "bar"
    assert path.suffix == ""


@pytest.mark.parametrize(
    "path_str",
    [
        "http://foo/bar",
        "http://foo/bar/" "https://foo/bar",
        "https://foo/bar/" "/vsicurl/https://foo/bar",
        "/vsicurl/https://foo/bar/",
    ],
)
def test_parse_remote_dirpath(path_str):
    path = MPath(path_str)
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar"
    assert path.stem == "bar"
    assert path.suffix == ""


@pytest.mark.parametrize("path_str", ["/foo/bar", "/foo/bar/" "foo/bar", "foo/bar/"])
def test_parse_local_dirpath(path_str):
    path = MPath(path_str)
    assert not path.exists()
    assert not path.is_remote()
    assert path.name == "bar"
    assert path.stem == "bar"
    assert path.suffix == ""


def test_makedirs_filepath(mp_tmpdir):
    path = MPath(mp_tmpdir) / "path_mkdir_test" / "file.ext"
    path.parent.makedirs()
    assert path.parent.exists()
    assert not path.exists()


def test_makedirs_dirpath(mp_tmpdir):
    path = MPath(mp_tmpdir) / "path_mkdir_test" / "directory/"
    path.makedirs()
    assert path.parent.exists()
    assert path.exists()


def test_startswith():
    path = MPath("foo.tif")
    assert path.startswith("foo")


def test_endswith():
    path = MPath("foo.tif")
    assert path.endswith("tif")


def test_relative_path_func():
    path = MPath("bar")
    assert isinstance(path.relative_path("/foo/bar"), MPath)


def test_without_suffix():
    path = MPath("foo/bar.tif")
    assert str(path.without_suffix()) == "foo/bar"


def test_with_suffix():
    path = MPath("foo/bar.tif")
    assert str(path.with_suffix("jpg")) == "foo/bar.jpg"


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("testdata_dir"),
    ],
)
def test_ls(path):
    dir_is_remote = path.is_remote()
    assert path.ls()
    for p in path.ls():
        assert isinstance(p, MPath)
        assert p.is_remote() == dir_is_remote
        assert p._info


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_testdata_dir"),
        lazy_fixture("secure_http_testdata_dir"),
        lazy_fixture("s3_testdata_dir"),
    ],
)
def test_ls_remote(path):
    test_ls(path)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("testdata_dir"),
    ],
)
@pytest.mark.parametrize("absolute_paths", [True, False])
def test_walk(path, absolute_paths):
    dir_is_remote = path.is_remote()
    assert list(path.ls())
    subdirs_available = False
    files_available = False
    for root, subdirs, files in path.walk(absolute_paths=absolute_paths):
        assert isinstance(root, MPath)
        if absolute_paths:
            assert root.is_remote() == dir_is_remote
        assert isinstance(subdirs, list)
        assert isinstance(files, list)
        for subdir in subdirs:
            subdirs_available = True
            assert isinstance(subdir, MPath)
            assert subdir._info is not None
        for file in files:
            files_available = True
            assert isinstance(file, MPath)
            assert file._info is not None
    assert subdirs_available
    assert files_available


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_testdata_dir"),
        lazy_fixture("secure_http_testdata_dir"),
        lazy_fixture("s3_testdata_dir"),
    ],
)
@pytest.mark.parametrize("absolute_paths", [True, False])
def test_walk_remote(path, absolute_paths):
    test_walk(path, absolute_paths=absolute_paths)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("testdata_dir"),
    ],
)
@pytest.mark.parametrize("items_per_page", [1, 10])
def test_paginate(path, items_per_page):
    paginated = path.paginate(items_per_page=items_per_page)
    assert paginated
    for page in paginated:
        assert len(page)
        assert len(page) <= items_per_page
        for item in page:
            assert item._info is not None
            if "s3" in path.protocols:
                assert item.startswith("s3://")
            assert isinstance(item, MPath)
            assert not item.is_directory()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_testdata_dir"),
        lazy_fixture("secure_http_testdata_dir"),
        lazy_fixture("s3_testdata_dir"),
    ],
)
@pytest.mark.parametrize("items_per_page", [1, 10])
def test_paginate_remote(path, items_per_page):
    test_paginate(path, items_per_page=items_per_page)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("testdata_dir"),
    ],
)
def test_is_directory(path):
    assert path.is_directory()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_testdata_dir"),
        lazy_fixture("secure_http_testdata_dir"),
        lazy_fixture("s3_testdata_dir"),
    ],
)
def test_is_dir_remote(path):
    test_is_directory(path)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("metadata_json"),
    ],
)
def test_io_read(path):
    assert path.exists()

    with path.open() as src:
        assert src.read()

    assert path.read_text()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_metadata_json"),
        lazy_fixture("secure_http_metadata_json"),
        lazy_fixture("s3_metadata_json"),
    ],
)
def test_io_read_remote(path):
    test_io_read(path)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("metadata_json"),
    ],
)
def test_size(path):
    assert path.size()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_metadata_json"),
        lazy_fixture("secure_http_metadata_json"),
        lazy_fixture("s3_metadata_json"),
    ],
)
def test_size_remote(path):
    test_size(path)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("metadata_json"),
    ],
)
def test_pretty_size(path):
    assert path.pretty_size()


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("metadata_json"),
    ],
)
def test_last_modified(path):
    assert isinstance(path.last_modified(), datetime)


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("http_metadata_json"),
        lazy_fixture("secure_http_metadata_json"),
        lazy_fixture("s3_metadata_json"),
    ],
)
def test_last_modified_remote(path):
    if "https" in path.protocols:
        with pytest.raises(ValueError):
            test_last_modified(path)
    else:
        test_last_modified(path)


@pytest.mark.integration
def test_io_s3(mp_s3_tmpdir):
    testfile = mp_s3_tmpdir / "bar.txt"
    assert not testfile.exists()
    with testfile.open("w") as dst:
        dst.write("test text")
    assert testfile.exists()
    with testfile.open("r") as src:
        assert src.read() == "test text"


@pytest.mark.parametrize(
    "path_str",
    [
        "s3://some-bucket/file.geojson",
        "s3://some-bucket/file.tif",
        "http://some-bucket/file.geojson",
        "http://some-bucket/file.tif",
        "https://some-bucket/file.geojson",
        "https://some-bucket/file.tif",
    ],
)
def test_gdal_env_params(path_str):
    path = MPath(path_str)

    # default
    remote_extensions = path.gdal_env_params()[
        "CPL_VSIL_CURL_ALLOWED_EXTENSIONS"
    ].split(", ")
    assert path.suffix in remote_extensions

    # add custom extensions
    remote_extensions = path.gdal_env_params(
        allowed_remote_extensions=[".foo", ".bar"]
    )["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"].split(", ")
    assert path.suffix in remote_extensions


def test_gdal_env_params_vrt():
    path = MPath("https://some-bucket/file.vrt")
    assert "CPL_VSIL_CURL_ALLOWED_EXTENSIONS" not in path.gdal_env_params()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path_str",
    [
        "http://localhost/open/cleantopo/1/",
        "http://localhost/open/cleantopo/1",
    ],
)
def test_http_ls(path_str):
    path = MPath(path_str)
    assert path.ls()


@pytest.mark.integration
def test_secure_http_tiledir(secure_http_tiledir):
    assert secure_http_tiledir.exists()
    assert secure_http_tiledir.ls()


@pytest.mark.integration
def test_secure_http_raster(secure_http_raster):
    assert secure_http_raster.exists()


@pytest.mark.parametrize("obj", [MPath("/foo/bar"), dict(key=MPath("/foo/bar"))])
def test_get_hash(obj):
    assert get_hash(obj)


@pytest.mark.integration
def test_dict_representation(secure_http_metadata_json):
    assert secure_http_metadata_json.exists()

    dict_repr = secure_http_metadata_json.to_dict()
    assert "path" in dict_repr
    assert "storage_options" in dict_repr

    restored_path = MPath.from_dict(dict_repr)
    assert restored_path.exists()

    assert secure_http_metadata_json == restored_path


def test_from_dict_error():
    with pytest.raises(ValueError):
        MPath.from_dict(dict(storage_options=None))


@pytest.mark.integration
def test_transfer_fs(secure_http_testdata_dir):
    path = MPath(
        str(secure_http_testdata_dir) + "aoi_br.geojson", fs=secure_http_testdata_dir.fs
    )
    assert path.exists()


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band"),
    ],
)
def test_pickle_path(path):
    packed = pickle.dumps(path)
    unpacked = pickle.loads(packed)
    assert path == unpacked


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
    ],
)
def test_pickle_path_remote(path):
    test_pickle_path(path)


@pytest.mark.aws_s3
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_aws_s3"),
    ],
)
def test_pickle_path_aws_s3(path):
    test_pickle_path(path)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band"),
    ],
)
def test_pickle_fs(path):
    assert pickle.loads(pickle.dumps(path.fs))


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
        lazy_fixture("raster_4band_aws_s3"),
    ],
)
def test_pickle_fs_remote(path):
    test_pickle_fs(path)


@pytest.mark.aws_s3
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_aws_s3"),
    ],
)
def test_pickle_fs_aws_s3(path):
    test_pickle_fs(path)


def test_batch_sort_property():
    assert batch_sort_property("{zoom}/{row}/{col}.{extension}") == "row"
    assert batch_sort_property("{zoom}/{col}/{row}.{extension}") == "col"


def test_without_protocol_s3():
    path = MPath("s3://foo/bar")
    assert path.without_protocol() == "foo/bar"


def test_without_protocol_http():
    path = MPath("http://foo/bar")
    assert path.without_protocol() == "foo/bar"


def test_without_protocol_relative():
    path = MPath("foo/bar")
    assert path.without_protocol() == "foo/bar"


def test_with_protocol_s3():
    path = MPath("s3://foo/bar")
    assert path.with_protocol("https") == "https://foo/bar"


def test_with_protocol_http():
    path = MPath("http://foo/bar")
    assert path.with_protocol("https") == "https://foo/bar"


def test_with_protocol_relative():
    path = MPath("foo/bar")
    assert path.with_protocol("https") == "https://foo/bar"


def test_s3_region_name():
    path = MPath("s3://foo", storage_options=dict(region_name="bar"))
    # trigger a rewrite of storage options to fit with S3FS
    path.fs
    # not an ideal way to test this, but the storage_options are passed on to S3FS that way
    assert path.storage_options.get("client_kwargs", {}).get("region_name") == "bar"


@pytest.mark.integration
@pytest.mark.parametrize(
    "src_path",
    [
        lazy_fixture("raster_4band"),
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
        lazy_fixture("raster_4band_aws_s3"),
    ],
)
@pytest.mark.parametrize(
    "dst_dir",
    [
        lazy_fixture("mp_s3_tmpdir"),
        lazy_fixture("mp_tmpdir"),
    ],
)
def test_cp(src_path: MPath, dst_dir: MPath):
    tempdir = dst_dir / "temp/"
    dst_path = tempdir / src_path.name
    try:
        src_path.cp(dst_path)
        assert dst_path.exists()
        assert not ReferencedRaster.from_file(dst_path).masked_array().mask.all()
    finally:
        dst_path.rm(ignore_errors=True)


@pytest.mark.integration
@pytest.mark.parametrize(
    "src_path",
    [
        lazy_fixture("raster_4band"),
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
        lazy_fixture("raster_4band_aws_s3"),
    ],
)
def test_checksum(src_path: MPath):
    assert (
        src_path.checksum()
        == "fff06260d08965a898021b9513dc9226f6c3b964d755734357603c21fb2359ad"
    )
