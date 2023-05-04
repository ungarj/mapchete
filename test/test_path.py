import pytest

from mapchete.io import MPath


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


def test_s3_path():
    path = MPath("s3://foo/bar.json")
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_http_path():
    path = MPath("http://foo/bar.json")
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_https_path():
    path = MPath("https://foo/bar.json")
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_vsicurl_path():
    path = MPath("/vsicurl/https://foo/bar.json")
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
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
def test_remote_dirpath(path_str):
    path = MPath(path_str)
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar"
    assert path.stem == "bar"
    assert path.suffix == ""


@pytest.mark.parametrize("path_str", ["/foo/bar", "/foo/bar/" "foo/bar", "foo/bar/"])
def test_local_dirpath(path_str):
    path = MPath(path_str)
    assert not path.exists()
    assert not path.is_remote()
    assert path.name == "bar"
    assert path.stem == "bar"
    assert path.suffix == ""


def test_makedirs_filepath(mp_tmpdir):
    path = MPath(mp_tmpdir).joinpath("path_mkdir_test", "file")
    path.makedirs()
    assert path.parent.exists()
    assert not path.exists()


def test_makedirs_dirpath(mp_tmpdir):
    path = MPath(mp_tmpdir).joinpath("path_mkdir_test", "directory/")
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


def test_ls(testdata_dir):
    for path in testdata_dir.ls():
        assert isinstance(path, MPath)
    for path in testdata_dir.ls(detail=True):
        assert isinstance(path.get("name"), MPath)
