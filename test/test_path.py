import pytest

from mapchete.io import Path


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
    path = Path(path_str)
    assert path.fs


def test_s3_path():
    path = Path("s3://foo/bar.json")
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_http_path():
    path = Path("http://foo/bar.json")
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_https_path():
    path = Path("https://foo/bar.json")
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_vsicurl_path():
    path = Path("/vsicurl/https://foo/bar.json")
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_absolute_path():
    path = Path("/foo/bar.json")
    assert not path.exists()
    assert not path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


def test_relative_path():
    path = Path("foo/bar.json")
    assert not path.exists()
    assert not path.is_remote()
    assert path.name == "bar.json"
    assert path.stem == "bar"
    assert path.suffix == ".json"


@pytest.mark.parametrize("path_str", ["s3://foo/bar", "s3://foo/bar/"])
def test_s3_dirpath(path_str):
    path = Path(path_str)
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
    path = Path(path_str)
    assert not path.exists()
    assert path.is_remote()
    assert path.name == "bar"
    assert path.stem == "bar"
    assert path.suffix == ""


@pytest.mark.parametrize("path_str", ["/foo/bar", "/foo/bar/" "foo/bar", "foo/bar/"])
def test_local_dirpath(path_str):
    path = Path(path_str)
    assert not path.exists()
    assert not path.is_remote()
    assert path.name == "bar"
    assert path.stem == "bar"
    assert path.suffix == ""
