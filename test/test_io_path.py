import pytest

from mapchete.io import path


def test_parse_path():
    test_paths = [
        {
            "uri": "/vsicurl/https://ungarj.github.io/non-existing.tif",
            "name": "/vsicurl/https://ungarj.github.io/non-existing.tif",
            "path": "https://ungarj.github.io/non-existing.tif",
            "gdal_prefix": "/vsicurl/",
            "vsi_path": "/vsicurl/https://ungarj.github.io/non-existing.tif",
            "is_remote": True
        },
        {
            "uri": "https://ungarj.github.io/non-existing.tif",
            "name": "https://ungarj.github.io/non-existing.tif",
            "path": "https://ungarj.github.io/non-existing.tif",
            "gdal_prefix": "/vsicurl/",
            "vsi_path": "/vsicurl/https://ungarj.github.io/non-existing.tif",
            "is_remote": True
        },
        {
            "uri": "/vsis3/mapchete-test/non-existing.tif",
            "name": "/vsis3/mapchete-test/non-existing.tif",
            "path": "s3://mapchete-test/non-existing.tif",
            "gdal_prefix": "/vsis3/",
            "vsi_path": "/vsis3/mapchete-test/non-existing.tif",
            "is_remote": True
        },
        {
            "uri": "s3://mapchete-test/non-existing.tif",
            "name": "s3://mapchete-test/non-existing.tif",
            "path": "s3://mapchete-test/non-existing.tif",
            "gdal_prefix": "/vsis3/",
            "vsi_path": "/vsis3/mapchete-test/non-existing.tif",
            "is_remote": True
        },
        {
            "uri": "/some-local-path/file",
            "name": "/some-local-path/file",
            "path": "/some-local-path/file",
            "gdal_prefix": None,
            "vsi_path": "/some-local-path/file",
            "is_remote": False
        },
    ]
    for a in test_paths:
        parsed = path.Path(a["uri"])
        assert parsed.name == a["name"]
        assert parsed.gdal_prefix == a["gdal_prefix"]
        assert parsed.path == a["path"]
        assert parsed.vsi_path == a["vsi_path"]
        assert parsed.is_remote is a["is_remote"]
        assert parsed.exists() is False

    # not yet implemented remote url
    with pytest.raises(ValueError):
        path.Path("gs://mapchete-test/non-existing.tif")


