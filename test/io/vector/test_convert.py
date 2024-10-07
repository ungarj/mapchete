import pytest
from mapchete.io.vector.convert import convert_vector
from mapchete.io.vector.open import fiona_open


def test_convert_vector_copy(aoi_br_geojson, tmpdir):
    out = tmpdir / "copied.geojson"

    # copy
    convert_vector(aoi_br_geojson, out)
    with fiona_open(str(out)) as src:
        assert list(iter(src))

    # raise error if output exists
    with pytest.raises(IOError):
        convert_vector(aoi_br_geojson, out, exists_ok=False)

    # do nothing if output exists
    convert_vector(aoi_br_geojson, out)
    with fiona_open(str(out)) as src:
        assert list(iter(src))


def test_convert_vector_overwrite(aoi_br_geojson, tmpdir):
    out = tmpdir / "copied.geojson"

    # write an invalid file
    with open(out, "w") as dst:
        dst.write("invalid")

    # overwrite
    convert_vector(aoi_br_geojson, out, overwrite=True)
    with fiona_open(str(out)) as src:
        assert list(iter(src))


def test_convert_vector_other_format_copy(aoi_br_geojson, tmpdir):
    out = tmpdir / "copied.gpkg"

    convert_vector(aoi_br_geojson, out, driver="GPKG")
    with fiona_open(str(out)) as src:
        assert list(iter(src))

    # raise error if output exists
    with pytest.raises(IOError):
        convert_vector(aoi_br_geojson, out, exists_ok=False)


def test_convert_vector_other_format_overwrite(aoi_br_geojson, tmpdir):
    out = tmpdir / "copied.gkpk"

    # write an invalid file
    with open(out, "w") as dst:
        dst.write("invalid")

    # overwrite
    convert_vector(aoi_br_geojson, out, driver="GPKG", overwrite=True)
    with fiona_open(str(out)) as src:
        assert list(iter(src))
