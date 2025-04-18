import pytest

from mapchete.io.raster.convert import convert_raster
from mapchete.io.raster.open import rasterio_open


def test_convert_raster_copy(cleantopo_br_tif, mp_tmpdir):
    out = mp_tmpdir / "copied.tif"

    # copy
    convert_raster(cleantopo_br_tif, out)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()

    # raise error if output exists
    with pytest.raises(IOError):
        convert_raster(cleantopo_br_tif, out, exists_ok=False)

    # do nothing if output exists
    convert_raster(cleantopo_br_tif, out)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()


@pytest.mark.integration
def test_convert_raster_copy_s3(cleantopo_br_tif_s3, mp_s3_tmpdir):
    out = mp_s3_tmpdir / "copied.tif"

    # copy
    convert_raster(cleantopo_br_tif_s3, out)
    with out.rio_env():
        with rasterio_open(out) as src:
            assert not src.read(masked=True).mask.all()

    # raise error if output exists
    with pytest.raises(IOError):
        convert_raster(cleantopo_br_tif_s3, out, exists_ok=False)

    # do nothing if output exists
    convert_raster(cleantopo_br_tif_s3, out)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()


def test_convert_raster_overwrite(cleantopo_br_tif, mp_tmpdir):
    out = mp_tmpdir / "copied.tif"

    # write an invalid file
    with out.open("w") as dst:
        dst.write("invalid")

    # overwrite
    convert_raster(cleantopo_br_tif, out, overwrite=True)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()


@pytest.mark.integration
def test_convert_raster_overwrite_s3(cleantopo_br_tif_s3, mp_s3_tmpdir):
    out = mp_s3_tmpdir / "copied.tif"

    # write an invalid file
    with out.open("w") as dst:
        dst.write("invalid")

    # overwrite
    convert_raster(cleantopo_br_tif_s3, out, overwrite=True)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()


def test_convert_raster_other_format_copy(cleantopo_br_tif, mp_tmpdir):
    out = mp_tmpdir / "copied.jp2"

    convert_raster(cleantopo_br_tif, out, driver="JP2OpenJPEG")
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()

    # raise error if output exists
    with pytest.raises(IOError):
        convert_raster(cleantopo_br_tif, out, exists_ok=False)


@pytest.mark.integration
def test_convert_raster_other_format_copy_s3(cleantopo_br_tif_s3, mp_s3_tmpdir):
    out = mp_s3_tmpdir / "copied.jp2"

    convert_raster(cleantopo_br_tif_s3, out, driver="JP2OpenJPEG")
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()

    # raise error if output exists
    with pytest.raises(IOError):
        convert_raster(cleantopo_br_tif_s3, out, exists_ok=False)


def test_convert_raster_other_format_overwrite(cleantopo_br_tif, mp_tmpdir):
    out = mp_tmpdir / "copied.jp2"

    # write an invalid file
    with out.open("w") as dst:
        dst.write("invalid")

    # overwrite
    convert_raster(cleantopo_br_tif, out, driver="JP2OpenJPEG", overwrite=True)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()


@pytest.mark.integration
def test_convert_raster_other_format_overwrite_s3(cleantopo_br_tif_s3, mp_s3_tmpdir):
    out = mp_s3_tmpdir / "copied.jp2"

    # write an invalid file
    with out.open("w") as dst:
        dst.write("invalid")

    # overwrite
    convert_raster(cleantopo_br_tif_s3, out, driver="JP2OpenJPEG", overwrite=True)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()
