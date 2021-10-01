"""Test GeoTIFF as process output."""

import numpy as np
import numpy.ma as ma
import os
import pytest
import rasterio
from rasterio.io import MemoryFile
from rio_cogeo.cogeo import cog_validate
import shutil
from tilematrix import Bounds
import warnings

import mapchete
from mapchete.errors import MapcheteConfigError
from mapchete.io import path_exists
from mapchete.formats.default import gtiff
from mapchete.tile import BufferedTilePyramid


def _gdal_cog_available():
    with rasterio.Env() as env:
        return "COG" in env.drivers()


GDAL_COG_AVAILABLE = _gdal_cog_available()


def test_output_data(mp_tmpdir):
    """Check GeoTIFF as output data."""
    output_params = dict(
        grid="geodetic",
        format="GeoTIFF",
        path=mp_tmpdir,
        pixelbuffer=0,
        metatiling=1,
        bands=1,
        dtype="int16",
        delimiters=dict(
            bounds=Bounds(-180.0, -90.0, 180.0, 90.0),
            effective_bounds=Bounds(-180.439453125, -90.0, 180.439453125, 90.0),
            zoom=[5],
            process_bounds=Bounds(-180.0, -90.0, 180.0, 90.0),
        ),
    )
    output = gtiff.OutputDataWriter(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".tif"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(*[mp_tmpdir, "5", "5", "5" + ".tif"])
    # prepare_path
    try:
        temp_dir = os.path.join(*[mp_tmpdir, "5", "5"])
        output.prepare_path(tile)
        assert os.path.isdir(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # profile
    assert isinstance(output.profile(tile), dict)
    # write
    try:
        data = np.ones((1,) + tile.shape) * 128
        output.write(tile, data)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.any()
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)
    # read empty
    try:
        data = output.read(tile)
        assert isinstance(data, np.ndarray)
        assert data[0].mask.all()
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)
    # empty
    try:
        empty = output.empty(tile)
        assert isinstance(empty, ma.MaskedArray)
        assert not empty.any()
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)
    # deflate with predictor
    try:
        # with pytest.deprecated_call():
        output_params.update(compress="deflate", predictor=2)
        output = gtiff.OutputDataWriter(output_params)
        assert output.profile(tile)["compress"] == "deflate"
        assert output.profile(tile)["predictor"] == 2
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)
    # using deprecated "compression" property
    try:
        with pytest.deprecated_call():
            output_params.update(compression="deflate", predictor=2)
            output = gtiff.OutputDataWriter(output_params)
            assert output.profile(tile)["compress"] == "deflate"
            assert output.profile(tile)["predictor"] == 2
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)


def test_for_web(client, mp_tmpdir):
    """Send GTiff via flask."""
    tile_base_url = "/wmts_simple/1.0.0/cleantopo_br/default/WGS84/"
    for url in ["/"]:
        response = client.get(url)
        assert response.status_code == 200
    for url in [
        tile_base_url + "5/30/62.tif",
        tile_base_url + "5/30/63.tif",
        tile_base_url + "5/31/62.tif",
        tile_base_url + "5/31/63.tif",
    ]:
        response = client.get(url)
        assert response.status_code == 200
        img = response.data
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with MemoryFile(img) as memfile:
                with memfile.open() as dataset:
                    assert dataset.read().any()


def test_input_data(mp_tmpdir, cleantopo_br):
    """Check GeoTIFF proces output as input data."""
    with mapchete.open(cleantopo_br.path) as mp:
        tp = BufferedTilePyramid("geodetic")
        # TODO tile with existing but empty data
        tile = tp.tile(5, 5, 5)
        output_params = dict(
            grid="geodetic",
            format="GeoTIFF",
            path=mp_tmpdir,
            pixelbuffer=0,
            metatiling=1,
            bands=2,
            dtype="int16",
            delimiters=dict(
                bounds=Bounds(-180.0, -90.0, 180.0, 90.0),
                effective_bounds=Bounds(-180.439453125, -90.0, 180.439453125, 90.0),
                zoom=[5],
                process_bounds=Bounds(-180.0, -90.0, 180.0, 90.0),
            ),
        )
        output = gtiff.OutputDataWriter(output_params)
        with output.open(tile, mp) as input_tile:
            for data in [
                input_tile.read(),
                input_tile.read(1),
                input_tile.read([1]),
                # TODO assert valid indexes are passed input_tile.read([1, 2])
            ]:
                assert isinstance(data, ma.masked_array)
                assert input_tile.is_empty()
        # open without resampling
        with output.open(tile, mp) as input_tile:
            pass


def test_write_geotiff_tags(mp_tmpdir, cleantopo_br, write_rasterfile_tags_py):
    """Pass on metadata tags from user process to rasterio."""
    conf = dict(**cleantopo_br.dict)
    conf.update(process=write_rasterfile_tags_py)
    with mapchete.open(conf) as mp:
        for tile in mp.get_process_tiles():
            data, tags = mp.execute(tile)
            assert data.any()
            assert isinstance(tags, dict)
            mp.write(process_tile=tile, data=(data, tags))
            # read data
            out_path = mp.config.output.get_path(tile)
            with rasterio.open(out_path) as src:
                assert "filewide_tag" in src.tags()
                assert src.tags()["filewide_tag"] == "value"
                assert "band_tag" in src.tags(1)
                assert src.tags(1)["band_tag"] == "True"


@pytest.mark.remote
def test_s3_write_output_data(gtiff_s3, s3_example_tile, mp_s3_tmpdir):
    """Write and read output."""
    with mapchete.open(gtiff_s3.dict) as mp:
        process_tile = mp.config.process_pyramid.tile(*s3_example_tile)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(tile=process_tile.id)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()


def test_output_single_gtiff(output_single_gtiff):
    tile_id = (5, 3, 7)
    with mapchete.open(output_single_gtiff.path) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(multi=2)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()
        # write empty array
        data = ma.masked_array(
            data=np.ones(process_tile.shape),
            mask=np.ones(process_tile.shape),
        )
        mp.config.output.write(process_tile, data)
    assert os.path.isfile(mp.config.output.path)

    # error on existing file
    with pytest.raises(MapcheteConfigError):
        mapchete.open(output_single_gtiff.path)

    # overwrite existing file
    with mapchete.open(output_single_gtiff.path, mode="overwrite") as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(tile=process_tile.id)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        assert mp.config.output.tiles_exist(
            output_tile=mp.config.output_pyramid.intersecting(process_tile)[0]
        )
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()


def test_output_single_gtiff_errors(output_single_gtiff):
    # single gtiff does not work on multiple zoom levels
    with pytest.raises(ValueError):
        mapchete.open(dict(output_single_gtiff.dict, zoom_levels=[5, 6]))

    # provide either process_tile or output_tile
    with mapchete.open(output_single_gtiff.path) as mp:
        tile = mp.config.process_pyramid.tile(5, 3, 7)
        with pytest.raises(ValueError):
            mp.config.output.tiles_exist(process_tile=tile, output_tile=tile)


def test_output_single_gtiff_pixelbuffer(output_single_gtiff):
    tile_id = (5, 3, 7)
    with mapchete.open(
        dict(
            output_single_gtiff.dict,
            output=dict(output_single_gtiff.dict["output"], pixelbuffer=5),
        ),
    ) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(tile=process_tile.id)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()


def test_output_single_gtiff_compression(output_single_gtiff):
    tile_id = (5, 3, 7)
    with mapchete.open(
        dict(
            output_single_gtiff.dict,
            output=dict(output_single_gtiff.dict["output"], compress="deflate"),
        ),
    ) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        assert "compress" in mp.config.output.profile()
        assert mp.config.output.profile()["compress"] == "deflate"
        mp.batch_process(tile=process_tile.id)

    with rasterio.open(mp.config.output.path) as src:
        assert src.profile["compress"] == "deflate"


def test_output_single_gtiff_overviews(output_single_gtiff):
    # overwrite existing file
    with mapchete.open(
        dict(
            output_single_gtiff.dict,
            output=dict(
                output_single_gtiff.dict["output"],
                overviews=True,
                overviews_resampling="bilinear",
            ),
        ),
    ) as mp:
        tile_id = (5, 3, 7)
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        mp.batch_process(tile=process_tile.id)

    with rasterio.open(mp.config.output.path) as src:
        assert src.overviews(1)
        assert src.tags().get("OVR_RESAMPLING_ALG").lower() == "bilinear"
        for o in [1, 2, 4, 8]:
            a = src.read(
                masked=True, out_shape=(1, int(src.height / o), int(src.width / o))
            )
            assert not a.mask.all()


@pytest.mark.remote
def test_output_single_gtiff_s3(output_single_gtiff, mp_s3_tmpdir):
    tile_id = (5, 3, 7)
    with mapchete.open(
        dict(
            output_single_gtiff.dict,
            output=dict(
                output_single_gtiff.dict["output"],
                path=os.path.join(mp_s3_tmpdir, "temp.tif"),
            ),
        )
    ) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(multi=2)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()
        # write empty array
        data = ma.masked_array(
            data=np.ones(process_tile.shape),
            mask=np.ones(process_tile.shape),
        )
        mp.config.output.write(process_tile, data)
    assert path_exists(mp.config.output.path)


@pytest.mark.remote
def test_output_single_gtiff_s3_tempfile(output_single_gtiff, mp_s3_tmpdir):
    tile_id = (5, 3, 7)
    with mapchete.open(
        dict(
            output_single_gtiff.dict,
            output=dict(
                output_single_gtiff.dict["output"],
                path=os.path.join(mp_s3_tmpdir, "temp.tif"),
                in_memory=False,
            ),
        )
    ) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(multi=2)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()
        # write empty array
        data = ma.masked_array(
            data=np.ones(process_tile.shape),
            mask=np.ones(process_tile.shape),
        )
        mp.config.output.write(process_tile, data)
    assert path_exists(mp.config.output.path)


@pytest.mark.skipif(
    not GDAL_COG_AVAILABLE, reason="GDAL>=3.1 with COG driver is required"
)
def test_output_single_gtiff_cog(output_single_gtiff_cog):
    tile_id = (5, 3, 7)
    with mapchete.open(output_single_gtiff_cog.dict) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(multi=2)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()
        # write empty array
        data = ma.masked_array(
            data=np.ones(process_tile.shape),
            mask=np.ones(process_tile.shape),
        )
        mp.config.output.write(process_tile, data)
    assert path_exists(mp.config.output.path)
    assert cog_validate(mp.config.output.path, strict=True)


@pytest.mark.skipif(
    not GDAL_COG_AVAILABLE, reason="GDAL>=3.1 with COG driver is required"
)
def test_output_single_gtiff_cog_tempfile(output_single_gtiff_cog):
    tile_id = (5, 3, 7)
    with mapchete.open(
        dict(
            output_single_gtiff_cog.dict,
            output=dict(output_single_gtiff_cog.dict["output"], in_memory=False),
        )
    ) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(multi=2)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()
        # write empty array
        data = ma.masked_array(
            data=np.ones(process_tile.shape),
            mask=np.ones(process_tile.shape),
        )
        mp.config.output.write(process_tile, data)
    assert path_exists(mp.config.output.path)
    assert cog_validate(mp.config.output.path, strict=True)


@pytest.mark.remote
@pytest.mark.skipif(
    not GDAL_COG_AVAILABLE, reason="GDAL>=3.1 with COG driver is required"
)
def test_output_single_gtiff_cog_s3(output_single_gtiff_cog, mp_s3_tmpdir):
    tile_id = (5, 3, 7)
    with mapchete.open(
        dict(
            output_single_gtiff_cog.dict,
            output=dict(
                output_single_gtiff_cog.dict["output"],
                path=os.path.join(mp_s3_tmpdir, "cog.tif"),
            ),
        )
    ) as mp:
        process_tile = mp.config.process_pyramid.tile(*tile_id)
        # basic functions
        assert mp.config.output.profile()
        assert mp.config.output.empty(process_tile).mask.all()
        assert mp.config.output.get_path(process_tile)
        # check if tile exists
        assert not mp.config.output.tiles_exist(process_tile)
        # write
        mp.batch_process(multi=2)
        # check if tile exists
        assert mp.config.output.tiles_exist(process_tile)
        # read again, this time with data
        data = mp.config.output.read(process_tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.all()
        # write empty array
        data = ma.masked_array(
            data=np.ones(process_tile.shape),
            mask=np.ones(process_tile.shape),
        )
        mp.config.output.write(process_tile, data)
    assert path_exists(mp.config.output.path)
    assert cog_validate(mp.config.output.path, strict=True)
