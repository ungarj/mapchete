"""Test Mapchete commons module."""

import numpy as np
import numpy.ma as ma
import pytest

from mapchete import Empty, MapcheteNodataTile
from mapchete.processes import clip, contours, convert, hillshade
from mapchete.processes.examples import example_process
from mapchete.testing import get_process_mp


def test_example_process(cleantopo_tl):
    # non-empty tile
    output = example_process.execute(cleantopo_tl.process_mp())
    assert isinstance(output, ma.masked_array)

    # empty tile
    output = example_process.execute(cleantopo_tl.process_mp(tile=(5, 3, 7)))
    assert output == "empty"


def test_convert_raster(local_raster, landpoly):
    # tile with data
    tile = (8, 28, 89)
    assert isinstance(
        convert.execute(
            inp=get_process_mp(input=dict(inp=local_raster), tile=tile).open("inp")
        ),
        np.ndarray,
    )


def test_convert_raster_empty(local_raster):
    tile = (8, 28, 189)
    # execute on empty tile
    with pytest.raises(MapcheteNodataTile):
        convert.execute(
            inp=get_process_mp(input=dict(inp=local_raster), tile=tile).open("inp")
        )


def test_convert_raster_clip(local_raster, landpoly):
    tile = (8, 28, 89)
    inp = get_process_mp(input=dict(inp=local_raster, clip=landpoly), tile=tile).open(
        "inp"
    )

    # tile with data
    default = convert.execute(inp)
    assert isinstance(default, np.ndarray)

    # scale_offset
    offset = convert.execute(inp, scale_offset=2)
    assert isinstance(offset, np.ndarray)

    # scale_ratio
    ratio = convert.execute(inp, scale_ratio=0.5)
    assert isinstance(ratio, np.ndarray)

    # clip_to_output_dtype
    clip_dtype = convert.execute(inp, scale_ratio=2, clip_to_output_dtype="uint8")
    assert isinstance(clip_dtype, np.ndarray)


def test_convert_raster_clip_empty(local_raster, landpoly):
    # NOTE: this was in the test suite but there is no reason why over this process tile
    # the execute function should return an empty tile
    # execute on empty tile
    tile = (8, 28, 189)
    mp = get_process_mp(input=dict(inp=local_raster, clip=landpoly), tile=tile)
    with pytest.raises(MapcheteNodataTile):
        convert.execute(mp.open("inp"), mp.open("clip"))


def test_convert_vector(landpoly):
    # execute without clip
    assert isinstance(
        convert.execute(
            get_process_mp(input=dict(inp=landpoly), zoom=5, metatiling=8).open("inp")
        ),
        list,
    )

    # execute on empty tile
    with pytest.raises(MapcheteNodataTile):
        convert.execute(
            get_process_mp(input=dict(inp=landpoly), tile=(5, 3, 7), metatiling=8).open(
                "inp"
            )
        )


def test_contours_dem(local_raster):
    # not empty dem
    tile = (8, 28, 89)
    output = contours.execute(
        get_process_mp(input=dict(dem=local_raster), tile=tile).open("dem")
    )
    assert isinstance(output, list)
    assert output


def test_contours_empty_dem(local_raster):
    # empty dem
    tile = (8, 28, 189)
    dem = get_process_mp(input=dict(dem=local_raster), tile=tile).open("dem")
    with pytest.raises(Empty):
        contours.execute(dem)


def test_contours_clipped(local_raster, landpoly):
    # clipped contours
    tile = (8, 28, 89)
    mp = get_process_mp(input=dict(dem=local_raster, clip=landpoly), tile=tile)
    output = contours.execute(mp.open("dem"), mp.open("clip"))
    assert isinstance(output, list)
    assert output


def test_contours_empty_clip(local_raster, landpoly):
    # empty clip geometry
    tile = (8, 68, 35)
    mp = get_process_mp(input=dict(dem=local_raster, clip=landpoly), tile=tile)
    with pytest.raises(Empty):
        contours.execute(mp.open("dem"), mp.open("clip"))


@pytest.mark.parametrize(
    "min_val, max_val, base, interval, control",
    [
        (0, 1000, 0, 500, [0, 500, 1000]),
        (10, 1000, 0, 500, [500, 1000]),
        (0, 1000, 10, 500, [10, 510]),
        (-100, 500, 0, 100, [-100, 0, 100, 200, 300, 400, 500]),
    ],
)
def test_get_contour_values(min_val, max_val, base, interval, control):
    assert contours.get_contour_values(min_val, max_val, base, interval) == control


def test_hillshade(local_raster):
    tile = (8, 68, 35)
    dem = get_process_mp(input=dict(dem=local_raster), tile=tile).open("dem")
    assert isinstance(hillshade.execute(dem), np.ndarray)


def test_hillshade_empty(local_raster, landpoly):
    # execute on empty tile
    tile = (8, 28, 189)
    dem = get_process_mp(input=dict(dem=local_raster), tile=tile).open("dem")
    with pytest.raises(MapcheteNodataTile):
        hillshade.execute(dem)


def test_hillshade_clip(local_raster, landpoly):
    tile = (8, 28, 89)
    dem = get_process_mp(input=dict(dem=local_raster, clip=landpoly), tile=tile).open(
        "dem"
    )
    assert isinstance(hillshade.execute(dem), np.ndarray)


def test_hillshade_clip_empty(local_raster, landpoly):
    tile = (8, 28, 189)
    # execute on empty tile
    mp = get_process_mp(input=dict(dem=local_raster, clip=landpoly), tile=tile)
    with pytest.raises(MapcheteNodataTile):
        hillshade.execute(mp.open("dem"), mp.open("clip"))


def test_clip(local_raster, landpoly):
    tile = (8, 28, 89)
    mp = get_process_mp(input=dict(inp=local_raster, clip=landpoly), tile=tile)
    output = clip.execute(mp.open("inp"), mp.open("clip"))
    assert isinstance(output, np.ndarray)
    assert not output.mask.all()
    assert output.mask.any()


def test_clip_empty(cleantopo_br_tif, landpoly):
    tile = (8, 28, 89)
    mp = get_process_mp(input=dict(inp=cleantopo_br_tif, clip=landpoly), tile=tile)
    with pytest.raises(Empty):
        clip.execute(mp.open("inp"), mp.open("clip"))
