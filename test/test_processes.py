"""Test Mapchete commons module."""

import numpy as np
import numpy.ma as ma
import pytest

import mapchete
from mapchete import MapcheteNodataTile
from mapchete.processes import contours, convert, hillshade
from mapchete.processes.examples import example_process
from mapchete.testing import get_process_mp


def test_example_process(cleantopo_tl):
    # non-empty tile
    output = example_process.execute(cleantopo_tl.process_mp())
    assert isinstance(output, ma.masked_array)

    # empty tile
    output = example_process.execute(cleantopo_tl.process_mp(tile=(5, 3, 7)))
    assert output == "empty"


def test_convert_raster(cleantopo_tl_tif, landpoly):
    # tile with data
    assert isinstance(
        convert.execute(
            inp=get_process_mp(input=dict(inp=cleantopo_tl_tif), zoom=5).open("inp")
        ),
        np.ndarray,
    )

    # execute on empty tile
    with pytest.raises(MapcheteNodataTile):
        convert.execute(
            inp=get_process_mp(input=dict(inp=cleantopo_tl_tif), tile=(5, 3, 7)).open(
                "inp"
            )
        )

    inp = get_process_mp(
        input=dict(inp=cleantopo_tl_tif, clip=landpoly), zoom=5, metatiling=8
    ).open("inp")

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

    # NOTE: this was in the test suite but there is no reason why over this process tile
    # the execute function should return an empty tile
    # execute on empty tile
    mp = get_process_mp(
        input=dict(inp=cleantopo_tl_tif, clip=landpoly),
        tile=(5, 0, 0),
        metatiling=1,
    )
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


def test_contours(cleantopo_tl_tif, landpoly):
    dem = get_process_mp(input=dict(dem=cleantopo_tl_tif), zoom=5, metatiling=8).open(
        "dem"
    )
    output = contours.execute(dem)
    assert isinstance(output, list)
    assert output

    # execute on empty tile
    dem = get_process_mp(
        input=dict(dem=cleantopo_tl_tif), tile=(5, 3, 7), metatiling=8
    ).open("dem")
    with pytest.raises(MapcheteNodataTile):
        contours.execute(dem)

    dem = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), zoom=5, metatiling=8
    ).open("dem")
    output = contours.execute(dem)
    assert isinstance(output, list)
    assert output

    dem = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), tile=(5, 3, 7), metatiling=8
    ).open("dem")
    with pytest.raises(MapcheteNodataTile):
        contours.execute(dem)


def test_hillshade(cleantopo_tl_tif, landpoly):
    dem = get_process_mp(input=dict(dem=cleantopo_tl_tif), zoom=5, metatiling=8).open(
        "dem"
    )
    assert isinstance(hillshade.execute(dem), np.ndarray)

    # execute on empty tile
    dem = get_process_mp(
        input=dict(dem=cleantopo_tl_tif), tile=(5, 3, 7), metatiling=8
    ).open("dem")
    with pytest.raises(MapcheteNodataTile):
        hillshade.execute(dem)

    dem = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), zoom=5, metatiling=8
    ).open("dem")
    assert isinstance(hillshade.execute(dem), np.ndarray)

    # execute on empty tile
    mp = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), tile=(5, 3, 7), metatiling=8
    )
    with pytest.raises(MapcheteNodataTile):
        hillshade.execute(mp.open("dem"), mp.open("clip"))
