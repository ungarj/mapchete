"""Test Mapchete commons module."""

import numpy as np
import numpy.ma as ma

import mapchete
from mapchete.processes.examples import example_process
from mapchete.processes import contours, convert, hillshade
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
        convert.execute(get_process_mp(input=dict(inp=cleantopo_tl_tif), zoom=5)),
        np.ndarray,
    )

    # execute on empty tile
    assert (
        convert.execute(
            get_process_mp(input=dict(inp=cleantopo_tl_tif), tile=(5, 3, 7))
        )
        == "empty"
    )

    process_mp = get_process_mp(
        input=dict(inp=cleantopo_tl_tif, clip=landpoly), zoom=5, metatiling=8
    )

    # tile with data
    default = convert.execute(process_mp)
    assert isinstance(default, np.ndarray)

    # scale_offset
    offset = convert.execute(process_mp, scale_offset=2)
    assert isinstance(offset, np.ndarray)

    # scale_ratio
    ratio = convert.execute(process_mp, scale_ratio=0.5)
    assert isinstance(ratio, np.ndarray)

    # clip_to_output_dtype
    clip_dtype = convert.execute(
        process_mp, scale_ratio=2, clip_to_output_dtype="uint8"
    )
    assert isinstance(clip_dtype, np.ndarray)

    # execute on empty tile
    assert (
        convert.execute(
            get_process_mp(
                input=dict(inp=cleantopo_tl_tif, clip=landpoly),
                tile=(5, 0, 0),
                metatiling=1,
            )
        )
        == "empty"
    )


def test_convert_vector(landpoly):
    # execute without clip
    assert isinstance(
        convert.execute(get_process_mp(input=dict(inp=landpoly), zoom=5, metatiling=8)),
        list,
    )

    # execute on empty tile
    assert (
        convert.execute(
            get_process_mp(input=dict(inp=landpoly), tile=(5, 3, 7), metatiling=8)
        )
        == "empty"
    )


def test_contours(cleantopo_tl_tif, landpoly):
    process_mp = get_process_mp(input=dict(dem=cleantopo_tl_tif), zoom=5, metatiling=8)
    output = contours.execute(process_mp)
    assert isinstance(output, list)
    assert output

    # execute on empty tile
    process_mp = get_process_mp(
        input=dict(dem=cleantopo_tl_tif), tile=(5, 3, 7), metatiling=8
    )
    assert contours.execute(process_mp) == "empty"

    process_mp = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), zoom=5, metatiling=8
    )
    output = contours.execute(process_mp)
    assert isinstance(output, list)
    assert output

    process_mp = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), tile=(5, 3, 7), metatiling=8
    )
    assert contours.execute(process_mp) == "empty"


def test_hillshade(cleantopo_tl_tif, landpoly):
    process_mp = get_process_mp(input=dict(dem=cleantopo_tl_tif), zoom=5, metatiling=8)
    assert isinstance(hillshade.execute(process_mp), np.ndarray)

    # execute on empty tile
    process_mp = get_process_mp(
        input=dict(dem=cleantopo_tl_tif), tile=(5, 3, 7), metatiling=8
    )
    assert hillshade.execute(process_mp) == "empty"

    process_mp = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), zoom=5, metatiling=8
    )
    assert isinstance(hillshade.execute(process_mp), np.ndarray)
    # execute on empty tile
    process_mp = get_process_mp(
        input=dict(dem=cleantopo_tl_tif, clip=landpoly), tile=(5, 3, 7), metatiling=8
    )
    assert hillshade.execute(process_mp) == "empty"
