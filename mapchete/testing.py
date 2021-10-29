"""
Useful tools to facilitate testing.
"""
from mapchete import MapcheteProcess
from mapchete.tile import BufferedTilePyramid


def parse_inputs(inputs):
    1 / 0


def mp(tile=None, params=None, input=None, output_params=None, config=None):
    """
    Return a MapcheteProcess object which can be used in a process function.

    e.g. execute(mp)
    """
    tile = tile or BufferedTilePyramid("geodetic").tile(0, 0, 0)
    input = input or parse_inputs(input)
    return MapcheteProcess(
        tile=tile,
        params=params,
        input=input,
        output_params=output_params,
        config=config,
    )
