#!/usr/bin/env python
"""Test GeoJSON as process output."""

import os

from mapchete import Mapchete
from mapchete.formats.default import gtiff
from mapchete.tile import BufferedTile

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")

def test_output_data():
    """Check GeoTIFF as output data."""

    output_params = dict(
        type="geodetic",
        format="GeoTIFF",
        path=OUT_DIR,
        pixelbuffer=0,
        metatiling=1
    )
    output = gtiff.OutputData(output_params)
    assert output.path == OUT_DIR
    assert output.file_extension == ".tif"
