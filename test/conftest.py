#!/usr/bin/env python
"""Fixtures such as Flask app for serve."""

import os
import pytest

from mapchete.cli.serve import create_app


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))


class Namespace(object):
    """Dummy argparse class."""

    def __init__(self, **kwargs):
        """Initialize using a dictionary."""
        self.__dict__.update(kwargs)


@pytest.fixture
def app():
    """Dummy Flask app."""
    example_process = os.path.join(
        SCRIPTDIR, "testdata/dem_to_hillshade.mapchete")
    args = Namespace(
        port=5000, mapchete_file=example_process, zoom=None, bounds=None,
        input_file=None, memory=None, readonly=False, overwrite=True,
        debug=False)
    return create_app(args)
