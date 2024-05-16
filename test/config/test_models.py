import pytest
from pydantic import ValidationError

from mapchete.config import ProcessConfig, PyramidConfig


def test_process_config_pyramid_settings():
    conf = ProcessConfig(
        pyramid=PyramidConfig(
            grid="geodetic",
        ),
        zoom_levels=5,
        output={},
    )
    assert conf.pyramid.pixelbuffer == 0
    assert conf.pyramid.metatiling == 1

    conf = ProcessConfig(
        pyramid=PyramidConfig(grid="geodetic", pixelbuffer=5, metatiling=4),
        zoom_levels=5,
        output={},
    )
    assert conf.pyramid.pixelbuffer == 5
    assert conf.pyramid.metatiling == 4

    with pytest.raises(ValidationError):
        ProcessConfig(
            pyramid=PyramidConfig(grid="geodetic", pixelbuffer=-1, metatiling=4),
            zoom_levels=5,
            output={},
        )

    with pytest.raises(ValidationError):
        ProcessConfig(
            pyramid=PyramidConfig(grid="geodetic", pixelbuffer=5, metatiling=5),
            zoom_levels=5,
            output={},
        )
