import pytest
from pydantic import ValidationError

from mapchete.config.models import ProcessConfig, PyramidConfig
from mapchete.errors import MapcheteConfigError


@pytest.mark.parametrize("strict", [True, False])
def test_process_config_from_dict(example_mapchete, strict):
    assert ProcessConfig.from_dict(example_mapchete.dict, strict=strict)


@pytest.mark.parametrize("strict", [True, False])
def test_process_config_from_file(example_mapchete, strict):
    assert ProcessConfig.from_file(example_mapchete.path, strict=strict)


@pytest.mark.parametrize("strict", [True, False])
def test_process_config_parse(example_mapchete, strict):
    assert ProcessConfig.parse(example_mapchete.path, strict=strict)
    assert ProcessConfig.parse(example_mapchete.dict, strict=strict)


def test_process_config_parse_error():
    with pytest.raises(MapcheteConfigError):
        ProcessConfig.parse(None)  # type: ignore


def test_config_parse_dict_zoom_overlaps_error(example_mapchete):
    raw_config = example_mapchete.dict.copy()
    raw_config.update(process_parameters={"foo": {"zoom<9": 1, "zoom<10": 2}})
    with pytest.raises(MapcheteConfigError):
        ProcessConfig.parse(raw_config).zoom_parameters(7)


def test_config_parse_dict_not_all_zoom_dependent_error(example_mapchete):
    raw_config = example_mapchete.dict.copy()
    raw_config.update(process_parameters={"foo": {"zoom<9": 1, "bar": 2}})
    with pytest.raises(MapcheteConfigError):
        ProcessConfig.parse(raw_config).zoom_parameters(7)


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
