"""Test deprecated items."""

import mapchete


def test_parse_deprecated(deprecated_params):
    with mapchete.open(deprecated_params.dict) as mp:
        assert mp.config.process_bounds() == mp.config.bounds_at_zoom()
        assert mp.config.process_area() == mp.config.process_area()
        assert mp.config.at_zoom(5) == mp.config.params_at_zoom(5)
        assert mp.config.inputs == mp.config.input
        assert mp.config.crs == mp.config.process_pyramid.crs
        assert mp.config.metatiling == mp.config.process_pyramid.metatiling
        assert mp.config.pixelbuffer == mp.config.process_pyramid.pixelbuffer


def test_parse_deprecated_zooms(deprecated_params):
    deprecated_params.dict.pop("process_zoom")
    deprecated_params.dict.update(process_minzoom=0, process_maxzoom=5)
    with mapchete.open(deprecated_params.dict) as mp:
        assert mp.config.zoom_levels == range(0, 6)
