from copy import deepcopy

import pytest
import yaml
from pytest_lazyfixture import lazy_fixture
from shapely import wkt
from shapely.geometry import Polygon, box
from shapely.ops import unary_union

from mapchete import MapcheteProcess
from mapchete.config import MapcheteConfig, snap_bounds
from mapchete.config.models import DaskAdaptOptions, DaskSpecs
from mapchete.errors import MapcheteConfigError


def test_wrong_config_type():
    # wrong config type
    with pytest.raises(MapcheteConfigError):
        MapcheteConfig("not_a_config")


def test_missing_process(example_mapchete):
    # missing process
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("process")
        MapcheteConfig(config)


def test_incompatible_output(example_mapchete):
    # output configuration not compatible with driver
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["output"].pop("bands")
        MapcheteConfig(config)


def test_no_baselevels(example_mapchete):
    # no baselevel params
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(baselevels={})
        MapcheteConfig(config).baselevels


def test_baselevel_minmax_error(example_mapchete):
    # wrong baselevel min or max
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(baselevels={"min": "invalid"})
        MapcheteConfig(config).baselevels


def test_wrong_pixelbuffer_type(example_mapchete):
    # wrong pixelbuffer type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["pyramid"].update(pixelbuffer="wrong_type")
        MapcheteConfig(config)


def test_wrong_metatiling_type(example_mapchete):
    # wrong pixelbuffer type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["pyramid"].update(pixelbuffer="wrong_type")
        MapcheteConfig(config)

    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["pyramid"].update(metatiling="wrong_type")
        MapcheteConfig(config)


def test_config_zoom7(example_mapchete, dummy2_tif):
    """Example configuration at zoom 5."""
    config = MapcheteConfig(example_mapchete.dict)
    zoom7 = config.params_at_zoom(7)
    input_files = zoom7["input"]
    assert input_files["file1"] is not None
    assert str(input_files["file1"].path) == dummy2_tif
    assert str(input_files["file2"].path) == dummy2_tif
    assert zoom7["process_parameters"]["some_integer_parameter"] == 12
    assert zoom7["process_parameters"]["some_float_parameter"] == 5.3
    assert zoom7["process_parameters"]["some_string_parameter"] == "string1"
    assert zoom7["process_parameters"]["some_bool_parameter"] is True


def test_config_zoom11(example_mapchete, dummy2_tif, dummy1_tif):
    """Example configuration at zoom 11."""
    config = MapcheteConfig(example_mapchete.dict)
    zoom11 = config.params_at_zoom(11)
    input_files = zoom11["input"]
    assert str(input_files["file1"].path) == dummy1_tif
    assert str(input_files["file2"].path) == dummy2_tif
    assert zoom11["process_parameters"]["some_integer_parameter"] == 12
    assert zoom11["process_parameters"]["some_float_parameter"] == 5.3
    assert zoom11["process_parameters"]["some_string_parameter"] == "string2"
    assert zoom11["process_parameters"]["some_bool_parameter"] is True


def test_read_zoom_level(zoom_mapchete):
    """Read zoom level from config file."""
    config = MapcheteConfig(zoom_mapchete.dict)
    assert 5 in config.zoom_levels


@pytest.mark.parametrize("zoom", [7, 8, 9, 10])
def test_minmax_zooms(minmax_zoom, zoom):
    """Read min/max zoom levels from config file."""
    config = MapcheteConfig(minmax_zoom.dict)
    assert zoom in config.zoom_levels


@pytest.mark.parametrize("zoom", [7, 8])
def test_override_zoom_levels(minmax_zoom, zoom):
    """Override zoom levels when constructing configuration."""
    config = MapcheteConfig(minmax_zoom.dict, zoom=[7, 8])
    assert zoom in config.zoom_levels


def test_read_bounds(zoom_mapchete):
    """Read bounds from config file."""
    config = MapcheteConfig(zoom_mapchete.dict)
    test_polygon = Polygon([[3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]])
    assert config.area_at_zoom(5).equals(test_polygon)


def test_override_bounds(zoom_mapchete):
    """Override bounds when construcing configuration."""
    config = MapcheteConfig(zoom_mapchete.dict, bounds=[3, 1.5, 3.5, 2])
    test_polygon = Polygon([[3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]])
    assert config.area_at_zoom(5).equals(test_polygon)


def test_bounds_from_input_files(files_bounds):
    """Read bounds from input files."""
    config = MapcheteConfig(files_bounds.dict)
    test_polygon = Polygon(
        [[3, 2], [4, 2], [4, 1], [3, 1], [2, 1], [2, 4], [3, 4], [3, 2]]
    )
    assert config.area_at_zoom(10).equals(test_polygon)


def test_effective_bounds(files_bounds, baselevels):
    config = MapcheteConfig(files_bounds.dict)
    assert config.effective_bounds == snap_bounds(
        bounds=config.bounds,
        pyramid=config.process_pyramid,
        zoom=min(config.zoom_levels),
    )

    config = MapcheteConfig(baselevels.dict, zoom=[5, 7], bounds=(0, 1, 2, 3))
    assert config.effective_bounds != config.init_bounds
    assert config.effective_bounds == snap_bounds(
        bounds=config.init_bounds, pyramid=config.process_pyramid, zoom=5
    )

    with pytest.raises(MapcheteConfigError):
        MapcheteConfig(
            dict(
                baselevels.dict,
                zoom_levels=dict(min=7, max=7),
                baselevels=dict(lower="cubic", max=7),
            )
        )


@pytest.mark.parametrize(
    "example_config",
    [
        lazy_fixture("custom_grid"),
        lazy_fixture("file_groups"),
        lazy_fixture("overviews"),
        lazy_fixture("baselevels"),
        lazy_fixture("baselevels_output_buffer"),
        lazy_fixture("baselevels_custom_nodata"),
        lazy_fixture("mapchete_input"),
        lazy_fixture("dem_to_hillshade"),
        lazy_fixture("env_storage_options_mapchete"),
        lazy_fixture("zoom_mapchete"),
        lazy_fixture("cleantopo_br_mercator"),
    ],
)
def test_effective_area(example_config):
    config = MapcheteConfig(example_config.dict)
    aoi = config.area.intersection(config.init_area)
    control_area = unary_union(
        [
            tile.bbox
            for tile in config.process_pyramid.tiles_from_geom(
                aoi, config.zoom_levels.min
            )
        ]
    )
    assert config.effective_area.difference(control_area).area == 0


def test_read_mapchete_input(mapchete_input):
    """Read Mapchete files as input files."""
    config = MapcheteConfig(mapchete_input.dict)
    area = config.area_at_zoom(5)
    # testpolygon = box(0.5, 1.5, 3.5, 3.5)
    testpolygon = wkt.loads(
        "POLYGON ((3 1.5, 3 1, 2 1, 2 1.5, 0.5 1.5, 0.5 3.5, 2 3.5, 2 4, 3 4, 3 3.5, 3.5 3.5, 3.5 1.5, 3 1.5))"
    )
    assert area.equals(testpolygon)


def test_read_baselevels(baselevels):
    """Read baselevels."""
    config = MapcheteConfig(baselevels.dict)
    assert isinstance(config.baselevels, dict)
    assert set(config.baselevels["zooms"]) == set([5, 6])
    assert config.baselevels["lower"] == "bilinear"
    assert config.baselevels["higher"] == "nearest"

    # without min
    config = deepcopy(baselevels.dict)
    del config["baselevels"]["min"]
    assert min(MapcheteConfig(config).baselevels["zooms"]) == 3

    # without max and resampling
    config = deepcopy(baselevels.dict)
    del config["baselevels"]["max"]
    del config["baselevels"]["lower"]
    assert max(MapcheteConfig(config).baselevels["zooms"]) == 7
    assert MapcheteConfig(config).baselevels["lower"] == "nearest"


def test_empty_input(file_groups):
    """Input has to be defined if required by process."""
    config = file_groups.dict
    config.update(input=None)
    with pytest.raises(MapcheteConfigError):
        MapcheteConfig(config)


def test_input_name_process_params(example_mapchete):
    """Input has to be defined if required by process."""
    config = example_mapchete.dict
    config.update(process_parameters=dict(file1="foo"))
    with pytest.raises(MapcheteConfigError):
        MapcheteConfig(config)


def test_read_input_groups(file_groups):
    """Read input data groups."""
    config = MapcheteConfig(file_groups.dict)
    input_files = config.params_at_zoom(0)["input"]
    assert "file1" in input_files["group1"]
    assert "file2" in input_files["group1"]
    assert "file1" in input_files["group2"]
    assert "file2" in input_files["group2"]
    assert "nested_group" in input_files
    assert "group1" in input_files["nested_group"]
    assert "file1" in input_files["nested_group"]["group1"]
    assert "file2" in input_files["nested_group"]["group1"]
    assert "file1" in input_files["nested_group"]["group2"]
    assert "file2" in input_files["nested_group"]["group2"]
    assert config.area_at_zoom()


def test_input_zooms(files_zooms):
    """Read correct input file per zoom."""
    config = MapcheteConfig(files_zooms.dict)
    # zoom 7
    input_files = config.params_at_zoom(7)["input"]
    assert input_files["greater_smaller"].path.name == "dummy1.tif"
    assert input_files["equals"].path.name == "dummy1.tif"
    # zoom 8
    input_files = config.params_at_zoom(8)["input"]
    assert input_files["greater_smaller"].path.name == "dummy1.tif"
    assert input_files["equals"].path.name == "dummy2.tif"
    # zoom 9
    input_files = config.params_at_zoom(9)["input"]
    assert input_files["greater_smaller"].path.name == "dummy2.tif"
    assert input_files["equals"].path.name == "cleantopo_br.tif"
    # zoom 10
    input_files = config.params_at_zoom(10)["input"]
    assert input_files["greater_smaller"].path.name == "dummy2.tif"
    assert input_files["equals"].path.name == "cleantopo_tl.tif"


def test_init_overrides_config_bounds_init_bounds(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)

    # bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, bounds=process_bounds), bounds=init_bounds
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds


def test_init_overrides_config_area_init_area(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)
    process_area = box(*process_bounds)
    init_area = box(*init_bounds)

    # area
    config = MapcheteConfig(
        dict(example_mapchete.dict, area=process_area), area=init_area
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds


def test_init_overrides_config_bounds_init_area(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)
    init_area = box(*init_bounds)

    # process bounds and init area
    config = MapcheteConfig(
        dict(example_mapchete.dict, bounds=process_bounds), area=init_area
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds


def test_init_overrides_config_area_init_bounds(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)
    process_area = box(*process_bounds)

    # process area and init bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, area=process_area), bounds=init_bounds
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds


def test_init_overrides_config_bounds_init_area_init_bounds(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)
    init_area = box(*init_bounds)

    # process bounds, init area and init bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, bounds=process_bounds),
        area=init_area,
        bounds=init_bounds,
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds


def test_init_overrides_config_area_init_area_init_bounds(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)
    process_area = box(*process_bounds)
    init_area = box(*init_bounds)

    # process area, init area and init bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, area=process_area),
        area=init_area,
        bounds=init_bounds,
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds


def test_init_overrides_config_area(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    process_area = box(*process_bounds)

    # process area
    config = MapcheteConfig(dict(example_mapchete.dict, area=process_area))
    assert config.bounds == process_bounds
    assert config.init_bounds == process_bounds


def test_init_overrides_config_bounds(example_mapchete):
    process_bounds = (0, 1, 2, 3)

    # process bounds
    config = MapcheteConfig(dict(example_mapchete.dict, bounds=process_bounds))
    assert config.bounds == process_bounds
    assert config.init_bounds == process_bounds


def test_read_input_order(file_groups):
    """Assert input objects are represented in the same order as configured."""
    config = MapcheteConfig(file_groups.dict)
    inputs = yaml.safe_load(open(file_groups.path).read())["input"]
    tile = config.process_pyramid.tile(0, 0, 0)
    # read written data from within MapcheteProcess object
    user_process = MapcheteProcess(
        tile=tile,
        params=config.params_at_zoom(tile.zoom),
        input=config.get_inputs_for_tile(tile),
    )
    assert inputs.keys() == user_process.input.keys()


def test_init_zoom(cleantopo_br):
    config = MapcheteConfig(cleantopo_br.dict, zoom=[3, 5])
    assert config.init_zoom_levels == list(range(3, 6))


def test_process_module(process_module):
    assert MapcheteConfig(process_module.dict)


# pytest-env must be installed
def test_env_storage_options(env_storage_options_mapchete):
    config = MapcheteConfig(env_storage_options_mapchete.dict)
    inp = config.params_at_zoom(5)
    assert inp["input"]["file1"].storage_options.get("access_key") == "foo"
    assert config.output.storage_options.get("access_key") == "bar"


# pytest-env must be installed
def test_env_params(env_input_path_mapchete):
    config = MapcheteConfig(env_input_path_mapchete.dict)
    inp = config.params_at_zoom(5)
    assert inp["input"]["file1"].path.endswith("dummy2.tif")


def test_dask_specs(dask_specs):
    config = MapcheteConfig(dask_specs.dict)
    assert isinstance(config.parsed_config.dask_specs, DaskSpecs)
    assert isinstance(config.parsed_config.dask_specs.adapt_options, DaskAdaptOptions)
