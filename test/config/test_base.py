from copy import deepcopy

import oyaml as yaml
import pytest
from pytest_lazyfixture import lazy_fixture
from rasterio.enums import Resampling
from shapely import wkt
from shapely.geometry import Polygon, box, shape
from shapely.ops import unary_union

import mapchete
from mapchete.config.base import MapcheteConfig, snap_bounds
from mapchete.config.models import DaskAdaptOptions, DaskSpecs, OverviewSettings
from mapchete.enums import Concurrency
from mapchete.errors import MapcheteConfigError
from mapchete.io import fiona_open, rasterio_open


def test_config_errors_type():
    # wrong config type
    with pytest.raises(MapcheteConfigError):
        MapcheteConfig("not_a_config")


def test_config_errors_missing_process(example_mapchete):
    # missing process
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("process")
        MapcheteConfig(config)


def test_config_errors_invalid_output_config(example_mapchete):
    # output configuration not compatible with driver
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["output"].pop("bands")
        MapcheteConfig(config)


def test_config_errors_no_baselevel(example_mapchete):
    # no baselevel params
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(baselevels={})
        assert not MapcheteConfig(config).baselevels


def test_config_errors_baselevel_invalid_min(example_mapchete):
    # wrong baselevel min or max
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(baselevels={"min": "invalid"})
        assert MapcheteConfig(config).baselevels


def test_config_errors_invalid_pyramid_pixelbuffer(example_mapchete):
    # wrong pixelbuffer type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["pyramid"].update(pixelbuffer="wrong_type")
        MapcheteConfig(config)


def test_config_errors_invalid_pyramid_metatiling(example_mapchete):
    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["pyramid"].update(metatiling="wrong_type")
        MapcheteConfig(config)


def test_config_errors_invalid_mode(example_mapchete):
    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        MapcheteConfig(config, mode="invalid")


def test_config_errors_output_format_missing(
    example_mapchete,
):
    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["output"].pop("format")
        MapcheteConfig(config)


def test_config_errors_output_invalid_format(
    example_mapchete,
):
    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["output"].update(format="foo")
        MapcheteConfig(config)


def test_config_errors_output_metatiling_larger_than_process_metatiling(
    example_mapchete,
):
    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["output"].update(metatiling=32)
        MapcheteConfig(config)


def test_config_parse_dict(example_mapchete):
    raw_config = example_mapchete.dict.copy()
    raw_config.update(process_parameters=dict(foo=dict(bar=1)))
    config = MapcheteConfig(raw_config)
    assert config.params_at_zoom(7)["process_parameters"]["foo"]["bar"] == 1  # type: ignore


def test_config_zoom7(example_mapchete, dummy2_tif):
    """Example configuration at zoom 5."""
    config = MapcheteConfig(example_mapchete.dict)
    zoom7 = config.params_at_zoom(7)
    input_files = zoom7["input"]
    assert input_files["file1"] is not None  # type: ignore
    assert str(input_files["file1"].path) == dummy2_tif  # type: ignore
    assert str(input_files["file2"].path) == dummy2_tif  # type: ignore
    assert zoom7["process_parameters"]["some_integer_parameter"] == 12  # type: ignore
    assert zoom7["process_parameters"]["some_float_parameter"] == 5.3  # type: ignore
    assert zoom7["process_parameters"]["some_string_parameter"] == "string1"  # type: ignore
    assert zoom7["process_parameters"]["some_bool_parameter"] is True  # type: ignore


def test_config_zoom11(example_mapchete, dummy2_tif, dummy1_tif):
    """Example configuration at zoom 11."""
    config = MapcheteConfig(example_mapchete.dict)
    zoom11 = config.params_at_zoom(11)
    input_files = zoom11["input"]
    assert str(input_files["file1"].path) == dummy1_tif  # type: ignore
    assert str(input_files["file2"].path) == dummy2_tif  # type: ignore
    assert zoom11["process_parameters"]["some_integer_parameter"] == 12  # type: ignore
    assert zoom11["process_parameters"]["some_float_parameter"] == 5.3  # type: ignore
    assert zoom11["process_parameters"]["some_string_parameter"] == "string2"  # type: ignore
    assert zoom11["process_parameters"]["some_bool_parameter"] is True  # type: ignore


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


def test_baselevels_output_reader(baselevels):
    config = MapcheteConfig(baselevels.dict)
    assert config.output_reader


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
            tile.bbox  # type: ignore
            for tile in config.process_pyramid.tiles_from_geom(
                aoi, config.zoom_levels.min
            )
        ]
    )
    assert config.effective_area.difference(control_area).area == 0  # type: ignore


def test_area_and_bounds(cleantopo_br_tiledir, sample_geojson):
    outside_bounds = (-1.7578125, 54.931640625, -1.73583984375, 54.95361328125)
    with mapchete.open(
        dict(cleantopo_br_tiledir.dict, area=sample_geojson), bounds=outside_bounds
    ) as mp:
        assert len(list(mp.get_process_tiles())) == 0


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
    assert isinstance(config.baselevels, OverviewSettings)
    assert set(config.baselevels.zooms) == set([5, 6])
    assert config.baselevels.lower == Resampling.bilinear
    assert config.baselevels.higher == Resampling.nearest

    # without min
    config = deepcopy(baselevels.dict)
    del config["baselevels"]["min"]
    assert min(MapcheteConfig(config).baselevels.zooms) == 3  # type: ignore

    # without max and resampling
    config = deepcopy(baselevels.dict)
    del config["baselevels"]["max"]
    del config["baselevels"]["lower"]
    assert max(MapcheteConfig(config).baselevels.zooms) == 7  # type: ignore
    assert MapcheteConfig(config).baselevels.lower == Resampling.nearest  # type: ignore


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
    assert "file1" in input_files["group1"]  # type: ignore
    assert "file2" in input_files["group1"]  # type: ignore
    assert "file1" in input_files["group2"]  # type: ignore
    assert "file2" in input_files["group2"]  # type: ignore
    assert "nested_group" in input_files  # type: ignore
    assert "group1" in input_files["nested_group"]  # type: ignore
    assert "file1" in input_files["nested_group"]["group1"]  # type: ignore
    assert "file2" in input_files["nested_group"]["group1"]  # type: ignore
    assert "file1" in input_files["nested_group"]["group2"]  # type: ignore
    assert "file2" in input_files["nested_group"]["group2"]  # type: ignore
    assert config.area_at_zoom()


def test_read_input_order(file_groups):
    """Assert input objects are represented in the same order as configured."""
    with mapchete.open(file_groups.dict) as mp:
        inputs = yaml.safe_load(open(file_groups.path).read())["input"]
        tile = mp.config.process_pyramid.tile(0, 0, 0)
        # read written data from within MapcheteProcess object
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert inputs.keys() == user_process.input.keys()  # type: ignore


def test_input_zooms(files_zooms):
    """Read correct input file per zoom."""
    config = MapcheteConfig(files_zooms.dict)
    # zoom 7
    input_files = config.params_at_zoom(7)["input"]
    assert input_files["greater_smaller"].path.name == "dummy1.tif"  # type: ignore
    assert input_files["equals"].path.name == "dummy1.tif"  # type: ignore
    # zoom 8
    input_files = config.params_at_zoom(8)["input"]
    assert input_files["greater_smaller"].path.name == "dummy1.tif"  # type: ignore
    assert input_files["equals"].path.name == "dummy2.tif"  # type: ignore
    # zoom 9
    input_files = config.params_at_zoom(9)["input"]
    assert input_files["greater_smaller"].path.name == "dummy2.tif"  # type: ignore
    assert input_files["equals"].path.name == "cleantopo_br.tif"  # type: ignore
    # zoom 10
    input_files = config.params_at_zoom(10)["input"]
    assert input_files["greater_smaller"].path.name == "dummy2.tif"  # type: ignore
    assert input_files["equals"].path.name == "cleantopo_tl.tif"  # type: ignore


def test_init_zoom(cleantopo_br):
    config = MapcheteConfig(cleantopo_br.dict, zoom=[3, 5])
    assert config.init_zoom_levels == list(range(3, 6))


def test_process_module(process_module):
    assert MapcheteConfig(process_module.dict)


def test_aoi(aoi_br, aoi_br_geojson, cleantopo_br_tif):
    zoom = 7

    # read geojson geometry
    with fiona_open(aoi_br_geojson) as src:
        area = shape(next(iter(src))["geometry"])  # type: ignore
    # read input tiff bounds
    with rasterio_open(cleantopo_br_tif) as src:
        raster = box(*src.bounds)
    aoi = area.intersection(raster)

    # area as path in mapchete config
    with mapchete.open(aoi_br.dict) as mp:
        aoi_tiles = list(mp.config.process_pyramid.tiles_from_geom(aoi, zoom))
        process_tiles = list(mp.get_process_tiles(zoom=zoom))
        assert len(aoi_tiles) == len(process_tiles)
        assert set(aoi_tiles) == set(process_tiles)

    # area as WKT in mapchete config
    with mapchete.open(
        dict(aoi_br.dict, area=area.wkt),
    ) as mp:
        process_tiles = list(mp.get_process_tiles(zoom=zoom))
        assert len(aoi_tiles) == len(process_tiles)
        assert set(aoi_tiles) == set(process_tiles)

    # area as path in mapchete.open
    with mapchete.open(dict(aoi_br.dict, area=None), area=aoi_br_geojson) as mp:
        process_tiles = list(mp.get_process_tiles(zoom=zoom))
        assert len(aoi_tiles) == len(process_tiles)
        assert set(aoi_tiles) == set(process_tiles)

    # errors
    # non-existent path
    with pytest.raises(MapcheteConfigError):
        mapchete.open(dict(aoi_br.dict, area=None), area="/invalid_path.geojson")


def test_init_overrides_config(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)
    process_area = box(*process_bounds)
    init_area = box(*init_bounds)

    # bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, bounds=process_bounds), bounds=init_bounds
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds

    # area
    config = MapcheteConfig(
        dict(example_mapchete.dict, area=process_area), area=init_area
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds

    # process bounds and init area
    config = MapcheteConfig(
        dict(example_mapchete.dict, bounds=process_bounds), area=init_area
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds

    # process area and init bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, area=process_area), bounds=init_bounds
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds

    # process bounds, init area and init bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, bounds=process_bounds),
        area=init_area,
        bounds=init_bounds,
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds

    # process area, init area and init bounds
    config = MapcheteConfig(
        dict(example_mapchete.dict, area=process_area),
        area=init_area,
        bounds=init_bounds,
    )
    assert config.bounds == process_bounds
    assert config.init_bounds == init_bounds

    # process area
    config = MapcheteConfig(dict(example_mapchete.dict, area=process_area))
    assert config.bounds == process_bounds
    assert config.init_bounds == process_bounds

    # process bounds
    config = MapcheteConfig(dict(example_mapchete.dict, bounds=process_bounds))
    assert config.bounds == process_bounds
    assert config.init_bounds == process_bounds


def test_custom_process(example_custom_process_mapchete):
    with mapchete.open(example_custom_process_mapchete.dict) as mp:
        tile = example_custom_process_mapchete.first_process_tile()
        assert mp.execute_tile(tile) is not None


# pytest-env must be installed
def test_env_storage_options(env_storage_options_mapchete):
    config = MapcheteConfig(env_storage_options_mapchete.dict)
    assert (
        config.params_at_zoom(5)["input"]["file1"].storage_options.get("access_key")  # type: ignore
        == "foo"
    )
    assert config.output.storage_options.get("access_key") == "bar"  # type: ignore


# pytest-env must be installed
def test_env_params(env_input_path_mapchete):
    config = MapcheteConfig(env_input_path_mapchete.dict)
    assert config.params_at_zoom(5)["input"]["file1"].path.endswith("dummy2.tif")  # type: ignore


def test_dask_specs(dask_specs):
    with dask_specs.mp() as mp:
        assert isinstance(mp.config.parsed_config.dask_specs, DaskSpecs)
        assert isinstance(
            mp.config.parsed_config.dask_specs.adapt_options, DaskAdaptOptions
        )


def test_typed_raster_input(typed_raster_input):
    with mapchete.open(typed_raster_input.path) as mp:
        list(mp.execute(concurrency=Concurrency.none))


def test_input_at_zoom(example_mapchete):
    config = MapcheteConfig(example_mapchete.dict)
    assert config.input_at_zoom("file1", 7)


def test_preprocessing_tasks_per_input(preprocess_cache_memory):
    config = MapcheteConfig(preprocess_cache_memory.dict)
    for (preprocessing_tasks,) in config.preprocessing_tasks_per_input().values():
        assert preprocessing_tasks


def test_preprocessing_tasks(preprocess_cache_memory):
    config = MapcheteConfig(preprocess_cache_memory.dict)
    assert config.preprocessing_tasks()
    assert config.preprocessing_tasks_count()


def test_preprocessing_task_finished(preprocess_cache_memory):
    config = MapcheteConfig(preprocess_cache_memory.dict)
    for task_key in config.preprocessing_tasks():
        assert not config.preprocessing_task_finished(task_key)


def test_set_preprocessing_task_result(preprocess_cache_memory):
    config = MapcheteConfig(preprocess_cache_memory.dict)
    for task_key in config.preprocessing_tasks():
        config.set_preprocessing_task_result(task_key, "foo")
    for task_key in config.preprocessing_tasks():
        assert config.preprocessing_task_finished(task_key)


@pytest.mark.skip(reason="just have this here for future reference")
def test_input_union_special_case():
    inputs_bboxes = [
        wkt.loads(
            "POLYGON ((61.87774658203125 25.31524658203125, 61.87774658203125 22.499459088891662, 61.54664001798359 22.50000394977196, 61.551935808978804 24.204692898426533, 60.46875 24.34987565522143, 59.670821629213485 24.45682447197602, 59.05975341796875 24.538727841428283, 59.05975341796875 25.31524658203125, 61.87774658203125 25.31524658203125))"
        ),
        wkt.loads(
            "POLYGON ((61.87774658203125 22.49725341796875, 61.54663101769545 22.49725341796875, 61.54664001798359 22.50018469008428, 61.87774658203125 22.500173492502597, 61.87774658203125 22.49725341796875))"
        ),
        wkt.loads(
            "POLYGON ((59.05975341796875 22.49725341796875, 59.05975341796875 24.538727841428283, 59.0625 24.5383597085117, 59.160145469148475 24.52527198313532, 59.670821629213485 24.45682447197602, 60.46875 24.34987565522143, 61.551935808978804 24.204692898426533, 61.54668582675388 22.49725341796875, 59.05975341796875 22.49725341796875))"
        ),
    ]
    # inputs are not overlapping but touching each other. however, there is a slight gap between them resulting in
    # a square with a little line in between.
    input_union = unary_union(inputs_bboxes)
    assert input_union.area
    init_area = wkt.loads(
        "POLYGON ((61.875 22.5, 59.0625 22.5, 59.0625 25.3125, 61.875 25.3125, 61.875 22.5))"
    )
    # when intersected with a perfect square (the init_area), instead of adding the small gap to the output,
    # the small gap _is_ the output which obviously is wrong
    config_area_at_zoom = init_area.intersection(input_union)
    assert config_area_at_zoom.area == pytest.approx(init_area.area)
