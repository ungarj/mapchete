import os
import pickle
from copy import deepcopy

import oyaml as yaml
import pytest
from fiona.errors import DriverError
from pydantic import ValidationError
from shapely import wkt
from shapely.errors import WKTReadingError
from shapely.geometry import Polygon, box, mapping, shape
from shapely.ops import unary_union

import mapchete
from mapchete.config import (
    MapcheteConfig,
    ProcessConfig,
    ProcessFunc,
    _guess_geometry,
    bounds_from_opts,
    snap_bounds,
)
from mapchete.errors import MapcheteConfigError
from mapchete.io import fiona_open, rasterio_open
from mapchete.path import MPath
from mapchete.types import Bounds

SCRIPT_DIR = MPath(os.path.dirname(os.path.realpath(__file__)))
TESTDATA_DIR = MPath(os.path.join(SCRIPT_DIR, "testdata/"))


def test_config_errors(example_mapchete):
    """Test various configuration parsing errors."""
    config_orig = example_mapchete.dict
    # wrong config type
    with pytest.raises(FileNotFoundError):
        mapchete.open("not_a_config")
    # missing process
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.pop("process")
        MapcheteConfig(config)
    # using input and input_files
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.update(input=None, input_files=None)
        mapchete.open(config)
    # output configuration not compatible with driver
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config["output"].pop("bands")
        MapcheteConfig(config)
    # no baselevel params
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.update(baselevels={})
        with mapchete.open(config) as mp:
            mp.config.baselevels
    # wrong baselevel min or max
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.update(baselevels={"min": "invalid"})
        with mapchete.open(config) as mp:
            mp.config.baselevels
    # wrong pixelbuffer type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config["pyramid"].update(pixelbuffer="wrong_type")
        mapchete.open(config)
    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config["pyramid"].update(metatiling="wrong_type")
        mapchete.open(config)


def test_config_zoom7(example_mapchete, dummy2_tif):
    """Example configuration at zoom 5."""
    config = MapcheteConfig(example_mapchete.dict)
    zoom7 = config.params_at_zoom(7)
    input_files = zoom7["input"]
    assert input_files["file1"] is None
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
        pytest.lazy_fixture("custom_grid"),
        pytest.lazy_fixture("file_groups"),
        pytest.lazy_fixture("overviews"),
        pytest.lazy_fixture("baselevels"),
        pytest.lazy_fixture("baselevels_output_buffer"),
        pytest.lazy_fixture("baselevels_custom_nodata"),
        pytest.lazy_fixture("mapchete_input"),
        pytest.lazy_fixture("dem_to_hillshade"),
        pytest.lazy_fixture("env_storage_options_mapchete"),
        pytest.lazy_fixture("zoom_mapchete"),
        pytest.lazy_fixture("cleantopo_br_mercator"),
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
    """Verify configuration gets parsed without input files."""
    config = file_groups.dict
    config.update(input=None)
    assert mapchete.open(config)


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
        assert inputs.keys() == user_process.input.keys()


def test_input_zooms(files_zooms):
    """Read correct input file per zoom."""
    config = MapcheteConfig(files_zooms.dict)
    # zoom 7
    input_files = config.params_at_zoom(7)["input"]
    assert os.path.basename(input_files["greater_smaller"].path) == "dummy1.tif"
    assert os.path.basename(input_files["equals"].path) == "dummy1.tif"
    # zoom 8
    input_files = config.params_at_zoom(8)["input"]
    assert os.path.basename(input_files["greater_smaller"].path) == "dummy1.tif"
    assert os.path.basename(input_files["equals"].path) == "dummy2.tif"
    # zoom 9
    input_files = config.params_at_zoom(9)["input"]
    assert os.path.basename(input_files["greater_smaller"].path) == "dummy2.tif"
    assert os.path.basename(input_files["equals"].path) == "cleantopo_br.tif"
    # zoom 10
    input_files = config.params_at_zoom(10)["input"]
    assert os.path.basename(input_files["greater_smaller"].path) == "dummy2.tif"
    assert os.path.basename(input_files["equals"].path) == "cleantopo_tl.tif"


def test_init_zoom(cleantopo_br):
    with mapchete.open(cleantopo_br.dict, zoom=[3, 5]) as mp:
        assert mp.config.init_zoom_levels == list(range(3, 6))


def test_process_module(process_module):
    mapchete.open(process_module.dict)


def test_aoi(aoi_br, aoi_br_geojson, cleantopo_br_tif):
    zoom = 7

    # read geojson geometry
    with fiona_open(aoi_br_geojson) as src:
        area = shape(next(iter(src))["geometry"])
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


def test_guess_geometry(aoi_br_geojson):
    with fiona_open(aoi_br_geojson) as src:
        area = shape(next(iter(src))["geometry"])

    # WKT
    geom, crs = _guess_geometry(area.wkt)
    assert geom.is_valid
    assert crs is None

    # GeoJSON mapping
    geom, crs = _guess_geometry(mapping(area))
    assert geom.is_valid
    assert crs is None

    # shapely Geometry
    geom, crs = _guess_geometry(area)
    assert geom.is_valid
    assert crs is None

    # path
    geom, crs = _guess_geometry(aoi_br_geojson)
    assert geom.is_valid
    assert crs

    # Errors
    # malformed WKT
    with pytest.raises(WKTReadingError):
        _guess_geometry(area.wkt.rstrip(")"))
    # non-existent path
    with pytest.raises(DriverError):
        _guess_geometry("/invalid_path.geojson")
    # malformed GeoJSON mapping
    with pytest.raises(AttributeError):
        _guess_geometry(dict(mapping(area), type=None))
    # unknown type
    with pytest.raises(TypeError):
        _guess_geometry(1)
    # wrong geometry type
    with pytest.raises(TypeError):
        _guess_geometry(area.centroid)


def test_bounds_from_opts(example_mapchete, wkt_geom):
    # WKT
    assert isinstance(bounds_from_opts(wkt_geometry=wkt_geom), Bounds)

    # point
    assert isinstance(
        bounds_from_opts(point=(0, 0), raw_conf=example_mapchete.dict), Bounds
    )

    # point from different CRS
    assert isinstance(
        bounds_from_opts(
            point=(0, 0), point_crs="EPSG:3857", raw_conf=example_mapchete.dict
        ),
        Bounds,
    )

    # bounds
    assert isinstance(
        bounds_from_opts(bounds=(1, 2, 3, 4), raw_conf=example_mapchete.dict), Bounds
    )

    # bounds from different CRS
    assert isinstance(
        bounds_from_opts(
            bounds=(1, 2, 3, 4), bounds_crs="EPSG:3857", raw_conf=example_mapchete.dict
        ),
        Bounds,
    )


def test_init_overrides_config(example_mapchete):
    process_bounds = (0, 1, 2, 3)
    init_bounds = (3, 4, 5, 6)
    process_area = box(*process_bounds)
    init_area = box(*init_bounds)

    # bounds
    with mapchete.open(
        dict(example_mapchete.dict, bounds=process_bounds), bounds=init_bounds
    ) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == init_bounds

    # area
    with mapchete.open(
        dict(example_mapchete.dict, area=process_area), area=init_area
    ) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == init_bounds

    # process bounds and init area
    with mapchete.open(
        dict(example_mapchete.dict, bounds=process_bounds), area=init_area
    ) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == init_bounds

    # process area and init bounds
    with mapchete.open(
        dict(example_mapchete.dict, area=process_area), bounds=init_bounds
    ) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == init_bounds

    # process bounds, init area and init bounds
    with mapchete.open(
        dict(example_mapchete.dict, bounds=process_bounds),
        area=init_area,
        bounds=init_bounds,
    ) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == init_bounds

    # process area, init area and init bounds
    with mapchete.open(
        dict(example_mapchete.dict, area=process_area),
        area=init_area,
        bounds=init_bounds,
    ) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == init_bounds

    # process area
    with mapchete.open(dict(example_mapchete.dict, area=process_area)) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == process_bounds

    # process bounds
    with mapchete.open(dict(example_mapchete.dict, bounds=process_bounds)) as mp:
        assert mp.config.bounds == process_bounds
        assert mp.config.init_bounds == process_bounds


def test_custom_process(example_custom_process_mapchete):
    with mapchete.open(example_custom_process_mapchete.dict) as mp:
        assert (
            mp.execute(example_custom_process_mapchete.first_process_tile()) is not None
        )


# pytest-env must be installed
def test_env_params(env_storage_options_mapchete):
    with mapchete.open(env_storage_options_mapchete.dict) as mp:
        inp = mp.config.params_at_zoom(5)
        assert inp["input"]["file1"].storage_options.get("access_key") == "foo"
        assert mp.config.output.storage_options.get("access_key") == "bar"


def test_process_config_pyramid_settings():
    conf = ProcessConfig(
        pyramid=dict(
            grid="geodetic",
        ),
        zoom_levels=5,
        output={},
    )
    assert conf.pyramid.pixelbuffer == 0
    assert conf.pyramid.metatiling == 1

    conf = ProcessConfig(
        pyramid=dict(grid="geodetic", pixelbuffer=5, metatiling=4),
        zoom_levels=5,
        output={},
    )
    assert conf.pyramid.pixelbuffer == 5
    assert conf.pyramid.metatiling == 4

    with pytest.raises(ValidationError):
        ProcessConfig(
            pyramid=dict(grid="geodetic", pixelbuffer=-1, metatiling=4),
            zoom_levels=5,
            output={},
        )

    with pytest.raises(ValidationError):
        ProcessConfig(
            pyramid=dict(grid="geodetic", pixelbuffer=5, metatiling=5),
            zoom_levels=5,
            output={},
        )


@pytest.mark.parametrize(
    "process_src",
    [
        "mapchete.processes.examples.example_process",
        SCRIPT_DIR / "example_process.py",
        (SCRIPT_DIR / "example_process.py").read_text().split("\n"),
    ],
)
def test_process(process_src, example_custom_process_mapchete):
    mp = example_custom_process_mapchete.process_mp()
    process = ProcessFunc(process_src)
    assert process.name
    assert process(mp) is not None


@pytest.mark.parametrize(
    "process_src",
    [
        "mapchete.processes.examples.example_process",
        SCRIPT_DIR / "example_process.py",
        (SCRIPT_DIR / "example_process.py").read_text().split("\n"),
    ],
)
def test_process_pickle(process_src, example_custom_process_mapchete):
    mp = example_custom_process_mapchete.process_mp()
    process = ProcessFunc(process_src)
    # pickle and unpickle
    reloaded = pickle.loads(pickle.dumps(process))
    assert reloaded(mp) is not None
