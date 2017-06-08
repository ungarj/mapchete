#!/usr/bin/env python
"""Test custom Mapchete errors."""


import os
import yaml
import shutil
from copy import copy

import mapchete
from mapchete.config import MapcheteConfig
from mapchete import errors


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")


def test_config_modes():
    """Assert process mode is handled correctly."""
    # invalid mode
    try:
        MapcheteConfig(
            os.path.join(SCRIPTDIR, "example.mapchete"), mode="invalid")
        raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_metatiles():
    """Assert metatile sizes are checked."""
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(
                config_dir=SCRIPTDIR, metatiling=1)
            config["output"].update(metatiling=2)
            MapcheteConfig(config)
            raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_no_cli_input_file():
    """Assert input file from command line is checked."""
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(
                config_dir=SCRIPTDIR, input_files="from_command_line")
            MapcheteConfig(config)
            raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_wrong_bounds():
    """Wrong bounds number raises error."""
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(
                config_dir=SCRIPTDIR)
            MapcheteConfig(config, bounds=[2, 3])
            raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_empty_input_files():
    """Assert empty input files raises error."""
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["input_files"]
            MapcheteConfig(config)
            raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_mandatory_params():
    """Check availability of mandatory parameters."""
    for param in ["process_file", "input_files", "output"]:
        try:
            with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
                config = copy(yaml.load(mc))
                del config[param]
                config.update(config_dir=SCRIPTDIR)
                MapcheteConfig(config)
                raise Exception()
        except errors.MapcheteConfigError:
            pass
    # invalid path
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR, process_file="invalid/path.py")
            MapcheteConfig(config).process_file
            raise Exception()
    except errors.MapcheteConfigError:
        pass

    # no config dir given
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            MapcheteConfig(config).process_file
            raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_invalid_output_params():
    """Check on invalid configuration."""
    # missing or invalid params
    for param in ["format", "type"]:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            try:
                # invalid
                config["output"][param] = "invalid"
                MapcheteConfig(config)
                raise Exception()
            except errors.MapcheteConfigError:
                pass
            try:
                # missing
                del config["output"][param]
                MapcheteConfig(config)
                raise Exception()
            except errors.MapcheteConfigError:
                pass


def test_invalid_zoom_levels():
    """Check on invalid zoom configuration."""
    # no zoom levels given
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config)
            raise Exception()
    except errors.MapcheteConfigError:
        pass
    # invalid single zoom level
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config, zoom=-5)
            raise Exception()
    except errors.MapcheteConfigError:
        pass
    # invalid zoom level in pair
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config, zoom=[-5, 0])
            raise Exception()
    except errors.MapcheteConfigError:
        pass
    # invalid number of zoom levels
    try:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config, zoom=[0, 5, 7])
            raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_invalid_baselevels():
    """Check on invalid baselevel configuration."""
    # invalid zoom levels
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete")
        ) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            config["baselevels"].update(min=-5, max="x")
            MapcheteConfig(config)
            raise Exception()
    except errors.MapcheteConfigError:
        pass


def test_import_error():
    """Assert import error is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/import_error.py"))
        mp = mapchete.open(config)
        try:
            mp.execute((5, 0, 0))
            raise Exception()
        except errors.MapcheteProcessImportError:
            pass
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_syntax_error():
    """Assert syntax error is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/syntax_error.py"))
        try:
            mapchete.open(config)
            raise Exception()
        except errors.MapcheteProcessSyntaxError:
            pass
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_process_exception():
    """Assert process exception is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/process_error.py"))
        mp = mapchete.open(config)
        try:
            mp.execute((5, 0, 0))
            raise Exception()
        except errors.MapcheteProcessException:
            pass
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_output_error():
    """Assert output error is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/output_error.py"))
        mp = mapchete.open(config)
        try:
            mp.execute((5, 0, 0))
            raise Exception()
        except errors.MapcheteProcessOutputError:
            pass
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)
