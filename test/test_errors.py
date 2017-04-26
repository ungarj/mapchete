#!/usr/bin/env python
"""Test custom Mapchete errors."""


import os
import yaml
import shutil

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete import errors


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")


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
        process = Mapchete(MapcheteConfig(config))
        tile = process.config.process_pyramid.tile(5, 0, 0)
        try:
            process.execute(tile)
        except errors.MapcheteProcessImportError:
            pass
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


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
            Mapchete(MapcheteConfig(config))
        except errors.MapcheteProcessSyntaxError:
            pass
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


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
        process = Mapchete(MapcheteConfig(config))
        tile = process.config.process_pyramid.tile(5, 0, 0)
        try:
            process.execute(tile)
        except errors.MapcheteProcessException:
            pass
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


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
        process = Mapchete(MapcheteConfig(config))
        tile = process.config.process_pyramid.tile(5, 0, 0)
        try:
            process.execute(tile)
        except errors.MapcheteProcessOutputError:
            pass
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass
