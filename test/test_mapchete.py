#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import os
import shutil
import rasterio
import numpy.ma as ma
import pkg_resources
from cPickle import dumps
from functools import partial
from multiprocessing import Pool

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTile
from mapchete.io.raster import create_mosaic

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_empty_execute():
    """Execute process outside of defined zoom levels."""
    try:
        process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/cleantopo_br.mapchete")))
        tile = process.config.process_pyramid.tile(6, 0, 0)
        out_tile = process.execute(tile)
        assert out_tile.data.mask.all()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_read_existing_output():
    """Read existing process output."""
    try:
        process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/cleantopo_tl.mapchete")))
        tile = process.config.process_pyramid.tile(5, 0, 0)
        # process and save
        process.write(process.get_raw_output(tile))
        # read written data
        out_tile = process.read(tile)
        assert not out_tile.data.mask.all()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_get_raw_output_outside():
    """Get raw process output outside of zoom levels."""
    try:
        process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/cleantopo_br.mapchete")))
        tile = process.config.process_pyramid.tile(6, 0, 0)
        out_tile = process.get_raw_output(tile)
        assert out_tile.data.mask.all()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_get_raw_output_memory():
    """Get raw process output using memory flag."""
    try:
        process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/cleantopo_tl.mapchete"), mode="memory"))
        assert process.config.mode == "memory"
        tile = process.config.process_pyramid.tile(5, 0, 0)
        out_tile = process.get_raw_output(tile)
        assert not out_tile.data.mask.all()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_get_raw_output_readonly():
    """Get raw process output using readonly flag."""
    try:
        readonly_process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/cleantopo_tl.mapchete"), mode="readonly"))
        readonly_tile = readonly_process.config.process_pyramid.tile(5, 0, 0)
        write_process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/cleantopo_tl.mapchete"), mode="continue"))
        write_tile = write_process.config.process_pyramid.tile(5, 0, 0)
        # read non-existing data (returns empty)
        out_tile = readonly_process.get_raw_output(readonly_tile)
        assert out_tile.data.mask.all()
        # process and save
        try:
            readonly_process.write(
                readonly_process.get_raw_output(readonly_tile))
        except AssertionError:
            pass
        write_process.write(write_process.get_raw_output(write_tile))
        # read written output
        out_tile = readonly_process.get_raw_output(readonly_tile)
        assert not out_tile.data.mask.all()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_get_raw_output_continue():
    """Get raw process output using memory flag."""
    try:
        process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/cleantopo_tl.mapchete")))
        assert process.config.mode == "continue"
        tile = process.config.process_pyramid.tile(5, 0, 0)
        # process and save
        process.write(process.get_raw_output(tile))
        # read written data
        out_tile = process.get_raw_output(tile)
        assert not out_tile.data.mask.all()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_baselevels():
    """Baselevel interpolation."""
    try:
        process = Mapchete(
            MapcheteConfig(os.path.join(
                SCRIPTDIR, "testdata/baselevels.mapchete"), mode="continue"))
        # from lower zoom level
        lower_tile = process.get_process_tiles(4).next()
        # process and save
        for tile in lower_tile.get_children():
            output = process.get_raw_output(tile)
            process.write(output)
        # read from baselevel
        out_tile = process.get_raw_output(lower_tile)
        assert not out_tile.data.mask.all()

        # from higher zoom level
        tile = process.get_process_tiles(6).next()
        # process and save
        output = process.get_raw_output(tile)
        process.write(output)
        # read from baselevel
        assert any([
            not process.get_raw_output(upper_tile).data.mask.all()
            for upper_tile in tile.get_children()
        ])
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_processing():
    """Test correct processing (read and write) outputs."""
    for cleantopo_process in [
        "testdata/cleantopo_tl.mapchete", "testdata/cleantopo_br.mapchete"
    ]:
        process = Mapchete(
            MapcheteConfig(os.path.join(SCRIPTDIR, cleantopo_process)))
        for zoom in range(6):
            tiles = []
            for tile in process.get_process_tiles(zoom):
                output = process.execute(tile)
                tiles.append(output)
                assert isinstance(output, BufferedTile)
                assert isinstance(output.data, ma.MaskedArray)
                assert output.data.shape == output.shape
                assert not ma.all(output.data.mask)
                process.write(output)
            mosaic, mosaic_affine = create_mosaic(tiles)
            try:
                temp_vrt = os.path.join(OUT_DIR, str(zoom)+".vrt")
                gdalbuildvrt = "gdalbuildvrt %s %s/%s/*/*.tif > /dev/null" % (
                    temp_vrt, OUT_DIR, zoom)
                os.system(gdalbuildvrt)
                with rasterio.open(temp_vrt, "r") as testfile:
                    for file_item, mosaic_item in zip(
                        testfile.meta["transform"], mosaic_affine
                    ):
                        assert file_item == mosaic_item
                    band = testfile.read(1, masked=True)
                    assert band.shape == mosaic.shape
                    assert ma.allclose(band, mosaic)
                    assert ma.allclose(band.mask, mosaic.mask)
            except Exception:
                raise
            finally:
                try:
                    os.remove(temp_vrt)
                    shutil.rmtree(OUT_DIR)
                except OSError:
                    pass


def test_multiprocessing():
    """Test parallel tile processing."""
    process = Mapchete(
        MapcheteConfig(os.path.join(
            SCRIPTDIR, "testdata/cleantopo_tl.mapchete")))
    assert dumps(process)
    assert dumps(process.config)
    assert dumps(process.config.output)
    for tile in process.get_process_tiles():
        assert dumps(tile)
    f = partial(_worker, process)
    try:
        pool = Pool()
        for zoom in reversed(process.config.zoom_levels):
            for raw_output in pool.imap_unordered(
                f, process.get_process_tiles(zoom), chunksize=8
            ):
                process.write(raw_output)
    except KeyboardInterrupt:
        pool.terminate()
    except Exception:
        raise
    finally:
        pool.close()
        pool.join()
        try:
            shutil.rmtree(OUT_DIR)
        except Exception:
            pass


def _worker(process, process_tile):
    """Multiprocessing worker processing a tile."""
    return process.execute(process_tile)


def test_process_template():
    """Template used to create an empty process."""
    process_template = pkg_resources.resource_filename(
        "mapchete.static", "process_template.py")
    dummy1 = os.path.join(TESTDATA_DIR, "dummy1.tif")
    mp = Mapchete(
        MapcheteConfig(
            dict(
                process_file=process_template,
                input_files=dict(file1=dummy1),
                output=dict(
                    format="GTiff",
                    path=".",
                    type="geodetic",
                    bands=1,
                    dtype="uint8"
                ),
                config_dir=".",
                process_zoom=4
            )))
    process_tile = mp.get_process_tiles(zoom=4).next()
    # Mapchete throws a RuntimeError if process output is empty
    try:
        mp.execute(process_tile)
    except RuntimeError:
        pass
