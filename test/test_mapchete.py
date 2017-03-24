#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import os
import shutil
import rasterio
import numpy.ma as ma
from cPickle import dumps
from functools import partial
from multiprocessing import Pool

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTile
from mapchete.io.raster import create_mosaic

scriptdir = os.path.dirname(os.path.realpath(__file__))
out_dir = os.path.join(scriptdir, "testdata/tmp")


def test_processing():
    """Test correct processing (read and write) outputs."""
    for cleantopo_process in [
        "testdata/cleantopo_tl.mapchete", "testdata/cleantopo_br.mapchete"
    ]:
        process = Mapchete(
            MapcheteConfig(os.path.join(scriptdir, cleantopo_process)))
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
                temp_vrt = os.path.join(out_dir, str(zoom)+".vrt")
                gdalbuildvrt = "gdalbuildvrt %s %s/%s/*/*.tif > /dev/null" % (
                    temp_vrt, out_dir, zoom)
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
            except:
                raise
            finally:
                try:
                    os.remove(temp_vrt)
                    shutil.rmtree(out_dir)
                except:
                    pass


def test_multiprocessing():
    """Test parallel tile processing."""
    process = Mapchete(
        MapcheteConfig(os.path.join(
            scriptdir, "testdata/cleantopo_tl.mapchete")))
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
    except:
        raise
    finally:
        pool.close()
        pool.join()
        try:
            shutil.rmtree(out_dir)
        except:
            pass


def _worker(process, process_tile):
    """Multiprocessing worker processing a tile."""
    return process.execute(process_tile)
