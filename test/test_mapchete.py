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

import mapchete
from mapchete.tile import BufferedTile
from mapchete.io.raster import create_mosaic
from mapchete.errors import MapcheteProcessOutputError

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_empty_execute():
    """Execute process outside of defined zoom levels."""
    try:
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mp:
            out_tile = mp.execute((6, 0, 0))
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
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete")
        ) as mp:
            tile = (5, 0, 0)
            # process and save
            mp.write(mp.get_raw_output(tile))
            # read written data
            out_tile = mp.read(tile)
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
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mp:
            out_tile = mp.get_raw_output((6, 0, 0))
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
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"),
            mode="memory"
        ) as mp:
            assert mp.config.mode == "memory"
            out_tile = mp.get_raw_output((5, 0, 0))
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
        tile = (5, 0, 0)
        readonly_mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"),
            mode="readonly")
        write_mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"),
            mode="continue")

        # read non-existing data (returns empty)
        out_tile = readonly_mp.get_raw_output(tile)
        assert out_tile.data.mask.all()

        # try to process and save empty data
        try:  # TODO
            readonly_mp.write(readonly_mp.get_raw_output(tile))
        except ValueError:
            pass

        # actually process and save
        write_mp.write(write_mp.get_raw_output(tile))

        # read written output
        out_tile = readonly_mp.get_raw_output(tile)
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
        mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
        assert mp.config.mode == "continue"
        tile = (5, 0, 0)
        # process and save
        mp.write(mp.get_raw_output(tile))
        # read written data
        out_tile = mp.get_raw_output(tile)
        assert not out_tile.data.mask.all()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_get_raw_output_reproject():
    """Get process output from a different CRS."""
    try:
        mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
        assert mp.config.mode == "continue"
        # TODO implement function
        mp.get_raw_output((5, 0, 0))
    except NotImplementedError:
        pass
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except OSError:
            pass


def test_baselevels():
    """Baselevel interpolation."""
    try:
        mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete"),
            mode="continue")
        # get tile from lower zoom level
        lower_tile = mp.get_process_tiles(4).next()
        # process and save
        for tile in lower_tile.get_children():
            output = mp.get_raw_output(tile)
            mp.write(output)
        # read from baselevel
        out_tile = mp.get_raw_output(lower_tile)
        assert not out_tile.data.mask.all()

        # get tile from higher zoom level
        tile = mp.get_process_tiles(6).next()
        # process and save
        output = mp.get_raw_output(tile)
        mp.write(output)
        # read from baselevel
        assert any([
            not mp.get_raw_output(upper_tile).data.mask.all()
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
        mp = mapchete.open(os.path.join(SCRIPTDIR, cleantopo_process))
        for zoom in range(6):
            tiles = []
            for tile in mp.get_process_tiles(zoom):
                output = mp.execute(tile)
                tiles.append(output)
                assert isinstance(output, BufferedTile)
                assert isinstance(output.data, ma.MaskedArray)
                assert output.data.shape == output.shape
                assert not ma.all(output.data.mask)
                mp.write(output)
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
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
    assert dumps(mp)
    assert dumps(mp.config)
    assert dumps(mp.config.output)
    for tile in mp.get_process_tiles():
        assert dumps(tile)
    f = partial(_worker, mp)
    try:
        pool = Pool()
        for zoom in reversed(mp.config.zoom_levels):
            for raw_output in pool.imap_unordered(
                f, mp.get_process_tiles(zoom), chunksize=8
            ):
                mp.write(raw_output)
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


def _worker(mp, tile):
    """Multiprocessing worker processing a tile."""
    return mp.execute(tile)


def test_write_empty():
    """Test write function when passing an empty process_tile."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
    # process and save
    mp.write((5, 0, 0))


def test_process_template():
    """Template used to create an empty process."""
    process_template = pkg_resources.resource_filename(
        "mapchete.static", "process_template.py")
    dummy1 = os.path.join(TESTDATA_DIR, "dummy1.tif")
    mp = mapchete.open(
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
        ))
    process_tile = mp.get_process_tiles(zoom=4).next()
    # Mapchete throws a RuntimeError if process output is empty
    try:
        mp.execute(process_tile)
    except MapcheteProcessOutputError:
        pass


def test_batch_process():
    """Test batch_process function."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
    try:
        # invalid parameters errors
        try:
            mp.batch_process(zoom=1, tile=(1, 0, 0))
            raise Exception
        except ValueError:
            pass
        try:
            mp.batch_process(debug=True, quiet=True)
            raise Exception
        except ValueError:
            pass
        # process single tile
        mp.batch_process(tile=(2, 0, 0))
        mp.batch_process(tile=(2, 0, 0), quiet=True)
        mp.batch_process(tile=(2, 0, 0), debug=True)
        # process using multiprocessing
        mp.batch_process(zoom=2, multi=2)
        # process without multiprocessing
        mp.batch_process(zoom=2, multi=1)
    finally:
        try:
            shutil.rmtree(OUT_DIR)
        except Exception:
            pass
