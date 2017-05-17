#!/usr/bin/env python
"""Functions to parse through input files."""

import os
import logging
import time
from shapely.geometry import box, MultiPolygon
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool

from mapchete.formats import load_input_reader

LOGGER = logging.getLogger(__name__)


def input_files_at_zoom(process, name, element, zoom):
    """Get readers and bounding boxes for input files."""
    LOGGER.debug("get input files metadata for zoom %s" % zoom)
    # case where single input files is provided by CLI
    if element == "from_command_line":
        element = {"input_file": None}
    # get input files for current zoom level
    files_tree = process._element_at_zoom(name, element, zoom)
    # convert tree to key-value object, where path within tree is the key
    files_flat = _flatten_tree(files_tree)

    LOGGER.debug("%s files to analyze", len(files_flat))
    if len(files_flat) >= cpu_count():
        # analyze files in parallel
        start = time.time()
        f = partial(
            _file_worker, process.config_dir, process.process_pyramid,
            process.pixelbuffer
        )
        pool = Pool()
        try:
            analyzed_files = {
                key: reader
                for key, reader in pool.imap_unordered(
                    f, files_flat, chunksize=1
                )
            }
        except KeyboardInterrupt:
            LOGGER.info(
                "Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
        except Exception:
            pool.terminate()
            raise
        finally:
            pool.close()
            pool.join()

        LOGGER.debug(
            "parallel file parsing: %ss" % round(time.time() - start, 3))
    else:
        start = time.time()
        analyzed_files = {
            k: _file_worker(
                process.config_dir, process.process_pyramid,
                process.pixelbuffer, (k, v)
            )[1]
            for k, v in files_flat
        }
        LOGGER.debug(
            "sequential file parsing: %ss" % round(time.time() - start, 3))

    # create original dicionary with file readers
    input_files = _unflatten_tree(analyzed_files)

    # collect boundding boxes of inputs
    input_files_areas = [
        reader.bbox(out_crs=process.crs)
        for reader in analyzed_files.values()
        if reader is not None
    ]
    if input_files_areas:
        LOGGER.debug("intersect input files bounding boxes")
        process_area = MultiPolygon((input_files_areas)).buffer(0)
    else:
        LOGGER.debug("assume global bounding box")
        process_area = box(
            process.process_pyramid.left, process.process_pyramid.bottom,
            process.process_pyramid.right, process.process_pyramid.top
        )
    return input_files, process_area


def _flatten_tree(tree, old_path=None):
    """Flatten dict tree into dictionary where keys are paths of old dict."""
    flat_tree = []
    for key, value in tree.iteritems():
        new_path = "/".join([old_path, key]) if old_path else key
        if isinstance(value, dict):
            flat_tree.extend(_flatten_tree(value, old_path=new_path))
        else:
            flat_tree.append((new_path, value))
    return flat_tree


def _unflatten_tree(flat):
    """Reverse tree flattening."""
    tree = {}
    for key, value in flat.iteritems():
        path = key.split("/")
        # we are at the end of a branch
        if len(path) == 1:
            tree[key] = value
        # there are more branches
        else:
            # create new dict
            if not path[0] in tree:
                tree[path[0]] = _unflatten_tree({"/".join(path[1:]): value})
            # add keys to existing dict
            else:
                branch = _unflatten_tree({"/".join(path[1:]): value})
                if not path[1] in tree[path[0]]:
                    tree[path[0]][path[1]] = branch[path[1]]
                else:
                    tree[path[0]][path[1]].update(branch[path[1]])
    return tree


def _file_worker(conf_dir, pyramid, pixelbuffer, kv):
    key, location = kv
    if location not in ["none", "None", None, ""]:
        # prepare input files metadata
        LOGGER.debug("read metadata from %s" % location)
        # get absolute paths if not on S3
        path = location if location.startswith("s3://") else os.path.normpath(
            os.path.join(conf_dir, location)
        )
        LOGGER.debug("load input reader for file %s" % location)
        _input_reader = load_input_reader(dict(
                path=path, pyramid=pyramid, pixelbuffer=pixelbuffer
        ))
        LOGGER.debug(
            "input reader for file %s is %s" % (location, _input_reader)
        )
        # trigger input bounding box caches
        _input_reader.bbox(out_crs=pyramid.crs)
        return key, _input_reader
    else:
        return key, None
