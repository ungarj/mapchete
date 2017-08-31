#!/usr/bin/env python
"""Functions to parse through input items."""

import os
import logging
import time
from sets import ImmutableSet
from shapely.geometry import box
from shapely.ops import unary_union
from shapely.wkt import dumps
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool

from mapchete.formats import load_input_reader

LOGGER = logging.getLogger(__name__)


def input_at_zoom(process, name, element, zoom):
    """Get readers and bounding boxes for input."""
    print name, element
    LOGGER.debug("get input items metadata for zoom %s" % zoom)
    # case where single input files is provided by CLI
    if element == "from_command_line":
        element = {"input_file": None}
    elif element is None:
        return dict(), box(
            process.process_pyramid.left, process.process_pyramid.bottom,
            process.process_pyramid.right, process.process_pyramid.top
        )
    # get input files for current zoom level
    print name, element
    files_tree = process._element_at_zoom(name, element, zoom)
    print files_tree
    # convert tree to key-value object, where path within tree is the key
    files_flat = _flatten_tree(files_tree)
    # select files not yet cached
    new_files = []
    cached_files = {}
    for name, path in files_flat:
        if path in process._input_cache:
            cached_files[name] = process._input_cache[path]
        else:
            new_files.append((name, path))

    LOGGER.debug("%s new files to analyze", len(new_files))
    if len(new_files) >= cpu_count():
        # analyze files in parallel
        start = time.time()
        f = partial(
            _file_worker, process.config_dir, process.process_pyramid,
            process.pixelbuffer
        )
        pool = Pool()
        try:
            analyzed_files = {
                key: location_reader
                for key, location_reader in pool.imap_unordered(
                    f, new_files, chunksize=8
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
            for k, v in new_files
        }
        LOGGER.debug(
            "sequential file parsing: %ss" % round(time.time() - start, 3))

    # create original dicionary with file readers
    analyzed_readers = {}
    for name, (location, reader) in analyzed_files.iteritems():
        process._input_cache[location] = reader
        analyzed_readers[name] = reader
    analyzed_readers.update(cached_files)
    input_ = _unflatten_tree(analyzed_readers)

    # collect boundding boxes of inputs
    input_areas = [
        reader.bbox(out_crs=process.crs)
        for reader in analyzed_readers.values()
        if reader is not None
    ]
    if input_areas:
        LOGGER.debug("intersect input files bounding boxes")
        id_ = ImmutableSet([dumps(i) for i in input_areas])
        if id_ not in process._process_area_cache:
            process._process_area_cache[id_] = unary_union(input_areas)
        process_area = process._process_area_cache[id_]
    else:
        LOGGER.debug("assume global bounding box")
        process_area = box(
            process.process_pyramid.left, process.process_pyramid.bottom,
            process.process_pyramid.right, process.process_pyramid.top
        )
    return input_, process_area


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
        # get absolute paths if not remote
        path = location if location.startswith(
            ("s3://", "https://", "http://")) else os.path.normpath(
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
        return key, (location, _input_reader)
    else:
        return key, (None, None)
