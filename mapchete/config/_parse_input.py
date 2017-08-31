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
    LOGGER.debug("get input items metadata for zoom %s" % zoom)
    # case where a single input file is provided by CLI
    if element == "from_command_line":
        element = {"input_file": None}
    elif element is None:
        return dict(), box(
            process.process_pyramid.left, process.process_pyramid.bottom,
            process.process_pyramid.right, process.process_pyramid.top
        )
    # get inputs for current zoom level
    input_tree = process._element_at_zoom(name, element, zoom)
    # convert tree to key-value object, where path within tree is the key
    input_flat = _flatten_tree(input_tree)
    # select inputs not yet cached
    new_inputs = []
    cached_inputs = {}
    for name, input_obj in input_flat:
        if str(input_obj) in process._input_cache:
            cached_inputs[name] = process._input_cache[str(input_obj)]
        else:
            new_inputs.append((name, input_obj))

    LOGGER.debug("%s new inputs to analyze", len(new_inputs))
    if len(new_inputs) >= cpu_count():
        # analyze inputs in parallel
        start = time.time()
        f = partial(
            _input_worker, process.config_dir, process.process_pyramid,
            process.pixelbuffer
        )
        pool = Pool()
        try:
            analyzed_inputs = {
                key: input_obj_reader
                for key, input_obj_reader in pool.imap_unordered(
                    f, new_inputs, chunksize=8
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
            "parallel input parsing: %ss" % round(time.time() - start, 3))
    else:
        start = time.time()
        analyzed_inputs = {
            k: _input_worker(
                process.config_dir, process.process_pyramid,
                process.pixelbuffer, (k, v)
            )[1]
            for k, v in new_inputs
        }
        LOGGER.debug(
            "sequential input parsing: %ss" % round(time.time() - start, 3))

    # create original dicionary with input readers
    analyzed_readers = {}
    for name, (input_obj, reader) in analyzed_inputs.iteritems():
        process._input_cache[str(input_obj)] = reader
        analyzed_readers[name] = reader
    analyzed_readers.update(cached_inputs)
    input_ = _unflatten_tree(analyzed_readers)

    # collect boundding boxes of inputs
    input_areas = [
        reader.bbox(out_crs=process.crs)
        for reader in analyzed_readers.values()
        if reader is not None
    ]
    if input_areas:
        LOGGER.debug("intersect input bounding boxes")
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
        if isinstance(value, dict) and "format" not in value:
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


def _input_worker(conf_dir, pyramid, pixelbuffer, kv):
    key, input_obj = kv
    if input_obj not in ["none", "None", None, ""]:
        # prepare input metadata
        LOGGER.debug("read metadata from %s" % input_obj)
        # for single file inputs
        if isinstance(input_obj, str):
            # get absolute paths if not remote
            path = input_obj if input_obj.startswith(
                ("s3://", "https://", "http://")) else os.path.normpath(
                os.path.join(conf_dir, input_obj)
            )
            LOGGER.debug("load input reader for file %s" % input_obj)
            _input_reader = load_input_reader(dict(
                path=path, pyramid=pyramid, pixelbuffer=pixelbuffer
            ))
            LOGGER.debug(
                "input reader for file %s is %s" % (input_obj, _input_reader)
            )
        # for abstract inputs
        elif isinstance(input_obj, dict):
            LOGGER.debug("load input reader for abstract input %s" % input_obj)
            _input_reader = load_input_reader(dict(
                abstract=input_obj, pyramid=pyramid, pixelbuffer=pixelbuffer
            ))
            LOGGER.debug(
                "input reader for abstract input %s is %s" % (
                    input_obj, _input_reader
                )
            )
        # trigger input bounding box caches
        _input_reader.bbox(out_crs=pyramid.crs)
        return key, (input_obj, _input_reader)
    else:
        return key, (None, None)
