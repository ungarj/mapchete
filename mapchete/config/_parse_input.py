"""Functions to parse through input items."""

import os
import logging
import six
import time
from shapely.geometry import box
from shapely.ops import cascaded_union
from shapely.wkt import dumps
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool

from mapchete.formats import load_input_reader
from mapchete.errors import MapcheteDriverError, MapcheteConfigError

LOGGER = logging.getLogger(__name__)


def input_at_zoom(config, name, element, zoom, readonly):
    """Get readers and bounding boxes for input."""
    LOGGER.debug("get input items metadata for zoom %s", zoom)
    # case where a single input file is provided by CLI
    if element == "from_command_line":
        element = {"input": None}
    elif element is None:
        return dict(), box(
            config.process_pyramid.left, config.process_pyramid.bottom,
            config.process_pyramid.right, config.process_pyramid.top
        )
    # get inputs for current zoom level
    input_tree = config._element_at_zoom(name, element, zoom)
    # convert tree to key-value object, where path within tree is the key
    input_flat = _flatten_tree(input_tree)
    # select inputs not yet cached
    new_inputs = []
    cached_inputs = {}
    for name, input_obj in input_flat:
        if str(input_obj) in config.inputs:
            cached_inputs[name] = config.inputs[str(input_obj)]
        else:
            new_inputs.append((name, input_obj))
    LOGGER.debug("%s new inputs to analyze", len(new_inputs))
    if len(new_inputs) >= cpu_count():
        # analyze inputs in parallel
        start = time.time()
        f = partial(
            _input_worker, config.config_dir, config.process_pyramid,
            config.pixelbuffer, config._delimiters, readonly
        )
        pool = Pool()
        try:
            analyzed_inputs = {
                key: input_obj_reader
                for key, input_obj_reader in pool.imap_unordered(
                    f, new_inputs,
                    chunksize=int(1 + len(new_inputs) / cpu_count())
                )
            }
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
                config.config_dir, config.process_pyramid,
                config.pixelbuffer, config._delimiters, readonly, (k, v)
            )[1]
            for k, v in new_inputs
        }
        LOGGER.debug(
            "sequential input parsing: %ss" % round(time.time() - start, 3))

    # create original dicionary with input readers
    analyzed_readers = {}
    for name, (input_obj, reader) in six.iteritems(analyzed_inputs):
        config.inputs[str(input_obj)] = reader
        analyzed_readers[name] = reader
    analyzed_readers.update(cached_inputs)
    input_ = _unflatten_tree(analyzed_readers)
    # collect bounding boxes of inputs
    input_areas = [
        reader.bbox(out_crs=config.crs)
        for reader in analyzed_readers.values()
        if reader is not None
    ]
    if input_areas:
        LOGGER.debug("union input bounding boxes")
        id_ = frozenset([dumps(i) for i in input_areas])
        if id_ not in config._process_area_cache:
            config._process_area_cache[id_] = cascaded_union(input_areas)
        process_area = config._process_area_cache[id_]
    else:
        LOGGER.debug("assume global bounding box")
        process_area = box(
            config.process_pyramid.left, config.process_pyramid.bottom,
            config.process_pyramid.right, config.process_pyramid.top
        )
    return input_, process_area


def _flatten_tree(tree, old_path=None):
    """Flatten dict tree into dictionary where keys are paths of old dict."""
    flat_tree = []
    for key, value in six.iteritems(tree):
        new_path = "/".join([old_path, key]) if old_path else key
        if isinstance(value, dict) and "format" not in value:
            flat_tree.extend(_flatten_tree(value, old_path=new_path))
        else:
            flat_tree.append((new_path, value))
    return flat_tree


def _unflatten_tree(flat):
    """Reverse tree flattening."""
    tree = {}
    for key, value in six.iteritems(flat):
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


def _input_worker(conf_dir, pyramid, pixelbuffer, delimiters, readonly, kv):
    try:
        key, input_obj = kv
        if input_obj not in ["none", "None", None, ""]:
            # prepare input metadata
            LOGGER.debug("read metadata from %s", input_obj)
            # for single file inputs
            if isinstance(input_obj, six.string_types):
                # get absolute paths if not remote
                path = input_obj if input_obj.startswith(
                    ("s3://", "https://", "http://")) else os.path.normpath(
                    os.path.join(conf_dir, input_obj)
                )
                LOGGER.debug("load input reader for file %s",  input_obj)
                _input_reader = load_input_reader(
                    dict(
                        path=path, pyramid=pyramid, pixelbuffer=pixelbuffer,
                        delimiters=delimiters
                    ),
                    readonly
                )
                LOGGER.debug(
                    "input reader for file %s is %s", input_obj, _input_reader
                )
            # for abstract inputs
            elif isinstance(input_obj, dict):
                LOGGER.debug(
                    "load input reader for abstract input %s", input_obj
                )
                _input_reader = load_input_reader(
                    dict(
                        abstract=input_obj, pyramid=pyramid,
                        pixelbuffer=pixelbuffer, delimiters=delimiters,
                        conf_dir=conf_dir
                    ),
                    readonly
                )
                LOGGER.debug(
                    "input reader for abstract input %s is %s", input_obj,
                    _input_reader
                )
            else:
                raise MapcheteConfigError(
                    "invalid input type %s", type(input_obj))
            # trigger input bounding box caches
            _input_reader.bbox(out_crs=pyramid.crs)
            return key, (input_obj, _input_reader)
        else:
            return key, (None, None)
    except Exception as e:
        LOGGER.exception("input driver error")
        raise MapcheteDriverError("%s could not be read: %s" % (key, e))
