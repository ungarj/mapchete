#!/usr/bin/env python
"""
Utility to create tile pyramids out of an input raster.

Also provides various options to rescale data if necessary
"""

import os
import rasterio

import mapchete
from mapchete.io import get_best_zoom_level

# ranges from rasterio
# https://github.com/mapbox/rasterio/blob/master/rasterio/dtypes.py#L61
DTYPE_RANGES = {
    'uint8': (0, 255),
    'uint16': (0, 65535),
    'int16': (-32768, 32767),
    'uint32': (0, 4294967295),
    'int32': (-2147483648, 2147483647),
    'float32': (-3.4028235e+38, 3.4028235e+38),
    'float64': (-1.7976931348623157e+308, 1.7976931348623157e+308)
}


def main(args=None):
    """Create tile pyramid out of input raster."""
    parsed = args
    options = {}
    bounds = parsed.bounds if ("bounds" in parsed) else None
    options.update(
        pyramid_type=parsed.pyramid_type, scale_method=parsed.scale_method,
        output_format=parsed.output_format,
        resampling=parsed.resampling_method, zoom=parsed.zoom, bounds=bounds,
        overwrite=parsed.overwrite)
    raster2pyramid(
        parsed.input_raster, parsed.output_dir, options)


def raster2pyramid(
    input_file,
    output_dir,
    options
):
    """Create a tile pyramid out of an input raster dataset."""
    pyramid_type = options["pyramid_type"]
    scale_method = options["scale_method"]
    output_format = options["output_format"]
    resampling = options["resampling"]
    zoom = options["zoom"]
    bounds = options["bounds"]
    mode = "overwrite" if options["overwrite"] else "continue"

    # Prepare process parameters
    minzoom, maxzoom = _get_zoom(zoom, input_file, pyramid_type)
    process_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "tilify.py")

    with rasterio.open(input_file, "r") as input_raster:
        output_bands = input_raster.count
        input_dtype = input_raster.dtypes[0]
        output_dtype = input_raster.dtypes[0]
        nodataval = input_raster.nodatavals[0]
        if not nodataval:
            nodataval = 0
        if output_format == "PNG":
            if output_bands > 3:
                output_bands = 3
                output_dtype = 'uint8'
        scales_minmax = ()
        if scale_method == "dtype_scale":
            for index in range(1, output_bands+1):
                scales_minmax += (DTYPE_RANGES[input_dtype], )
        elif scale_method == "minmax_scale":
            for index in range(1, output_bands+1):
                band = input_raster.read(index)
                scales_minmax += ((band.min(), band.max()), )
        elif scale_method == "crop":
            for index in range(1, output_bands+1):
                scales_minmax += ((0, 255), )
        if input_dtype == "uint8":
            scale_method = None
            scales_minmax = ()
            for index in range(1, output_bands+1):
                scales_minmax += ((None, None), )

    # Create configuration
    config = dict(
        process_file=process_file,
        output={
            "path": output_dir,
            "format": output_format,
            "type": pyramid_type,
            "bands": output_bands,
            "dtype": output_dtype
            },
        scale_method=scale_method,
        scales_minmax=scales_minmax,
        input_files={"raster": input_file},
        config_dir=os.getcwd(),
        process_minzoom=minzoom,
        process_maxzoom=maxzoom,
        nodataval=nodataval,
        resampling=resampling,
        bounds=bounds,
        pixelbuffer=5,
        baselevel={"zoom": maxzoom, "resampling": resampling},
        mode=mode
    )

    # create process
    with mapchete.open(config, zoom=zoom, bounds=bounds) as mp:
        # prepare output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        # run process
        mp.batch_process(zoom=[minzoom, maxzoom])


def _get_zoom(zoom, input_raster, pyramid_type):
    """Determine minimum and maximum zoomlevel."""
    if not zoom:
        minzoom = 1
        maxzoom = get_best_zoom_level(input_raster, pyramid_type)
    elif len(zoom) == 1:
        minzoom = zoom[0]
        maxzoom = zoom[0]
    elif len(zoom) == 2:
        if zoom[0] < zoom[1]:
            minzoom = zoom[0]
            maxzoom = zoom[1]
        else:
            minzoom = zoom[1]
            maxzoom = zoom[0]
    return minzoom, maxzoom
