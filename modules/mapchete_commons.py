#!/usr/bin/env python

import os
import sys

def create_and_clean_dirs(tile, parsed, extension):

    output_folder = parsed.output_folder[0]
    zoom, col, row = tile

    # Create output folders if not existing.
    # OGC Standard:
    # {TileMatrixSet}/{TileMatrix}/{TileRow}/{TileCol}.png
    basedir = output_folder
    zoomdir = os.path.join(basedir, str(zoom))
    rowdir = os.path.join(zoomdir, str(row))
    out_tile = tile_path(tile, parsed, extension)
    # Create output folders
    if not os.path.exists(basedir):
        os.makedirs(basedir)
    if not os.path.exists(zoomdir):
        os.makedirs(zoomdir)
    if not os.path.exists(rowdir):
        os.makedirs(rowdir)

    if os.path.exists(out_tile):
        os.remove(out_tile)


def tile_path(tile, parsed, extension):

    output_folder = parsed.output_folder[0]
    zoom, col, row = tile

    basedir = output_folder
    zoomdir = os.path.join(basedir, str(zoom))
    rowdir = os.path.join(zoomdir, str(row))
    out_tile = os.path.join(rowdir, (str(col) + extension))

    return out_tile