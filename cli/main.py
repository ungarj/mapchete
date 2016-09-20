#!/usr/bin/env python
"""
This is the main entry point from the command line to all mapchete subcommands.
"""

import sys
import argparse

from cli.execute import main as execute
from cli.serve import main as serve
from cli.pyramid import main as pyramid
from cli.create import create_empty_process
from mapchete.io_utils.formats import FORMATS, TILING_TYPES

def main():
    MapcheteCLI()
    print "files created successfully"

class MapcheteCLI(object):
    """
    From http://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html
    """
    def __init__(self):
        parser = argparse.ArgumentParser(
            description="Mapchete helps developing and running geoprocesses.",
            usage="""mapchete <command> [<args>]

  create      Creates an empty process and configuration file
  serve       Serves a process on localhost
  execute     Executes a process
  pyramid     Creates a tile pyramid from an input raster dataset
            """)
        parser.add_argument("command", help="Subcommand to run")
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print "Unrecognized command"
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    def create(self):
        parser = argparse.ArgumentParser(
            description="Creates an empty process and configuration file",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete create <mapchete_file> <process_file>")
        parser.add_argument("mapchete_file", type=str, help="Mapchete file")
        parser.add_argument("process_file", type=str,
            help="process (Python) file")
        parser.add_argument("--out_format", "-of", type=str,
            choices=FORMATS.keys(), help="process output format")
        parser.add_argument("--out_path", "-op", type=str,
            help="path for process output", metavar="<path>")
        parser.add_argument("--pyramid_type", "-pt", type=str, choices=TILING_TYPES,
            default="geodetic", help="output pyramid type")
        parser.add_argument("--force", "-f", action="store_true",
            help="overwrite if Mapchete and process files already exist")
        args = parser.parse_args(sys.argv[2:])
        create_empty_process(args)

    def serve(self):
        parser = argparse.ArgumentParser(
            description="Serves a process on localhost",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete serve <mapchete_file>")
        parser.add_argument("mapchete_file", type=str,
            help="Mapchete file")
        parser.add_argument("--port", "-p", type=int,
            help="port process is hosted on",
            metavar="<int>")
        parser.add_argument("--zoom", "-z", type=int, nargs='*',
            help="either minimum and maximum zoom level or just one zoom level",
            metavar="<int>")
        parser.add_argument("--bounds", "-b", type=float, nargs=4,
            help="left, bottom, right, top bounds in tile pyramid CRS",
            metavar="<float>")
        parser.add_argument("--overwrite", "-o", action="store_true",
            help="overwrite if tile(s) already exist(s)")
        parser.add_argument("--input_file", "-i", type=str,
            help="specify an input file via command line (in Mapchete file, \
set 'input_file' parameter to 'from_command_line')",
            metavar="<path>")
        args = parser.parse_args(sys.argv[2:])
        serve(args)

    def execute(self):
        parser = argparse.ArgumentParser(
            description="Executes a process",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete execute <mapchete_file>")
        parser.add_argument("mapchete_file", type=str,
            help="Mapchete file"
            )
        parser.add_argument("--zoom", "-z", type=int, nargs='*',
            help="either minimum and maximum zoom level or just one zoom level",
            metavar="<int>")
        parser.add_argument("--bounds", "-b", type=float, nargs=4,
            help="left, bottom, right, top bounds in tile pyramid CRS",
            metavar="<float>")
        parser.add_argument("--tile", "-t", type=int, nargs=3,
            help="zoom, row, column of single tile",
            metavar="<int>")
        parser.add_argument("--failed_from_log", type=str,
            help="process failed tiles from log file",
            metavar="<path>")
        parser.add_argument("--failed_since", type=str,
            help="furthermore filter failed tiles by time (e.g. 2016-09-20)",
            metavar="<date>")
        parser.add_argument("--overwrite", "-o", action="store_true",
            help="overwrite if tile(s) already exist(s)")
        parser.add_argument("--multi", "-m", type=int,
            help="number of concurrent processes",
            metavar="<int>")
        parser.add_argument("--create_vrt", action="store_true",
            help="if raster output, this option creates a VRT for each zoom \
                level")
        parser.add_argument("--input_file", "-i", type=str,
            help="specify an input file via command line (in apchete file, \
                set 'input_file' parameter to 'from_command_line')",
            metavar="<path>")
        args = parser.parse_args(sys.argv[2:])
        execute(args)

    def pyramid(self):
        parser = argparse.ArgumentParser(
            description="Creates a tile pyramid from an input raster dataset",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete pyramid <raster_file>")
        parser.add_argument("input_raster", type=str, help="input raster file")
        parser.add_argument(
            "output_dir", type=str,
            help="output directory where tiles are stored")
        parser.add_argument(
            "--pyramid_type", "-pt", type=str, default="mercator",
            choices=["geodetic", "mercator"],
            help="pyramid schema to be used")
        parser.add_argument("--output_format", "-of", type=str, default="GTiff",
            choices=["GTiff", "PNG"], help="output data format (GTiff or PNG)",
            metavar="<str>")
        parser.add_argument("--resampling_method", "-r", type=str,
            default="nearest", choices=["nearest", "bilinear", "cubic",
                "cubic_spline", "lanczos", "average", "mode"],
                help="resampling method to be used (nearest, bilinear, cubic, \
                    cubic_spline, lanczos, average or mode)",
            metavar="<str>")
        parser.add_argument("--scale_method", "-s", type=str,
            default="minmax_scale", choices=["dtype_scale", "minmax_scale",
                "crop"],
            help="scale method if input bands have more than 8 bit \
                (dtype_scale, minmax_scale or crop)",
            metavar="<str>")
        parser.add_argument("--zoom", "-z", type=int, nargs='*',
            help="either minimum and maximum zoom level or just one zoom level",
            metavar="<int>")
        parser.add_argument("--overwrite", "-o", action="store_true",
            help="overwrite if tile(s) already exist(s)")
        args = parser.parse_args(sys.argv[2:])
        pyramid(args)

if __name__ == "__main__":
    main()
