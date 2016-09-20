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
            description="Creates an empty process and configuration file")
        parser.add_argument("mapchete_file", type=str)
        parser.add_argument("process_file", type=str)
        parser.add_argument("--out_format", "-of", type=str,
            choices=FORMATS.keys())
        parser.add_argument("--out_path", "-op", type=str)
        parser.add_argument("--out_type", "-ot", type=str, choices=TILING_TYPES)
        parser.add_argument("--force", "-f", action="store_true")
        args = parser.parse_args(sys.argv[2:])
        create_empty_process(args)

    def serve(self):
        parser = argparse.ArgumentParser(
            description="Serves a process on localhost")
        parser.add_argument("mapchete_file", type=str)
        parser.add_argument("--port", "-p", type=int)
        parser.add_argument("--zoom", "-z", type=int, nargs="*", )
        parser.add_argument("--bounds", "-b", type=float, nargs="*")
        parser.add_argument("--log", action="store_true")
        parser.add_argument("--overwrite", action="store_true")
        parser.add_argument("--input_file", type=str)
        args = parser.parse_args(sys.argv[2:])
        serve(args)

    def execute(self):
        parser = argparse.ArgumentParser(
            description="Executes a process")
        parser.add_argument("mapchete_file", type=str)
        parser.add_argument("--zoom", "-z", type=int, nargs='*', )
        parser.add_argument("--bounds", "-b", type=float, nargs='*')
        parser.add_argument("--tile", "-t", type=int, nargs=3, )
        parser.add_argument("--failed_from_log", type=str)
        parser.add_argument("--failed_since", type=str)
        parser.add_argument("--log", action="store_true")
        parser.add_argument("--overwrite", action="store_true")
        parser.add_argument("--multi", "-m", type=int)
        parser.add_argument("--create_vrt", action="store_true")
        parser.add_argument("--input_file", type=str)
        args = parser.parse_args(sys.argv[2:])
        execute(args)

    def pyramid(self):
        parser = argparse.ArgumentParser(
            description="Creates a tile pyramid from an input raster dataset")
        parser.add_argument(
            "input_raster",
            type=str,
            help="input raster file"
        )
        parser.add_argument(
            "output_dir",
            type=str,
            help="output directory where tiles are stored"
        )
        parser.add_argument(
            "--pyramid_type",
            type=str,
            default="mercator",
            choices=["geodetic", "mercator"],
            help="pyramid schema to be used"
        )
        parser.add_argument(
            "--output_format",
            type=str,
            default="GTiff",
            choices=["GTiff", "PNG"],
            help="output data type"
        )
        parser.add_argument(
            "--resampling_method",
            type=str,
            default="nearest",
            choices=[
                "nearest",
                "bilinear",
                "cubic",
                "cubic_spline",
                "lanczos",
                "average",
                "mode"
            ]
        )
        parser.add_argument(
            "--scale_method",
            type=str,
            default="minmax_scale",
            choices=["dtype_scale", "minmax_scale", "crop"],
            help="scale method if input bands have more than 8 bit"
        )
        parser.add_argument(
            "--zoom",
            "-z",
            type=int,
            nargs='*'
        )
        parser.add_argument(
            "--bounds",
            "-b",
            type=float,
            nargs='*',
            help='left bottom right top in pyramid CRS (i.e. either EPSG:4326 \
            for geodetic or EPSG:3857 for mercator)'
        )
        parser.add_argument("--log", action="store_true")
        parser.add_argument("--overwrite", action="store_true")
        args = parser.parse_args(sys.argv[2:])
        pyramid(args)

if __name__ == "__main__":
    main()
