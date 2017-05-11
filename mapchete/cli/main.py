#!/usr/bin/env python
"""
Mapchete command line tool with subcommands.

Structure inspired by
http://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html
"""

import sys
import argparse

import mapchete
from mapchete.cli.execute import main as execute
from mapchete.cli.serve import main as serve
from mapchete.cli.pyramid import main as pyramid
from mapchete.cli.create import create_empty_process
from mapchete.cli.formats import list_formats
from mapchete.formats import available_output_formats


def main(args=None, _test_serve=False):
    """Pass on command line arguments."""
    args = args or sys.argv
    MapcheteCLI(args, _test_serve=_test_serve)


class MapcheteCLI(object):
    """Entry point to 'mapchete' command."""

    def __init__(self, args=None, _test_serve=False):
        """Initialize command line tool."""
        self.args = args
        self._test_serve = _test_serve
        parser = argparse.ArgumentParser(
            description="Mapchete helps developing and running geoprocesses.",
            usage=(
                """mapchete <command> [<args>]"""
                """\n"""
                """\n    """
                """create      Creates empty process and configuration files."""
                """\n    """
                """serve       Locally serves a process."""
                """\n    """
                """execute     Executes a process."""
                """\n    """
                """pyramid     Creates a tile pyramid from an input raster."""
                """\n    """
                """formats     Lists available input and/or output formats."""
            ))
        parser.add_argument("command", help="Subcommand to run")
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[1:2])
        if not hasattr(self, args.command):
            print "Unrecognized command"
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    def create(self):
        """Parse params and run create command."""
        parser = argparse.ArgumentParser(
            description="Creates an empty process and configuration file",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage=(
                """mapchete create <mapchete_file> <process_file> """
                """<out_format>""")
            )
        parser.add_argument("mapchete_file", type=str, help="Mapchete file")
        parser.add_argument(
            "process_file", type=str, help="process (Python) file")
        parser.add_argument(
            "out_format", type=str, choices=available_output_formats(),
            help="process output format")
        parser.add_argument(
            "--out_path", "-op", type=str, help="path for process output",
            metavar="<path>")
        parser.add_argument(
            "--pyramid_type", "-pt", type=str,
            choices=mapchete.config.TILING_TYPES, default="geodetic",
            help="output pyramid type")
        parser.add_argument(
            "--force", "-f", action="store_true",
            help="overwrite if Mapchete and process files already exist")
        args = parser.parse_args(self.args[2:])
        create_empty_process(args)

    def serve(self):
        """Parse params and run serve command."""
        parser = argparse.ArgumentParser(
            description="Serves a process on localhost",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete serve <mapchete_file>")
        parser.add_argument(
            "mapchete_file", type=str, help="Mapchete file")
        parser.add_argument(
            "--debug", "-d", action="store_true",
            help=(
                """print more debug information and allow Flask to return """
                """stack trace""")
            )
        parser.add_argument(
            "--port", "-p", type=int, help="port process is hosted on",
            metavar="<int>", default=5000)
        parser.add_argument(
            "--internal_cache", "-c", type=int,
            help="number of web tiles to be cached in RAM",
            metavar="<int>", default=1024)
        parser.add_argument(
            "--zoom", "-z", type=int, nargs='*',
            help="either minimum and maximum zoom level or just one zoom level",
            metavar="<int>")
        parser.add_argument(
            "--bounds", "-b", type=float, nargs=4,
            help="left, bottom, right, top bounds in tile pyramid CRS",
            metavar="<float>")
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument(
            "--overwrite", "-o", action="store_true",
            help="overwrite if tile(s) already exist(s)")
        mode.add_argument(
            "--readonly", "-ro", action="store_true",
            help="just read process output without writing")
        mode.add_argument(
            "--memory", "-mo", action="store_true",
            help="always get output from freshly processed output")
        parser.add_argument(
            "--input_file", "-i", type=str, help=(
                """specify an input file via command line (in Mapchete file, """
                """set 'input_file' parameter to 'from_command_line')"""),
            metavar="<path>")
        args = parser.parse_args(self.args[2:])
        serve(args, _test=self._test_serve)

    def execute(self):
        """Parse params and run execute command."""
        parser = argparse.ArgumentParser(
            description="Executes a process",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete execute <mapchete_file>")
        parser.add_argument("mapchete_file", type=str, help="Mapchete file")
        parser.add_argument(
            "--zoom", "-z", type=int, nargs='*',
            help="either minimum and maximum zoom level or just one zoom level",
            metavar="<int>")
        parser.add_argument(
            "--bounds", "-b", type=float, nargs=4,
            help="left, bottom, right, top bounds in tile pyramid CRS",
            metavar="<float>")
        parser.add_argument(
            "--tile", "-t", type=int, nargs=3,
            help="zoom, row, column of single tile", metavar="<int>")
        parser.add_argument(
            "--failed_from_log", type=str,
            help="process failed tiles from log file", metavar="<path>")
        parser.add_argument(
            "--failed_since", type=str,
            help="furthermore filter failed tiles by time (e.g. 2016-09-20)",
            metavar="<date>")
        parser.add_argument(
            "--overwrite", "-o", action="store_true",
            help="overwrite if tile(s) already exist(s)")
        parser.add_argument(
            "--multi", "-m", type=int, help="number of concurrent processes",
            metavar="<int>")
        parser.add_argument(
            "--create_vrt", action="store_true",
            help="if raster output, this option creates a VRT for each zoom \
                level")
        parser.add_argument(
            "--input_file", "-i", type=str,
            help="specify an input file via command line (in apchete file, \
                set 'input_file' parameter to 'from_command_line')",
            metavar="<path>")
        parser.add_argument(
            "--quiet", "-q", action="store_true",
            help="suppress progress bar and log output")
        parser.add_argument(
            "--debug", "-d", action="store_true",
            help="show log output and disable progress bar")

        args = parser.parse_args(self.args[2:])
        execute(args)

    def pyramid(self):
        """Parse params and run pyramid command."""
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
        parser.add_argument(
            "--output_format", "-of", type=str, default="GTiff",
            choices=["GTiff", "PNG"], help="output data format (GTiff or PNG)",
            metavar="<str>")
        parser.add_argument(
            "--resampling_method", "-r", type=str, default="nearest",
            choices=[
                "nearest", "bilinear", "cubic", "cubic_spline", "lanczos",
                "average", "mode"],
            help="resampling method to be used (nearest, bilinear, cubic, \
                    cubic_spline, lanczos, average or mode)",
            metavar="<str>")
        parser.add_argument(
            "--scale_method", "-s", type=str, default=None,
            choices=["dtype_scale", "minmax_scale", "crop", None],
            help="scale method if input bands have more than 8 bit \
                (dtype_scale, minmax_scale or crop)",
            metavar="<str>")
        parser.add_argument(
            "--zoom", "-z", type=int, nargs='*',
            help="either minimum and maximum zoom level or just one zoom level",
            metavar="<int>")
        parser.add_argument(
            "--overwrite", "-o", action="store_true",
            help="overwrite if tile(s) already exist(s)")
        args = parser.parse_args(self.args[2:])
        pyramid(args)

    def formats(self):
        """Parse arguments and run formats command."""
        parser = argparse.ArgumentParser(
            description="Lists available input and/or outpup formats",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete formats")
        parser.add_argument(
            "--input_formats", "-i", action="store_true",
            help="show only input formats")
        parser.add_argument(
            "--output_formats", "-o", action="store_true",
            help="show only output formats")
        args = parser.parse_args(self.args[2:])
        list_formats(args)


if __name__ == "__main__":
    main(sys.argv)
