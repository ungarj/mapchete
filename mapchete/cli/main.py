"""
Mapchete command line tool with subcommands.

Structure inspired by
http://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html
"""

import argparse
import sys
import tilematrix

import mapchete
from mapchete.cli.create import create_empty_process
from mapchete.cli.execute import main as execute
from mapchete.cli.formats import list_formats
from mapchete.cli.index import index
from mapchete.cli.pyramid import main as pyramid
from mapchete.cli.serve import main as serve
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
            description="Tile-based geodata processing with Python.",
            usage=(
                """mapchete <command> [<args>]"""
                """\n"""
                """\n"""
                """available commands:"""
                """\n  """
                """create         Create new process and configuration."""
                """\n  """
                """serve          Serve a process for inspection."""
                """\n  """
                """execute        Execute a process."""
                """\n  """
                """index          Create index for process output."""
                """\n  """
                """pyramid        Create a tile pyramid from an input raster."""
                """\n  """
                """formats        List available input and/or output formats."""
                """\n  """
            ))
        parser.add_argument(
            '-v', '--version', action='version', version=mapchete.__version__
        )
        parser.add_argument("command", help="Subcommand to run")
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[1:2])
        # if given command has no corresponding function, throw error
        if not hasattr(self, args.command):
            parser.error('unrecognized command "%s"' % args.command)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    def create(self):
        """Parse params and run create command."""
        parser = argparse.ArgumentParser(
            description="Create an empty process and configuration file.",
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
            choices=tilematrix._conf.PYRAMID_PARAMS.keys(), default="geodetic",
            help="output pyramid type")
        parser.add_argument(
            "--force", "-f", action="store_true",
            help="overwrite if Mapchete and process files already exist")
        create_empty_process(parser.parse_args(self.args[2:]))

    def serve(self):
        """Parse params and run serve command."""
        parser = argparse.ArgumentParser(
            description="Serve a process on localhost.",
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
        serve(parser.parse_args(self.args[2:]), _test=self._test_serve)

    def execute(self):
        """Parse params and run execute command."""
        parser = argparse.ArgumentParser(
            description="Execute a process.",
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
            "--point", "-p", type=float, nargs=2,
            help="process tiles over single point location", metavar="<float>")
        parser.add_argument(
            "--wkt_geometry", "-g", type=str,
            help="take boundaries from WKT geometry in tile pyramid CRS",
            metavar="<str>")
        parser.add_argument(
            "--tile", "-t", type=int, nargs=3,
            help="zoom, row, column of single tile", metavar="<int>")
        parser.add_argument(
            "--overwrite", "-o", action="store_true",
            help="overwrite if tile(s) already exist(s)")
        parser.add_argument(
            "--multi", "-m", type=int, help="number of concurrent processes",
            metavar="<int>")
        parser.add_argument(
            "--input_file", "-i", type=str,
            help="specify an input file via command line (in apchete file, \
                set 'input_file' parameter to 'from_command_line')",
            metavar="<path>")
        parser.add_argument(
            "--logfile", "-l", type=str, metavar="<path>",
            help="write debug log infos into file")
        parser.add_argument(
            "--verbose", "-v", action="store_true",
            help="print info for each process tile")
        parser.add_argument(
            "--no_pbar", action="store_true",
            help="don't show progress bar")
        parser.add_argument(
            "--debug", "-d", action="store_true",
            help="deactivate progress bar and print debug log output")
        parser.add_argument(
            "--max_chunksize", "-c", type=int, metavar="<int>", default=16,
            help="maximum number of process tiles to be queued for each \
                worker; (default: 1)")
        execute(parser.parse_args(self.args[2:]))

    def pyramid(self):
        """Parse params and run pyramid command."""
        parser = argparse.ArgumentParser(
            description="Create a tile pyramid from an input raster dataset.",
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
        pyramid(parser.parse_args(self.args[2:]))

    def formats(self):
        """Parse arguments and run formats command."""
        parser = argparse.ArgumentParser(
            description="List available input and/or outpup formats.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete formats")
        parser.add_argument(
            "--input_formats", "-i", action="store_true",
            help="show only input formats")
        parser.add_argument(
            "--output_formats", "-o", action="store_true",
            help="show only output formats")
        list_formats(parser.parse_args(self.args[2:]))

    def index(self):
        """Parse params and run index command."""
        parser = argparse.ArgumentParser(
            description="Create index of output tiles.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage="mapchete index <mapchete_file>")
        parser.add_argument("mapchete_file", type=str, help="Mapchete file")
        parser.add_argument(
            "--out_dir", type=str,
            help="output directory (default: output path in mapchete file")
        parser.add_argument(
            "--geojson", action="store_true",
            help="write GeoJSON index")
        parser.add_argument(
            "--gpkg", action="store_true",
            help="write GeoPackage index")
        parser.add_argument(
            "--txt", action="store_true",
            help="write text file with paths")
        parser.add_argument(
            "--fieldname", type=str, default="location",
            help="take boundaries from WKT geometry in tile pyramid CRS",
            metavar="<str>")
        parser.add_argument(
            "--basepath", type=str,
            help="use other base path than process output path",
            metavar="<str>")
        parser.add_argument(
            "--for_gdal", action="store_true",
            help="make remote paths readable by GDAL (not applied for txt output)")
        parser.add_argument(
            "--zoom", "-z", type=int, nargs='*',
            help="either minimum and maximum zoom level or just one zoom level",
            metavar="<int>")
        parser.add_argument(
            "--bounds", "-b", type=float, nargs=4,
            help="left, bottom, right, top bounds in tile pyramid CRS",
            metavar="<float>")
        parser.add_argument(
            "--wkt_geometry", "-g", type=str,
            help="take boundaries from WKT geometry in tile pyramid CRS",
            metavar="<str>")
        parser.add_argument(
            "--tile", "-t", type=int, nargs=3,
            help="zoom, row, column of single tile", metavar="<int>")
        parser.add_argument(
            "--verbose", "-v", action="store_true",
            help="print info for each process tile")
        parser.add_argument(
            "--debug", "-d", action="store_true",
            help="deactivate progress bar and print debug log output")
        index(parser.parse_args(self.args[2:]))


if __name__ == "__main__":
    main(sys.argv)
