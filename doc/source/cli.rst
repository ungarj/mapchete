==================
Command Line Tools
==================

Mapchete offers various useful subcommands:

.. code-block:: none

    Usage: mapchete [OPTIONS] COMMAND [ARGS]...

        Options:
          --version  Show the version and exit.
          --help     Show this message and exit.

        Commands:
          convert    Convert outputs or other geodata.
          create     Create a new process.
          execute    Execute a process.
          formats    List available input and/or output formats.
          index      Create index of output tiles.
          processes  List available processes.
          serve      Serve a process on localhost.


``mapchete convert``
====================

Convert outputs or other geodata.

This command can convert between different Mapchete outputs, for example from a
``TileDirectory`` output to a single file GeoTIFF. It can also convert between different
tile pyramid schemes, projections output formats and apply scale factors and scale
offsets to raster data.

.. code-block:: none

    Usage: mapchete convert [OPTIONS] INPUT OUTPUT

      Convert outputs or other geodata.

    Options:
      -z, --zoom TEXT                 Single zoom level or min and max separated
                                      by ','.
      -b, --bounds FLOAT...           Left, bottom, right, top bounds in tile
                                      pyramid CRS.
      -p, --point FLOAT...            Process tiles over single point location.
      -g, --wkt-geometry TEXT         Take boundaries from WKT geometry in tile
                                      pyramid CRS.
      -c, --clip-geometry PATH        Clip output by geometry
      --bidx TEXT                     Band indexes to copy.
      --output-pyramid [mercator|geodetic]
                                      Output pyramid to write to.
      --output-metatiling INTEGER     Output metatiling.
      --output-format [GTiff|PNG_hillshade|xarray|GeoJSON|PNG]
                                      Output format.
      --output-dtype [int8|int16|float64|int32|uint8|uint32|uint16|float32]
                                      Output data type (for raster output only).
      --co, --profile NAME=VALUE      Driver specific creation options.See the
                                      documentation for the selected output driver
                                      for more information.
      --scale-ratio FLOAT             Scaling factor (for raster output only).
      --scale-offset FLOAT            Scaling offset (for raster output only).
      -r, --resampling-method [nearest|bilinear|cubic|cubic_spline|lanczos|average|mode|gauss]
                                      Resampling method used. (default: nearest)
      --overviews                     Generate overviews (single GTiff output
                                      only).
      --overviews-resampling-method [nearest|bilinear|cubic|cubic_spline|lanczos|average|mode|gauss]
                                      Resampling method used for overviews.
                                      (default: cubic_spline)
      --cog                           Write a valid COG. This will automatically
                                      generate verviews. (GTiff only)
      -o, --overwrite                 Overwrite if tile(s) already exist(s).
      -v, --verbose                   Print info for each process tile.
      --no-pbar                       Deactivate progress bar.
      -d, --debug                     Deactivate progress bar and print debug log
                                      output.
      -m, --multi INTEGER             Number of concurrent processes.
      -l, --logfile PATH              Write debug log infos into file.
      --vrt                           Write VRT file.
      -od, --idx-out-dir PATH         Index output directory.
      --help                          Show this message and exit.


``mapchete create``
===================

Create a new process.

This subcommand will generate an empty ``.mapchete`` and a dummy ``.py`` process
file.

.. code-block:: none

    Usage: mapchete create [OPTIONS] MAPCHETE_FILE PROCESS_FILE
                           [PNG_hillshade|GeoJSON|GTiff|PNG]

      Create a new process.

    Options:
      -op, --out-path PATH            Process output path.
      -pt, --pyramid-type [mercator|geodetic]
                                      Output pyramid type. (default: geodetic)
      -f, --force                     Overwrite if files already exist.
      --help                          Show this message and exit.


``mapchete execute``
====================

Execute a process.

This is intended to batch seed your output pyramid. You can also process a
specific tile by providing the tile index (``zoom`` ``row`` ``col``).

.. code-block:: none

    Usage: mapchete execute [OPTIONS] [MAPCHETE_FILES]...

      Execute a process.

    Options:
      -z, --zoom TEXT              Single zoom level or min and max separated by
                                   ','.
      -b, --bounds FLOAT...        Left, bottom, right, top bounds in tile pyramid
                                   CRS.
      -p, --point FLOAT...         Process tiles over single point location.
      -g, --wkt-geometry TEXT      Take boundaries from WKT geometry in tile
                                   pyramid CRS.
      -t, --tile INTEGER...        Zoom, row, column of single tile.
      -o, --overwrite              Overwrite if tile(s) already exist(s).
      -m, --multi INTEGER          Number of concurrent processes.
      -i, --input-file PATH        Specify an input file via command line (in
                                   mapchete file, set 'input_file' parameter to
                                   'from_command_line').
      -l, --logfile PATH           Write debug log infos into file.
      -v, --verbose                Print info for each process tile.
      --no-pbar                    Deactivate progress bar.
      -d, --debug                  Deactivate progress bar and print debug log
                                   output.
      -c, --max-chunksize INTEGER  Maximum number of process tiles to be queued
                                   for each  worker. (default: 1)
      --vrt                        Write VRT file.
      -od, --idx-out-dir PATH      Index output directory.
      --help                       Show this message and exit.


``mapchete formats``
====================

List available input and/or output formats.

This command lists all registered input and output drivers.

.. code-block:: none

    Usage: mapchete formats [OPTIONS]

      List available input and/or output formats.

    Options:
      -i, --input-formats   Show only input formats.
      -o, --output-formats  Show only output formats.
      -d, --debug           Deactivate progress bar and print debug log output.
      --help                Show this message and exit.


``mapchete index``
==================

Create index of output tiles.

This command lets you create index files for raster ``TileDirectory`` outputs. Such index
files can be ``VRT`` for ``GDAL``, shape index files in either ``GeoJSON``, ``GeoPackage``
or ``ESRI Shapefile`` format or simple ``.txt`` files with lists of existing tile paths.
Shape index files are used in ``Mapserver`` to add large raster mosaics.

.. code-block:: none

    Usage: mapchete index [OPTIONS] [MAPCHETE_FILES]...

      Create index of output tiles.

    Options:
      -od, --idx-out-dir PATH  Index output directory.
      --geojson                Write GeoJSON index.
      --gpkg                   Write GeoPackage index.
      --shp                    Write Shapefile index.
      --vrt                    Write VRT file.
      --txt                    Write output tile paths to text file.
      --fieldname TEXT         Field to store tile paths in.
      --basepath TEXT          Use other base path than given process output path.
      --for-gdal               Make remote paths readable by GDAL (not applied for
                               txt output).
      -z, --zoom TEXT          Single zoom level or min and max separated by ','.
      -b, --bounds FLOAT...    Left, bottom, right, top bounds in tile pyramid
                               CRS.
      -p, --point FLOAT...     Process tiles over single point location.
      -g, --wkt-geometry TEXT  Take boundaries from WKT geometry in tile pyramid
                               CRS.
      -t, --tile INTEGER...    Zoom, row, column of single tile.
      -v, --verbose            Print info for each process tile.
      --no-pbar                Deactivate progress bar.
      -d, --debug              Deactivate progress bar and print debug log output.
      -l, --logfile PATH       Write debug log infos into file.
      --help                   Show this message and exit.


``mapchete processes``
======================

List available processes.

Custom processes can be registered to ``mapchete.processes``. This is helpful in case you
have a separate python package with mapchete processes you want to share.

.. code-block:: none

    Usage: mapchete processes [OPTIONS]

      List available processes.

    Options:
      -n, --process_name TEXT  Print docstring of process.
      --docstrings             Print docstrings of all processes.
      --help                   Show this message and exit.


``mapchete serve``
==================

Serve a process on localhost.

Start a local HTTP server which hosts a simple OpenLayers page and a WMTS simple
endpoint to **serve a process** for quick assessment (default port 5000). This
is intended to process on-demand and show just the current map extent to
facilitate process calibration.

.. code-block:: none

    Usage: mapchete serve [OPTIONS] MAPCHETE_FILE

      Serve a process on localhost.

    Options:
      -p, --port INTEGER            Port process is hosted on. (default: 5000)
      -c, --internal-cache INTEGER  Number of web tiles to be cached in RAM.
                                    (default: 1024)
      -z, --zoom TEXT               Single zoom level or min and max separated by
                                    ','.
      -b, --bounds FLOAT...         Left, bottom, right, top bounds in tile
                                    pyramid CRS.
      -o, --overwrite               Overwrite if tile(s) already exist(s).
      -ro, --readonly               Just read process output without writing.
      -mo, --memory                 Always get output from freshly processed
                                    output.
      -i, --input-file PATH         Specify an input file via command line (in
                                    mapchete file, set 'input_file' parameter to
                                    'from_command_line').
      -d, --debug                   Deactivate progress bar and print debug log
                                    output.
      -l, --logfile PATH            Write debug log infos into file.
      --help                        Show this message and exit.
