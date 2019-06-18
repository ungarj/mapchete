==================
Command Line Tools
==================

Mapchete offers various useful subcommands: ``create``, ``execute``, ``serve``
 and ``convert``.

Create an empty process
=======================

``mapchete create <mapchete_file> <process_file>``

This subcommand will generate an empty ``.mapchete`` and a dummy ``.py`` process
file.

.. code-block:: shell

    usage: mapchete create <mapchete_file> <process_file>

    Creates an empty process and configuration file

    positional arguments:
      mapchete_file         Mapchete file
      process_file          process (Python) file

    optional arguments:
      -h, --help            show this help message and exit
      --out_format {GTiff,GeoJSON,PostGIS,NumPy,PNG_hillshade,PNG}, -of {GTiff,
                            GeoJSON, PostGIS, NumPy, PNG_hillshade, PNG}
                            process output format (default: None)
      --out_path <path>, -op <path>
                            path for process output (default: None)
      --pyramid_type {geodetic,mercator}, -pt {geodetic,mercator}
                            output pyramid type (default: geodetic)
      --force, -f           overwrite if Mapchete and process files already
                            exist (default: False)


Execute a process
=================

``mapchete execute <mapchete_file>``

This is intended to batch seed your output pyramid. You can also process a
specific tile by providing the tile index (``zoom`` ``row`` ``col``).

.. code-block:: shell

    usage: mapchete execute <mapchete_file>

    Executes a process

    positional arguments:
      mapchete_file         Mapchete file

    optional arguments:
      -h, --help            show this help message and exit
      --zoom [<int> [<int> ...]], -z [<int> [<int> ...]]
                            either minimum and maximum zoom level or just one
                            zoom level (default: None)
      --bounds <float> <float> <float> <float>, -b <float> <float> <float> <float>
                            left, bottom, right, top bounds in tile pyramid CRS
                            (default: None)
      --tile <int> <int> <int>, -t <int> <int> <int>
                            zoom, row, column of single tile (default: None)
      --failed_from_log <path>
                            process failed tiles from log file (default: None)
      --failed_since <date>
                            furthermore filter failed tiles by time (e.g.
                            2016-09-20) (default: None)
      --overwrite, -o       overwrite if tile(s) already exist(s) (default: False)
      --multi <int>, -m <int>
                            number of concurrent processes (default: None)
      --create_vrt          if raster output, this option creates a VRT for each
                            zoom level (default: False)
      --input_file <path>, -i <path>
                            specify an input file via command line (in apchete
                            file, set 'input_file' parameter to
                            'from_command_line') (default: None)

Serve a process
===============

``mapchete serve <mapchete_file>``

Start a local HTTP server which hosts a simple OpenLayers page and a WMTS simple
endpoint to **serve a process** for quick assessment (default port 5000). This
is intended to process on-demand and show just the current map extent to
facilitate process calibration.

.. code-block:: shell

    usage: mapchete serve <mapchete_file>

    Serves a process on localhost

    positional arguments:
      mapchete_file         Mapchete file

    optional arguments:
      -h, --help            show this help message and exit
      --port <int>, -p <int>
                            port process is hosted on (default: None)
      --zoom [<int> [<int> ...]], -z [<int> [<int> ...]]
                            either minimum and maximum zoom level or just one zoom
                            level (default: None)
      --bounds <float> <float> <float> <float>, -b <float> <float> <float> <float>
                            left, bottom, right, top bounds in tile pyramid CRS
                            (default: None)
      --overwrite, -o       overwrite if tile(s) already exist(s) (default: False)
      --input_file <path>, -i <path>
                            specify an input file via command line (in Mapchete
                            file, set 'input_file' parameter to
                            'from_command_line') (default: None)

With both commands you can also limit the processing zoom levels and bounding
box with a ``-z``and a ``-b`` parameter respectively. This overrules the zoom
level and output bounds settings in the mapchete configuration file.

In addition, there is the possibility to **create a tile pyramid** out of a
raster file. It can either take the original data types and create the output
tiles as GeoTIFFS, or scale the data to 8 bits and create PNGs.
