# Mapchete

Mapchete applies any user-defined process to geospatial raster or vector data.

Single Python scripts easily fail when processing highly detailed, global data. Mapchete cuts and resamples input data into easily digestible chunks and applies any kind of geoprocess written in Python to these tiles. It helps
1. developing a geoprocess by only processing data on demand during viewing in a browser, and
2. batch processing large model inputs in parallel on all available CPU cores.

## Usage

Mapchete offers various useful subcommands: ``create``, ``execute``, ``serve`` and ``pyramid``. 

### Create an empty process

This subcommand will generate an empty ``.mapchete`` and a dummy ``.py`` process file.

```shell
usage: mapchete create <mapchete_file> <process_file>

Creates an empty process and configuration file

positional arguments:
  mapchete_file         Mapchete file
  process_file          process (Python) file

optional arguments:
  -h, --help            show this help message and exit
  --out_format {GTiff,GeoJSON,PostGIS,NumPy,PNG_hillshade,PNG}, -of {GTiff,GeoJSON,PostGIS,NumPy,PNG_hillshade,PNG}
                        process output format (default: None)
  --out_path <path>, -op <path>
                        path for process output (default: None)
  --pyramid_type {geodetic,mercator}, -pt {geodetic,mercator}
                        output pyramid type (default: geodetic)
  --force, -f           overwrite if Mapchete and process files already exist
                        (default: False)
```

### Execute a process
This is intended to batch seed your output pyramid. You can also process a specific tile by providing the tile index (``zoom`` ``row`` ``col``).
```shell
usage: mapchete execute <mapchete_file>

Executes a process

positional arguments:
  mapchete_file         Mapchete file

optional arguments:
  -h, --help            show this help message and exit
  --zoom [<int> [<int> ...]], -z [<int> [<int> ...]]
                        either minimum and maximum zoom level or just one zoom
                        level (default: None)
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
```
### Serve a process
Start a local HTTP server which hosts a simple OpenLayers page and a WMTS simple endpoint to **serve a process** for quick assessment (default port 5000). This is intended to process on-demand and show just the current map extent to facilitate process calibration.
```shell
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
```

With both commands you can also limit the processing zoom levels and bounding box with a ``-z``and a ``-b`` parameter respectively. This overrules the zoom level and output bounds settings in the mapchete configuration file.

In addition, there is the possibility to **create a tile pyramid** out of a raster file. It can either take the original data types and create the output tiles as GeoTIFFS, or scale the data to 8 bits and create PNGs.

# Quickly build a tile pyramid out of a raster
```shell
usage: mapchete pyramid <raster_file>

Creates a tile pyramid from an input raster dataset

positional arguments:
  input_raster          input raster file
  output_dir            output directory where tiles are stored

optional arguments:
  -h, --help            show this help message and exit
  --pyramid_type {geodetic,mercator}, -pt {geodetic,mercator}
                        pyramid schema to be used (default: mercator)
  --output_format <str>, -of <str>
                        output data format (GTiff or PNG) (default: GTiff)
  --resampling_method <str>, -r <str>
                        resampling method to be used (nearest, bilinear,
                        cubic, cubic_spline, lanczos, average or mode)
                        (default: nearest)
  --scale_method <str>, -s <str>
                        scale method if input bands have more than 8 bit
                        (dtype_scale, minmax_scale or crop) (default:
                        minmax_scale)
  --zoom [<int> [<int> ...]], -z [<int> [<int> ...]]
                        either minimum and maximum zoom level or just one zoom
                        level (default: None)
  --overwrite, -o       overwrite if tile(s) already exist(s) (default: False)
```

## Documentation

Please find a more detailed description here:

* [Writing a Process](doc/processes.md)
* [Configuring a Process](doc/configuration.md)

## Installation

Clone repository and run
```
python setup.py install
```

or use ``pip``:
```
pip install mapchete
```

In case there are problems installing GDAL/OGR for ``virtualenv``, try the following (from [here](https://gist.github.com/cspanring/5680334); works on Ubuntu 14.04):

```shell
sudo apt-add-repository ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update
sudo apt-get install libgdal-dev
```

and run ``pip`` while also providing your GDAL version installed and the locations of the headers:

```shell
pip install gdal==1.11.2 --global-option=build_ext --global-option="-I/usr/include/gdal/"
```

## Planned next

* ~~have a mapchete process as data input option for another process~~
* on-demand processing if another Mapchete process is used as input
* store in GeoPackage, not just single tiles in the filesystem
* add .SAFE archive (Sentinel-2) as data input option
* ~~add Spherical Mercator (EPSG:3857) tile pyramid support~~

## Inspirations
* [PyWPS](http://pywps.wald.intevation.org/)
* [geopackage](https://github.com/opengeospatial/geopackage/)
