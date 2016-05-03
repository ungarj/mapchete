# mapchete

``mapchete`` preprocesses all kinds of input data for web maps using tile pyramids.

## Idea

``mapchete`` simply applies a user-defined geo-process written in Python in small chunks to any kind of input data. It handles all the resampling, reprojection, data cutting & cropping and parallelizing of the geo-process so all you have to do is defining what shall happen with the data within a tile and the tool handles the rest.

Please see the [example configuration](test/example.mapchete) and [example process](test/example_process.py) to catch an idea how it works.

## Usage

There are two main functions:

**Execute a process**. This is intended to batch seed your output pyramid. You can also process a specific tile by providing the tile index (``zoom`` ``row`` ``col``).
```shell
mapchete_execute [-h] [--zoom [ZOOM [ZOOM ...]]]
                        [--bounds [BOUNDS [BOUNDS ...]]]
                        [--tile TILE TILE TILE] [--log] [--overwrite]
                        mapchete_file
```

Start a local HTTP server which hosts a simple OpenLayers page and a WMTS simple endpoint to **serve a process for quick assessment** (default port 5000). This is intended to process on-demand and show just the current map extent to facilitate process calibration.
```shell
mapchete_serve [-h] [--port PORT] [--zoom [ZOOM [ZOOM ...]]]
                      [--bounds [BOUNDS [BOUNDS ...]]] [--log]
                      mapchete_file

```

With both commands you can also limit the processing zoom levels and bounding box with a ``-z``and a ``-b`` parameter respectively. This overrules the zoom level and output bounds settings in the mapchete configuration file.

In addition, there is the possibility to **create a tile pyramid** out of a raster file. It can either take the original data types and create the output tiles as GeoTIFFS, or scale the data to 8 bits and create PNGs.
```shell
raster2pyramid [-h] [--output_format {GTiff,PNG}]
                      [--resampling_method {nearest,bilinear,cubic,cubic_spline,lanczos,average,mode}]
                      [--scale_method {dtype_scale,minmax_scale,crop}]
                      [--zoom [ZOOM [ZOOM ...]]]
                      [--bounds [BOUNDS [BOUNDS ...]]] [--log] [--overwrite]
                      input_raster {geodetic,mercator} output_dir
```

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
* on-demand processing if another ``mapchete`` process is used as input
* store in GeoPackage, not just single tiles in the filesystem
* add .SAFE archive (Sentinel-2) as data input option
* add Spherical Mercator (EPSG:3857) tile pyramid support

## Inspirations
* [PyWPS](http://pywps.wald.intevation.org/)
* [geopackage](https://github.com/opengeospatial/geopackage/)
