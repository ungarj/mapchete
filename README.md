# mapchete

``mapchete`` prerocesses input data as tile pyramids for web maps.

## Idea

``mapchete`` handles all the resampling, data cutting & cropping and parallelizing of a user-defined process so all you have to do is defining what shall happen with the data within a tile and the tool handles the rest.

Please see the [example configuration](test/example.mapchete) and [example process](test/example_process.py) to catch an idea how it works.

## Usage

There are two main functions:

```
mapchete_execute.py <mapchete_file>
```
Executes a process. This is intended to batch seed your output pyramid.

```
mapchete_serve.py <mapchete_file>
```

Starts a http server on localhost:5000 which hosts a simple OpenLayers page and a WMTS simple endpoint. This is intended to process on-demand and show just the current map extent to facilitate process calibration.

With both commands you can also limit the processing zoom levels and bounding box with a ``-z```and a ``-b`` parameter respectively. This overrules the zoom level and output bounds settings in the mapchete configuration file.

## Installation

Clone repository and run
```
python setup.py install
```

or use ``pip``:
```
pip install mapchete
```

## Planned next

* have a mapchete process as data input option for another process
* store in GeoPackage, not just single tiles in the filesystem
* add .SAFE archive (Sentinel-2) as data input option
* add Spherical Mercator (EPSG:3857) tile pyramid support

## Inspirations
* [PyWPS](http://pywps.wald.intevation.org/)
* [geopackage](https://github.com/opengeospatial/geopackage/)
