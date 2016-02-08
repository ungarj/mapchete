# mapchete

``mapchete`` abstracts geodata preprocessing for web maps.

## Idea

``mapchete`` helps preprocessing data for web mapping projects by letting the operator focus on developing processes. It is designed to resample, cut, process and write data to a predefined tile pyramid. Tile pyramids consist of prerendered maps in various zoom levels. ``mapchete`` extends this concept by also storing the original data (like a DEM) and derived data (like a hillshading) in such tile pyramids (TBDtiles) as well.

### mapchete does
* resample and chop data to tile matrix pyramids
* handle multiprocessing
* read and write from TBDtiles

### mapchete does not
* WPS
* define certain workflows, such as hillshading (mapchete plugins do)

## Usage

```
mapchete_execute.py <mapchete_file>
```
Executes a process. Please see the [example configuration](test/example.mapchete) and [example process](test/example.process.py).

```
mapchete_serve.py <mapchete_file>
```
Serves process results as a [WMTS simple](http://docs.opengeospatial.org/is/13-082r2/13-082r2.html) endpoint under ``http://localhost:5000/wmts``. This can be used to view the process results in any client supporting WMTS such as [QGIS](http://qgis.org) or [OpenLayers](http://openlayers.org/).

## Inspirations
* [PyWPS](http://pywps.wald.intevation.org/)
* [geopackage](https://github.com/opengeospatial/geopackage/)
