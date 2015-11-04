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

* command line
```
mapchete_execute <process> --bounds --metatiling --multi
```

* as class in other projects (e.g. [``piri``](https://github.com/ungarj/piri))
```python
# TBD
```

## Python bindings
``mapchete`` is intended to be used in python. It borrows some of the concepts such as the [process structure](http://pywps.wald.intevation.org/documentation/pywps-3.2/process/structure.html) developed by the [PyWPS](http://pywps.wald.intevation.org/) project, mainly the process class definition. ``mapchete`` is _not_ a WPS server though.

## Inspirations
* [PyWPS](http://pywps.wald.intevation.org/)
* [geopackage](https://github.com/opengeospatial/geopackage/)
