========
Mapchete
========

Mapchete processes raster and vector geodata.

.. image:: https://badge.fury.io/py/mapchete.svg
    :target: https://badge.fury.io/py/mapchete

.. image:: https://travis-ci.org/ungarj/mapchete.svg?branch=master
    :target: https://travis-ci.org/ungarj/mapchete

.. image:: https://coveralls.io/repos/github/ungarj/mapchete/badge.svg?branch=master
    :target: https://coveralls.io/github/ungarj/mapchete?branch=master

.. image:: https://landscape.io/github/ungarj/mapchete/master/landscape.svg?style=flat
       :target: https://landscape.io/github/ungarj/mapchete/master
       :alt: Code Health

.. image:: https://readthedocs.org/projects/mapchete/badge/?version=latest
    :target: http://mapchete.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Processing larger amounts of data requires chunking the input data into smaller
tiles and process them one by one. Python provides a lot of useful packages to
process geodata like shapely_ or numpy_.

Mapchete takes care about resampling and reprojecting geodata, applying your
Python code to the tiles and writing the output into a WMTS_-like tile pyramid.

.. _shapely: http://toblerity.org/shapely/
.. _numpy: http://www.numpy.org/
.. _WMTS: https://en.wikipedia.org/wiki/Web_Map_Tile_Service


For deeper insights, please go to the documentation_.

.. _documentation: http://mapchete.readthedocs.io/en/latest/index.html

-----
Usage
-----

Mapchete is used in many preprocessing steps for the `EOX Maps`_ layers:

* Merge multiple DEMs into one global DEM.
* Create a customized relief shade for the Terrain Layer.
* Generalize landmasks & coastline from OSM for multiple zoom levels.
* Extract cloudless pixel for Sentinel-2 cloudless.

.. _`EOX Maps`: http://maps.eox.at/

-------
License
-------

MIT License

Copyright (c) 2015, 2016, 2017 `EOX IT Services`_

.. _`EOX IT Services`: https://eox.at/
