========
Mapchete
========

Mapchete processes raster and vector geodata.

Processing larger amounts of data requires chunking the input data into smaller tiles and process them one by one. Python provides a lot of useful packages to process geodata like shapely_ or numpy_.

Mapchete takes care about resampling and reprojecting geodata, applying your Python code to the tiles and writing the output into a WMTS_-like tile pyramid.

.. _shapely: http://toblerity.org/shapely/
.. _numpy: http://www.numpy.org/
.. _WMTS: https://en.wikipedia.org/wiki/Web_Map_Tile_Service

-------------
Documentation
-------------

* Installation_
* `Command Line Tools`_
* `Write a Process`_
* `Special functions`_
* `Configure a Process`_
* `Run a Process`_
* Changelog_

.. _Installation: doc/installation.rst
.. _`Command Line Tools`: doc/cli.rst
.. _`Write a Process`: doc/processes.rst
.. _`Special functions`: doc/common_functions.rst
.. _`Configure a Process`: doc/configuration.rst
.. _`Run a Process`: doc/run_process.rst
.. _Changelog: CHANGELOG.rst

-------
Example
-------

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
