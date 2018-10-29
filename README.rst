========
Mapchete
========

Tile-based geodata processing.

.. image:: https://badge.fury.io/py/mapchete.svg
    :target: https://badge.fury.io/py/mapchete

.. image:: https://travis-ci.org/ungarj/mapchete.svg?branch=master
    :target: https://travis-ci.org/ungarj/mapchete

.. image:: https://coveralls.io/repos/github/ungarj/mapchete/badge.svg?branch=master
    :target: https://coveralls.io/github/ungarj/mapchete?branch=master

.. image:: https://readthedocs.org/projects/mapchete/badge/?version=latest
    :target: http://mapchete.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/pypi/pyversions/mapchete.svg
    :target: https://pypi.python.org/pypi/mapchete

Developing a script which does some geoprocessing is usually an iterative
process where modifying code, running the script and inspecting the output
repeat until the desired result. This can take a long time as processing and
visualizing the output data repeat very often and therefore sum up. Especially
when using a remote machine because the input data is huge, the time to wait
for the script to finish, download and open the output can be tedious.

Mapchete aims to facilitate this development circle by providing tools to
quickly inspect the output (from a remote or local machine) and allows larger
scale processing jobs by running multiple tiles in parallel.

Python is used a lot because it is a very user-friendly language to quickly
develop working processing chains and it provides a rich ecosystem of packages
which help to efficiently process geodata (e.g. shapely_ for features, numpy_
for rasters).

Mapchete takes care about dissecting, resampling and reprojecting geodata,
applying user defined Python code to each tile and writing the output into a
WMTS_-like tile pyramid which is already optimized to be further used for web
maps.

.. _shapely: http://toblerity.org/shapely/
.. _numpy: http://www.numpy.org/
.. _WMTS: https://en.wikipedia.org/wiki/Web_Map_Tile_Service


-----
Usage
-----

You need a ``.mapchete`` file for the process configuration

.. code-block:: yaml

    process: my_python_process.py  # or a Python module path: mypythonpackage.myprocess
    zoom_levels:
        min: 0
        max: 12
    input:
        dem: /path/to/dem.tif
        land_polygons: /path/to/polygon/file.geojson
    output:
        format: PNG_hillshade
        path: /output/path
    pyramid:
        grid: mercator

    # process specific parameters
    resampling: cubic_spline


and a ``.py`` file or a Python module path where you specify the process itself

.. code-block:: python

    def execute(mp, resampling="nearest"):
        # Open elevation model.
        with mp.open("dem", resampling=resampling) as src:
            # Skip tile if there is no data available.
            if src.is_empty(1):
                return "empty"
            dem = src.read(1)
        # Create hillshade.
        hillshade = mp.hillshade(dem)
        # Clip with polygons and return result.
        with mp.open("land_polygons") as land_file:
            return mp.clip(hillshade, land_file.read())


You can then interactively inspect the process output directly on a map in a
browser (first, install dependencies by ``pip install mapchete[serve]`` go to
``localhost:5000``):

.. code-block:: shell

    mapchete serve hillshade.mapchete --memory


The ``serve`` tool recognizes changes in your process configuration or in the
process file. If you edit one of these, just refresh the browser and inspect the
changes (note: use the ``--memory`` flag to make sure to reprocess each tile and
turn off browser caching).

Once you are done with editing, batch process everything using the ``execute``
tool.

.. code-block:: shell

    mapchete execute hillshade.mapchete


There are many more options such as zoom-dependent process parameters,
metatiling, tile buffers or interpolating from an existing output of a higher
zoom level. For deeper insights, please go to the documentation_.

.. _documentation: http://mapchete.readthedocs.io/en/latest/index.html

Mapchete is used in many preprocessing steps for the `EOX Maps`_ layers:

* Merge multiple DEMs into one global DEM.
* Create a customized relief shade for the Terrain Layer.
* Generalize landmasks & coastline from OSM for multiple zoom levels.
* Extract cloudless pixel for Sentinel-2 cloudless.

.. _`EOX Maps`: http://maps.eox.at/

------------
Installation
------------

via PyPi:

.. code-block:: shell

    pip install mapchete


from source:

.. code-block:: shell

    pip install -r requirements.txt
    python setup.py install


To make sure Rasterio and Fiona are properly built against your local GDAL installation,
don't install the binaries but build them on your system:

.. code-block:: shell

    pip install "rasterio>=1.0.2" "fiona>=1.8b1" --no-binary :all:


-------
License
-------

MIT License

Copyright (c) 2015 - 2018 `EOX IT Services`_

.. _`EOX IT Services`: https://eox.at/
