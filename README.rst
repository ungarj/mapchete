.. image:: logo/mapchete.svg

Tile-based geodata processing.

.. image:: https://badge.fury.io/py/mapchete.svg
    :target: https://badge.fury.io/py/mapchete

.. image:: https://github.com/ungarj/mapchete/actions/workflows/python-package.yml/badge.svg
    :target: https://github.com/ungarj/mapchete/actions

.. image:: https://readthedocs.org/projects/mapchete/badge/?version=latest
    :target: http://mapchete.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/pypi/pyversions/mapchete.svg
    :target: https://pypi.python.org/pypi/mapchete

Mapchete processes raster and vector geodata in digestable chunks.

Processing larger amounts of data requires chunking the input data into smaller tiles
and process them one by one. Python provides a lot of useful packages to process geodata
like shapely_ or numpy_. From within your process code you will have access to the geodata
in the form of ``NumPy`` arrays for raster data or GeoJSON-like feature dictionaries for
vector data.

Internally the processing job is split into tasks which can processed in parallel using either
``concurrent.futures`` or build task graphs and use dask_ to intelligently process either on
the local machine or on a cluster.

With the help of fiona_ and rasterio_ Mapchete takes care about resampling and
reprojecting geodata, applying your Python code to the tiles and writing the output either
into a single file or into a directory of files organized in a WMTS_-like tile pyramid.
Details on tiling scheme and available map projections are outlined in the
`tiling documentation`_.

.. _shapely: http://toblerity.org/shapely/
.. _numpy: http://www.numpy.org/
.. _dask: https://www.dask.org/
.. _fiona: https://github.com/Toblerity/Fiona
.. _rasterio: https://github.com/mapbox/rasterio/
.. _WMTS: https://en.wikipedia.org/wiki/Web_Map_Tile_Service
.. _`tiling documentation`: https://mapchete.readthedocs.io/en/latest/tiling.html


.. figure:: https://mapchete.readthedocs.io/en/latest/_images/mercator_pyramid.svg
   :align: center
   :target: https://mapchete.readthedocs.io/en/latest/tiling.html

   (standard Web Mercator pyramid used in the web)


-----
Usage
-----

You need a ``.mapchete`` file for the process configuration. The configuration is based
on the ``YAML`` syntax.

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


You also need either a ``.py`` file or a Python module path where you specify the process
itself.

.. code-block:: python

    def execute(mp, resampling="nearest"):

        # Open elevation model.
        with mp.open("dem") as src:
            # Skip tile if there is no data available or read data into a NumPy array.
            if src.is_empty(1):
                return "empty"
            else:
                dem = src.read(1, resampling=resampling)

        # Create hillshade using a built-in hillshade function.
        hillshade = mp.hillshade(dem)

        # Clip with polygons from vector file and return result.
        with mp.open("land_polygons") as land_file:
            return mp.clip(hillshade, land_file.read())


You can then interactively inspect the process output directly on a map in a
browser (first, install dependencies by ``pip install mapchete[serve]`` go to
``localhost:5000``):

.. code-block:: shell

    $ mapchete serve hillshade.mapchete --memory


The ``serve`` tool recognizes changes in your process configuration or in the
process file. If you edit one of these, just refresh the browser and inspect the
changes (note: use the ``--memory`` flag to make sure to reprocess each tile and
turn off browser caching).

Once you are done with editing, batch process everything using the ``execute``
tool.

.. code-block:: shell

    $ mapchete execute hillshade.mapchete


-------------
Documentation
-------------

There are many more options such as zoom-dependent process parameters, metatiling, tile
buffers or interpolating from an existing output of a higher zoom level. For deeper
insights, please go to the documentation_.

.. _documentation: http://mapchete.readthedocs.io/en/latest/index.html

Mapchete is used in many preprocessing steps for the `EOX Maps`_ layers:

* Merge multiple DEMs into one global DEM.
* Create a customized relief shade for the Terrain Layer.
* Generalize landmasks & coastline from OSM for multiple zoom levels.
* Extract cloudless pixel for Sentinel-2 cloudless_.

.. _cloudless: https://cloudless.eox.at/
.. _`EOX Maps`: http://maps.eox.at/


------------
Installation
------------

via PyPi:

.. code-block:: shell

    $ pip install mapchete


from source:

.. code-block:: shell

    $ git clone git@github.com:ungarj/mapchete.git && cd mapchete
    $ pip install .



To make sure Rasterio, Fiona and Shapely are properly built against your local GDAL and
GEOS installations, don't install the binaries but build them on your system:

.. code-block:: shell

    $ pip install --upgrade rasterio fiona shapely --no-binary :all:


To keep the core dependencies minimal if you install mapchete using ``pip``, some features
are only available if you manually install additional dependencies:

.. code-block:: shell

    # for contour extraction:
    $ pip install mapchete[contours]

    # for dask processing:
    $ pip install mapchete[dask]

    # for S3 bucket reading and writing:
    $ pip install mapchete[s3]

    # for mapchete serve:
    $ pip install mapchete[serve]

    # for VRT generation:
    $ pip install mapchete[vrt]


-------
License
-------

MIT License

Copyright (c) 2015 - 2022 `EOX IT Services`_

.. _`EOX IT Services`: https://eox.at/
