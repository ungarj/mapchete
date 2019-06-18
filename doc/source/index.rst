.. Mapchete documentation master file, created by
   sphinx-quickstart on Fri Feb 24 21:39:27 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Mapchete: geospatial processing
===============================

Mapchete processes raster and vector geodata.

Processing larger amounts of data requires chunking the input data into smaller
tiles and process them one by one. Python provides a lot of useful packages to
process geodata like shapely_ or numpy_.

.. _shapely: http://toblerity.org/shapely/
.. _numpy: http://www.numpy.org/

With the help of fiona_ and rasterio_ Mapchete takes care about resampling and
reprojecting geodata, applying your Python code to the tiles and writing the output either
into a single file or into a directory of files organized in a WMTS_-like tile pyramid.
Details on tiling scheme and available map projections are outlined in
:doc:`tiling`.

.. _fiona: https://github.com/Toblerity/Fiona
.. _rasterio: https://github.com/mapbox/rasterio/
.. _WMTS: https://en.wikipedia.org/wiki/Web_Map_Tile_Service


The code_ is available under the :doc:`MIT License <license_link>`.

.. _code: https://github.com/ungarj/mapchete


Example
=======

A process creating a hillshade from an elevation model and clipping it with a
vector dataset could look like this:

.. code-block:: yaml

    # content of hillshade.mapchete
    process: hillshade.py
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


.. code-block:: python

    # content of hillshade.py

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


Examine the result in your browser by serving the process by pointing it to
``localhost:5000``:

.. code-block:: shell

    $ mapchete serve hillshade.mapchete

If the result looks fine, seed zoom levels 0 to 12:

.. code-block:: shell

    $ mapchete execute hillshade.mapchete --zoom 0 12


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   cli
   tiling
   processes
   common_functions
   configuration
   process_input
   process_output
   apidoc/modules
   changelog_link
   license_link
   misc

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
