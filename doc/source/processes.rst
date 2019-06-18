================
Mapchete Process
================

A Mapchete process has two parts. First, the process itself has to be implemented by
creating a python file with an ``execute(mp)`` function. The ``mp`` object then helps to
read data and access the process parameters. You can also ship the process file within
a python package and reference from the configuration directly to the python module path
instead of the python file path.

Second, a Mapchete process requires a configuration where all the necessary
information is collected such as the location to your process Python file, the
output location and format, and any user-defined parameters desired. This
configuration file has a YAML syntax and has to have a ``.mapchete`` file
extension.

If you have both ready, you can point either ``mapchete serve`` or ``mapchete
execute`` to your process configuration (``.mapchete``) file to either view your
process output in a browser or batch process a larger area.

.. code-block:: shell

    mapchete serve my_process.mapchete

Starts a local web server on port 5000 with a simple OpenLayers interface.

.. code-block:: shell

    mapchete execute my_process.mapchete --zoom 5 10

Executes your process on zoom level 5 to 10.

To access the process parameters, use the dictionary stored in ``mp.params``.
To read data, use the ``mp.open(<input_file_id>)`` function.


---------
Reference
---------


Open and read data
==================

.. code-block:: python

    with mp.open("<input_file_id>") as src:

* ``<input_file_id>``: Input file from ``mp.params``. Can be a raster or vector
  file or the configuration file from another Mapchete process.
* ``resampling``: Resampling method to be used when reading raster data.

Opens a reader object, depending on the data source (raster, vector, Mapchete
process). This object offers following standard functions:


The data reader object
----------------------

.. code-block:: python

    src.is_empty(indexes=None)

* ``indexes``: A list of bands, a single band index or ``None`` to check all
  bands.

Returns ``bool`` indicating whether data within this tile is available or not.

.. code-block:: python

    src.read(indexes=None, resampling="nearest")

* ``indexes``: A list of bands, a single band index or ``None`` to read all
  bands.

For raster files it either returns a masked ``numpy array`` of reprojected and resampled
data fitting to the current tile.

For vector files it returns a ``list`` of ``GeoJSON``-like feature dictionaries
intersecting with and clipped to current tile boundaries.

If reading a Mapchete file, either vector or raster data in the form described
above is returned.

All input drivers have a similar method interface, i.e. all have a generic ``.is_empty()``
and a ``.read()`` function implemented. It depends however on the driver which data type
is returned.


Modify data
===========

After reading the data you can do whatever you want. For vector data, shapely_
provides a rich selection of functions to deal with geometries, for raster data,
NumPy_, SciPy_ or Pillow_ are excellent packages for image processing and other
desired tasks.

Mapchete also comes with some [common purpose functions](common_functions.md)
which allow clipping, calculating a hillshade or extract contour lines from an
elevation model.

.. _shapely: https://github.com/Toblerity/Shapely
.. _NumPy: http://www.numpy.org/
.. _SciPy: http://scipy.org/
.. _Pillow: http://pillow.readthedocs.io/en/3.3.x/


Write data
==========

.. code-block:: python

    return output_data

* ``output_data``: For raster data either a single or a ``tuple`` of
  ``numpy array(s)``. For vector data, a ``GeoJSON``-like ``iterator`` of
  properties-geometry pairs. The write options are specified in the process
  configuration.


-------
Example
-------

The process file should look like this:

.. code-block:: python

    def execute(mp):
        """User defined process."""

        # Reading and writing data works like this:
        with mp.open("raster_file") as my_raster_rgb_file:
            if my_raster_rgb_file.is_empty():
                # this ensures a transparent tile instead of a pink error tile is returned
                # when using mapchete serve
                return "empty"
            r, g, b = my_raster_rgb_file.read(resampling="bilinear")

        return (r, g, b)


-------
Plug-in
-------

You can also package a process within a python module and register it to the entrypoint
``mapchete.processes`` in your packages ``setup.py`` file. This will show your process
when you run ``mapchete processes`` from the command line.
