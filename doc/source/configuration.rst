===================================
How to configure a Mapchete Process
===================================

A Mapchete process configuration (a Mapchete file) is a YAML File which
requires a certain schema. Some parameters can be separately defined per zoom
level if necessary.


--------------------
Mandatory parameters
--------------------


process_file
============

Path to python file, relative from the Mapchete file.


input_files
===========

List of input files required for the process. Each file has to be assigned a
name, wich then can be referenced from the ``self.open()`` function within the
process.

**Example:**

.. code-block:: yaml

    input_files:
        dem: path/to/elevation_data.tif
        land_polygons: path/to/land_polygons.shp

from_command_line
-----------------

If the process is designed for a single file, you can also use the
``--input_file`` parameter of the :doc:`cli`. In this case, the ``input_files``
parameter has to be set to ``from_command_line``.

**Example:**

.. code-block:: yaml

    input_files: from_command_line

Run the process afterwards like this:

.. code-block:: shell

    mapchete execute my_process.mapchete --input_file path/to/elevation_data.tif



output
======

Here the output file format, the tile pyramid type (``geodetic`` or
``mercator``) as well as the output ``metatiling`` and ``pixelbuffer`` (if
deviating from global process settings) can be set.

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: GTiff
        metatiling: 4  # optional
        pixelbuffer: 10  # optional
        # plus format specific parameters


Default output formats
----------------------

GTiff
~~~~~

:doc:`GTiff API Reference <apidoc/mapchete.formats.default.gtiff>`

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: GTiff
        bands: 1
        path: my/output/directory
        dtype: uint8
        compress: deflate


PNG
~~~

:doc:`PNG API Reference <apidoc/mapchete.formats.default.png>`

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: PNG
        bands: 4
        path: my/output/directory


PNG_hillshade
~~~~~~~~~~~~~

:doc:`PNG_hillshade API Reference <apidoc/mapchete.formats.default.png_hillshade>`

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: PNG_hillshade
        path: my/output/directory
        nodata: 255


GeoJSON
~~~~~~~

:doc:`GeoJSON API Reference <apidoc/mapchete.formats.default.geojson>`

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: GeoJSON
        path: my/output/directory
        schema:
            properties:
                id: 'int'
            geometry: Polygon


Additional output formats
-------------------------

Additional drivers can be written and installed. TODO: driver chapter


-------------------
Optional parameters
-------------------

process_minzoom, process_maxzoom or process_zoom
================================================

A process can also have one or more valid zoom levels. Outside of these zoom
levels, it returns empty data.

**Example:**

.. code-block:: yaml

    # only zooms 0 to 8 are processed
    process_minzoom: 0
    process_maxzoom: 8


.. code-block:: yaml

    # only zoom 10 to is processed
    process_zoom: 10


process_bounds
==============

Likewise, a process can also be limited to geographical bounds. The bouds are
to be given in the output pyramid CRS and in form of a list and in the form
``[left, bottom, right, top]``.

**Example:**

.. code-block:: yaml

    # only the area between the South Pole and 60°S is processed
    process_bounds: [-180, -90, 180, -60]


metatiling
==========

Metatile size used by process. A metatiling setting of 2 combines 2x2 tiles into
a bigger metatile. Metatile size can only be one of 1, 2, 4, 8, 16. For more
details, go to :doc:`tiling`.


**Example:**

.. code-block:: yaml

    # process 8x8 tiles
    metatiling: 8


pixelbuffer
===========

Buffer around each process tile in pixels. This can prevent artefacts at tile
boundaries and is sometimes required when using some algorithms or image filters
(e.g. hillshade). Tile buffers of course overlap with their neighbors so it is
recommended to keep the buffers as small as possible and the metatiles as large
as possible to minimize redundant processed areas.

**Example:**

.. code-block:: yaml

    # this will result in a tile size of 276x276 px instead of 256x256
    pixelbuffer: 10


baselevels
==========

``baselevels`` are zoom levels which are always freshly processed using the
original input data. Process zoom levels which are outside of the ``baselevels``
range are interpolated from the next zoom level. This is useful when a process
can be run on one or just a few zoom levels and the rest can be interpolated.

The ``baselevels`` setting requires four parameters: ``min`` and ``max``
describe the zoom level range. In ``lower`` and ``higher``, the resampling
method used to interpolate must be defined.

**Example:**

.. code-block:: yaml

    # process zoom ranges from 0 to 14
    process_minzoom: 0
    process_maxzoom: 14

    # levels 10, 11 and 12 are processed
    # level 9 is interpolated from 10, level 8 from level 9 and so on
    # likewise, level 13 is extrapolated from 12 and level 14 from 13
    baselevels:
        min: 10
        max: 12
        # for levels 0 to 9 use cubic resampling
        lower: cubic
        # for levels 13 and 14 use bilinear resampling
        higher: bilinear


User defined parameters
=======================

All other entries used in the Mapchete file are considered user specific and can
be accessed using ``self.params`` from within the Process class. ``self.params``
is a dictionary which contains all parameters.

**Example:**

Mapchete file:

.. code-block:: yaml

    scaling: 2.0

Process file:

.. code-block:: python

    def execute(self):
        scaling = self.params["scaling"]  # scaling now has the value 2.0


Zoom level dependent parameters
===============================

User defined parameters can be adapted for zoom levels. This is usefull if a
process needs different input parameters for different scales (i.e. zoom
levels). ``self.params`` will always provide a configuration snapshot for the
zoom level of the current tile.

This can be triggered by adding another level to the YAML file using one of the
following prefixes:

- ``zoom=``*zoom_level*
- ``zoom<=``*zoom_level*
- ``zoom<``*zoom_level*
- ``zoom>=``*zoom_level*
- ``zoom>``*zoom_level*

**Example:**

Mapchete file:

.. code-block:: yaml

    scaling:
        zoom<=8: 2.0
        zoom>8: 1.5

Process file:

.. code-block:: python

    def execute(self):
        scaling = self.params["scaling"]
        # scaling has the value 2.0 if the current tile is from zoom 8 or
        # lower and 1.5 from zoom 9 or higher

Likewise, this works for input data as well:

.. code-block:: yaml

    input_files:
        land_polygons:
            zoom<=10: land_polygons_simplified.shp
            zoom>10: land_polygons.shp

.. code-block:: python

    def execute(self):
        with self.open("land_polygons") as polygons:
            p = polygons.read()
            # if the current tile zoom is 10 or lower, features from
            # land_polygons_simplified.shp are returned, if the tile zoom
            # is 11 or higher, features from land_polygons.shp are returned
