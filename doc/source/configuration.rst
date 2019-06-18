==============================
Mapchete Process Configuration
==============================

A Mapchete process configuration (a Mapchete file) is a YAML File which
requires a certain schema. Some parameters can be separately defined per zoom
level if necessary.

.. code-block:: yaml

    # mandatory parameters
    ######################

    # this is the location of user python code:
    process: example_process.py
    # alternatively, you can point to a python module:
    # process: mapchete.processes.convert

    # process pyramid definition
    pyramid:
        grid: geodetic
        metatiling: 4 # can be 1, 2, 4, 8, 16 (default 1)
        pixelbuffer: 10

    # process input
    input:
        file1:
            zoom>=10: testdata/dummy1.tif
        file2: testdata/dummy2.tif

    # process output
    output:
        path: testdata/tmp/example
        format: GTiff
        dtype: float32
        bands: 1
        metatiling: 1

    # zoom level range:
    zoom_levels:
        min: 7
        max: 11
    # or define single zoom level
    # zoom_levels: 5

    # optional parameters
    #####################

    # geographical subset:
    bounds: [1.0, 2.0, 3.0, 4.0]

    # generate overview levels from baselevels
    baselevels:
        min: 11
        max: 11

    # free parameters
    #################

    some_integer_parameter: 12
    some_float_parameter: 5.3
    some_string_parameter:
        zoom<=7: string1
        zoom>7: string2
    some_bool_parameter: true


--------------------
Mandatory parameters
--------------------

process
=======

Path to python file, relative from the Mapchete file or module path from within python.

pyramid
=======

Process pyramid and projection can be defined here. The two default pyramids available
are ``geodetic`` and ``mercator``

metatiling
----------

Metatile size used by process. A metatiling setting of 2 combines 2x2 tiles into
a bigger metatile. Metatile size can only be one of 1, 2, 4, 8, 16. For more
details, go to :doc:`tiling`.


**Example:**

.. code-block:: yaml

    # process 8x8 tiles
    metatiling: 8

pixelbuffer
-----------

Buffer around each process tile in pixels. This can prevent artefacts at tile
boundaries and is sometimes required when using some algorithms or image filters
(e.g. hillshade). Tile buffers of course overlap with their neighbors so it is
recommended to keep the buffers as small as possible and the metatiles as large
as possible to minimize redundant processed areas.

**Example:**

.. code-block:: yaml

    # this will result in a tile size of 276x276 px instead of 256x256
    pixelbuffer: 10

input
=====

see :doc:`process_input`

output
======

see :doc:`process_output`

zoom_levels
===========

A process can also have one or more valid zoom levels. Outside of these zoom
levels, it returns empty data.

**Example:**

.. code-block:: yaml

    # only zooms 0 to 8 are processed
    zoom_levels:
        min: 0
        max: 8


.. code-block:: yaml

    # only zoom 10 to is processed
    zoom_levels: 10


-------------------
Optional parameters
-------------------

bounds
======

Likewise, a process can also be limited to geographical bounds. The bouds are
to be given in the output pyramid CRS and in form of a list and in the form
``[left, bottom, right, top]``.

**Example:**

.. code-block:: yaml

    # only the area between the South Pole and 60°S is processed
    bounds: [-180, -90, 180, -60]


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


-----------------------
User defined parameters
-----------------------

All other entries used in the Mapchete file are considered user specific and can
be accessed using ``mp.params`` from within the Process class. ``mp.params``
is a dictionary which contains all parameters.

**Example:**

Mapchete file:

.. code-block:: yaml

    scaling: 2.0

Process file:

.. code-block:: python

    def execute(mp):
        scaling = mp.params["scaling"]  # scaling now has the value 2.0


-------------------------------
Zoom level dependent parameters
-------------------------------

User defined parameters can be adapted for zoom levels. This is usefull if a
process needs different input parameters for different scales (i.e. zoom
levels). ``mp.params`` will always provide a configuration snapshot for the
zoom level of the current tile.

This can be triggered by adding another level to the YAML file using one of the
following prefixes:

- ``zoom=`` *zoom_level*
- ``zoom<=`` *zoom_level*
- ``zoom<`` *zoom_level*
- ``zoom>=`` *zoom_level*
- ``zoom>`` *zoom_level*

**Example:**

Mapchete file:

.. code-block:: yaml

    scaling:
        zoom<=8: 2.0
        zoom>8: 1.5

Process file:

.. code-block:: python

    def execute(mp):
        scaling = mp.params["scaling"]
        # scaling has the value 2.0 if the current tile is from zoom 8 or
        # lower and 1.5 from zoom 9 or higher

This works likewise for input data:

.. code-block:: yaml

    input:
        land_polygons:
            zoom<=10: land_polygons_simplified.shp
            zoom>10: land_polygons.shp

.. code-block:: python

    def execute(mp):
        with mp.open("land_polygons") as polygons:
            p = polygons.read()
            # if the current tile zoom is 10 or lower, features from
            # land_polygons_simplified.shp are returned, if the tile zoom
            # is 11 or higher, features from land_polygons.shp are returned
