===================================
How to configure a Mapchete Process
===================================

A Mapchete process configuration (a Mapchete file) is a YAML File which
requires a certain schema. Some parameters can be separately defined per zoom
level if necessary.


--------------------
Mandatory parameters
--------------------


``process_file``
================


``input_files``
===============

``from_command_line``
---------------------


``output``
==========

Default output formats
----------------------

``GTiff``
~~~~~~~~~

``PNG``
~~~~~~~

``PNG_hillshade``
~~~~~~~~~~~~~~~~~

``GeoJSON``
~~~~~~~~~~~

Additional output formats
-------------------------


---------------------------
Optional process parameters
---------------------------


``process_minzoom``, ``process_maxzoom`` or ``process_zoom``
============================================================


``process_bounds``
==================


``metatiling``
==============


``pixelbuffer``
===============


``baselevels``
==============


--------
Examples
--------


Zoom level dependent
====================

.. code-block:: yaml

    process_file: # path to process (python file)

    input_files: # these files are scanned and if the current tile does not
                 # intersect with any input file bounding box, this tile is
                 # considered emtpy and omitted.
        file1: # path to file 1
        file2: # path to file 2

    output: # output specific information, e.g.:
        path: # absolute output path
        format: # PNG, PNG_hillshade, GTiff, GeoJSON, PostGIS or NumPy
        type: # geodetic or mercator
        dtype: # bool, uint8, uint16, int16, uint32, int32, float32, float64
        bands: # number of output bands
        nodata: # nodata value

    process_minzoom: # minimum zoom level this process is valid
    process_maxzoom: # maximum zoom level this process is valid
    process_bounds: # left bottom right top in "type" CRS
    metatiling: # (default is 1); has to be one of 2, 4, 6, 8 or 16
