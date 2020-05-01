==============
Output Formats
==============


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


Default formats can also read and write from and to S3 Object Storages. This is simply
achieved by providing the full S3 path, i.e. ``s3://my_output_bucket/process_output``.


----------------------
Default output formats
----------------------


GTiff
-----

:doc:`GTiff API Reference <apidoc/mapchete.formats.default.gtiff>`

This output format can store either ``TileDirectories`` of GeoTIFF files or a single
GeoTIFF file. Just provide either a directory or a file path to trigger which kind of
output you like

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: GTiff
        bands: 1
        # as tile directory
        path: my/output/directory
        # as single file
        path: my/output/directory/single_file.tif
        dtype: uint8
        compress: deflate


For single GeoTIFF files you can pass on optional flags:

- ``bigtiff``: Pass on setting directly to GDAL. (``YES``, ``NO``, ``IF_NEEDED``, or
  ``IF_SAFER``)
- ``cog`` Create a valid Cloud Optimized GeoTIFF. Note that this setting will
  automatically generate overviews. (``true`` or ``false``)
- ``overviews`` Generate internal overviews. (``true`` or ``false``)
- ``overviews_resampling`` ``rasterio`` Resampling method to be used. (default:
  ``nearest``)
- ``overviews_levels`` List of zoom levels to be written as overviews. (default: every
  level up to level 0)

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: GTiff
        bands: 1
        path: s3://my-bucket/my/output/directory/single_file.tif
        dtype: uint8
        compress: deflate
        cog: true
        overviews_resampling: bilinear

PNG
---

:doc:`PNG API Reference <apidoc/mapchete.formats.default.png>`

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: PNG
        bands: 4
        path: my/output/directory


PNG_hillshade
-------------

:doc:`PNG_hillshade API Reference <apidoc/mapchete.formats.default.png_hillshade>`

**Example:**

.. code-block:: yaml

    output:
        type: geodetic
        format: PNG_hillshade
        path: my/output/directory
        nodata: 255


GeoJSON
-------

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


-------------------------
Additional output formats
-------------------------

Additional drivers can be written and installed. They have to be registered to the
``mapchete.formats.drivers`` entrypoint from within the driver's ``setup.py`` file.
