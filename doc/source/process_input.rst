=============
Input Formats
=============


Input data required for the process. Each input type has to be assigned an
identifier, wich then can be referenced from the ``mp.open()`` function
from within the process.


Single file inputs
------------------

Mapchete can guess the driver from single file paths. In general, all formats readable by
``rasterio`` and ``fiona`` will be read as ``NumPy`` arrays and GeoJSON-like feature
dictionaries respectively.

Alternatively, a ``.mapchete`` file can be provided. In this case, the data type read
depends on the output driver specified in the ``.mapchete`` file. It is well-intended that
mapchete processes can be chained in such a way!

For single files like GeoTIFFs, JPEG2000 files, Shapefiles or GeoJSON files,
a file path (either remote or local) is sufficient.

**Example:**

.. code-block:: yaml

    input:
        dem: path/to/elevation_data.tif
        land_polygons: path/to/land_polygons.shp


Processing performance is best if the input files support quick access to subsets of the data.
For rasters, converting them to COG (Cloud Optimized GeoTIFF) can sometimes do wonders especially
if the data sits on an object storage. For vector data a format which support spatial indexing (e.g.
GeoPackage or FlatGeoBuf) can do similar tricks although this works just in a limited way if the data
is on an object storage.

**Example:**

.. code-block:: yaml

    input:
        dem:
            format: raster_file
            path: path/to/elevation_data.tif
            cache:
                path: tmp/path/to/elevation_data.tif
                format: COG
        land_polygons:
            format: vector_file
            path: path/to/land_polygons.shp
            cache:
                path: tmp/path/to/land_polygons.fgb
                format: FlatGeobuf
                keep: false  # if set to false cached data will not be deleted after the process finishes

In some cases it can be beneficial if the data is not cached on disk but directly in memory. This feature
should be used with care and only for smaller datasets.

**Example:**

.. code-block:: yaml

    input:
        dem:
            format: raster_file
            path: path/to/elevation_data.tif
            cache: memory
        land_polygons:
            format: vector_file
            path: path/to/land_polygons.shp
            cache: memory

It is also possible to define input data groups e.g. for extracted Sentinel-2
granules, where bands are stored in separate files:

**Example:**

.. code-block:: yaml

    input:
        sentinel2_granule:
            red: path/to/B04.jp2
            green: path/to/B03.jp2
            blue: path/to/B02.jp2

In case the inputs are stored on separate storages with individual access settings,
they can be provided in a `storage_options` mapping:

**Example:**

.. code-block:: yaml

    input:
        foo:
            format: raster_file
            path: s3://bucket1/image.tif
            storage_options:
                AWS_ACCESS_KEY_ID: some_key_id
                AWS_SECRET_ACCESS_KEY: some_key_secret
        foo2:
            format: raster_file
            path: s3://bucket2/image.tif
            storage_options:
                AWS_ACCESS_KEY_ID: some_other_key_id
                AWS_SECRET_ACCESS_KEY: some_other_key_secret


It is not recommended to put in access credatials as plain text into the configuration.
It is also possible to point to environment variables instead of values:

**Example:**

.. code-block:: yaml

    input:
        foo:
            format: raster_file
            path: s3://bucket1/image.tif
            storage_options:
                AWS_ACCESS_KEY_ID: ${SOME_KEY_ID}
                AWS_SECRET_ACCESS_KEY: ${SOME_KEY_SECRET}
        foo2:
            format: raster_file
            path: s3://bucket2/image.tif
            storage_options:
                AWS_ACCESS_KEY_ID: ${SOME_OTHER_KEY_ID}
                AWS_SECRET_ACCESS_KEY: ${SOME_OTHER_KEY_SECRET}


TileDirectory inputs
--------------------

It is also possible to directly point to a ``TileDirectory`` output path from another
mapchete process. This is very similar to provide a ``.mapchete`` file path but with the
convenience to just refer to the path.


**Example:**

.. code-block:: yaml

    input:
        foo: path_to_tiledirectory

Sometimes it can be beneficial to pass on some default values to a TileDirectory, such
as the maximum zoom level available. In that case Mapchete knows to read data from this
zoom level in case a process runs on a higher zoom.

**Example:**

.. code-block:: yaml

    input:
        foo:
            format: TileDirectory
            path: path_to_tiledirectory
            resampling: bilinear
            max_zoom: 8  # now data can be read also from e.g. zoom 9 and will be resampled


-------------------------
Additional output formats
-------------------------

Additional drivers can be written and installed. They have to be registered to the
``mapchete.formats.drivers`` entrypoint from within the driver's ``setup.py`` file.
