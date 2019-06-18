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

from_command_line
-----------------

If the process is designed for a single file, you can also use the
``--input_file`` parameter of the :doc:`cli`. In this case, the ``input``
parameter has to be set to ``from_command_line``.

**Example:**

.. code-block:: yaml

    input: from_command_line

Run the process afterwards like this:

.. code-block:: shell

    mapchete execute my_process.mapchete --input_file path/to/elevation_data.tif

It is also possible to define input data groups e.g. for extracted Sentinel-2
granules, where bands are stored in separate files:

**Example:**

.. code-block:: yaml

    input:
        sentinel2_granule:
            red: path/to/B04.jp2
            green: path/to/B03.jp2
            blue: path/to/B02.jp2


TileDirectory inputs
--------------------

It is also possible to directly point to a ``TileDirectory`` output path from another
mapchete process. This is very similar to provide a ``.mapchete`` file path but with the
convenience to just refer to the path.


-------------------------
Additional output formats
-------------------------

Additional drivers can be written and installed. They have to be registered to the
``mapchete.formats.drivers`` entrypoint from within the driver's ``setup.py`` file.
