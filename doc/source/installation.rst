============
Installation
============

Use ``pip`` to install the latest stable version:

.. code-block:: shell

    $ pip install mapchete

Manually install the latest development version

.. code-block:: shell

    $ git clone git@github.com:ungarj/mapchete.git && cd mapchete
    $ pip install .


To make sure Rasterio, Fiona and Shapely are properly built against your local GDAL and
GEOS installations, don't install the binaries but build them on your system:

.. code-block:: shell

    $ pip install --upgrade rasterio fiona shapely --no-binary :all:
