============
Installation
============

Use ``pip`` to install the latest stable version:

.. code-block:: shell

    pip install mapchete

Manually install the latest development version

.. code-block:: shell

    pip install -r requirements.txt
    python setup.py install


To make sure Rasterio and Fiona are properly built against your local GDAL installation,
don't install the binaries but build them on your system:

.. code-block:: shell

    pip install "rasterio>=1.0.2" "fiona>=1.8b1" --no-binary :all: