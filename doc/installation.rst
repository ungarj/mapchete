============
Installation
============

Use ``pip`` to install the latest stable version:

.. code-block:: shell

    pip install mapchete

Manually install the latest development version

.. code-block:: shell

    python setup.py install

------------
Dependencies
------------

In case there are problems installing GDAL/OGR for ``virtualenv``, try the following (from [here](https://gist.github.com/cspanring/5680334); works on Ubuntu 14.04):

.. code-block:: shell

    sudo apt-add-repository ppa:ubuntugis/ubuntugis-unstable
    sudo apt-get update
    sudo apt-get install libgdal-dev

and run ``pip`` while also providing your GDAL version installed and the locations of the headers:

.. code-block:: shell

    pip install gdal==2.1.0 --global-option=build_ext --global-option="-I/usr/include/gdal/"
