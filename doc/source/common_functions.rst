================
Common Functions
================

These functions are available from within a process:


-----------------------------------------
Calculate hillshading from elevation data
-----------------------------------------

.. code-block:: python

    mp.hillshade(
        elevation,
        azimuth=315.0,
        altitude=45.0,
        z=1.0,
        scale=1.0
    )


Returns an array with the same shape as the input array.

* ``elevation``: input array
* ``azimuth``: horizontal angle of light source (315: North-West)
* ``altitude``: vertical angle of light source (90 would result in slope shading)
* ``z``: vertical exaggeration
* ``scale``: scale factor of pixel size units versus height units (insert 112000 when having elevation values in meters in a geodetic projection)


-----------------------------------------
Extract contour lines from elevation data
-----------------------------------------

.. code-block:: python

    mp.contours(
        array,
        interval=100,
        pixelbuffer=0,
        field='elev'
    )


Returns contours as GeoJSON-like pairs of properties and geometry.

* ``elevation``: input array
* ``interval``: elevation value interval
* ``field``: output field name containing elevation value


-------------------------
Clip array by vector data
-------------------------

.. code-block:: python

    mp.clip(
        array,
        geometries,
        inverted=False,
        clip_buffer=0
    )

* ``array``: source array
* ``geometries``: geometries used to clip source array
* ``inverted``: bool, invert clipping
* ``clip_buffer``: int (in pixels), buffer geometries before applying clip
