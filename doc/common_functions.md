# Common Functions

These functions are available from within a process:

```python
.hillshade(
    elevation,
    azimuth=315.0,
    altitude=45.0,
    z=1.0,
    scale=1.0
    )
```
* ``elevation``: array with elevation data
* ``MapcheteProcess``: process instance ( insert ``self`` when using in process)
* ``azimuth``: horizontal angle of light source (315: North-West)
* ``altitude``: vertical angle of light source (90 would result in a slope shade)
* ``z``: vertical exaggeration
* ``scale``: scale factor of pixel size units versus height units (insert 112000 when having elevation values in meters in a geodetic projection)

Returns a ``numpy array`` of the same shape as the elevation array.

```python
.contours(
    array,
    interval=100,
    pixelbuffer=0,
    field='elev'
    )
```
* ``array``: array with elevation data
* ``array_affine``: ``Affine`` object of source array
* ``geometries``: list of GeoJSON-like properties-geometry pairs
* ``inverted``: if ``True``, pixels outside of geometries are clipped
* ``clip_buffer``: buffer geometries before clipping

Returns a ``list`` of GeoJSON-like properties-geometry pairs

```python
.clip(
    array,
    geometries,
    inverted=False,
    clip_buffer=0
    )
```
* ``array``: source array
* ``array_affine``: ``Affine`` object of source array
* ``geometries``: list of GeoJSON-like properties-geometry pairs
* ``inverted``: if ``True``, pixels outside of geometries are clipped
* ``clip_buffer``: buffer geometries before clipping

Returns a ``numpy array`` of the same shape as the source array where all pixels intersecting with the mask are masked as nodata.
