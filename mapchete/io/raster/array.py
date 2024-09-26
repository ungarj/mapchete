import itertools
import logging
import warnings
from typing import Iterable, List, Optional, Tuple, Union

import numpy as np
import numpy.ma as ma
from affine import Affine
from numpy.typing import DTypeLike
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.warp import reproject
from rasterio.windows import from_bounds
from shapely.ops import unary_union

from mapchete.geometry.shape import to_shape
from mapchete.grid import Grid
from mapchete.protocols import GridProtocol
from mapchete.types import BoundsLike, CRSLike, NodataVal

logger = logging.getLogger(__name__)


def extract_from_array(
    array: Union[np.ndarray, ma.MaskedArray, GridProtocol],
    array_transform: Optional[Affine] = None,
    in_affine: Optional[Affine] = None,
    out_grid: Optional[GridProtocol] = None,
    out_tile: Optional[GridProtocol] = None,
) -> Union[np.ndarray, ma.MaskedArray]:
    """
    Extract raster data window array.

    Returns
    -------
    extracted array : array
    """
    if out_tile:  # pragma: no cover
        warnings.warn(
            DeprecationWarning("'out_tile' is deprecated and should be 'out_grid'")
        )
        out_grid = out_tile
    if in_affine:  # pragma: no cover
        warnings.warn(
            DeprecationWarning(
                "'in_affine' is deprecated and should be 'array_transform'"
            )
        )
        array_transform = array_transform or in_affine

    if out_grid is None:  # pragma: no cover
        raise ValueError("grid must be defined")

    from mapchete.io.raster.referenced_raster import ReferencedRaster

    raster = ReferencedRaster.from_array_like(
        array, transform=array_transform, crs=out_grid.crs
    )

    if raster.crs != out_grid.crs:  # pragma: no cover
        raise ValueError(
            f"source CRS {raster.crs} and destination CRS {out_grid.crs} do not match!"
        )

    # get range within array
    minrow, maxrow, mincol, maxcol = bounds_to_ranges(
        bounds=out_grid.bounds, transform=raster.transform
    )
    # if output window is within input window
    if (
        minrow >= 0
        and mincol >= 0
        and maxrow <= array.shape[-2]
        and maxcol <= array.shape[-1]
    ):
        return raster.array[..., minrow:maxrow, mincol:maxcol]
    # raise error if output is not fully within input
    else:
        raise ValueError("extraction fails if output shape is not within input")


def resample_from_array(
    array: Union[np.ndarray, ma.MaskedArray, GridProtocol],
    array_transform: Optional[Affine] = None,
    out_grid: Optional[Union[Grid, GridProtocol]] = None,
    in_affine: Optional[Affine] = None,
    out_tile: Optional[Union[Grid, GridProtocol]] = None,
    in_crs: Optional[CRSLike] = None,
    resampling: Union[Resampling, str] = Resampling.nearest,
    nodataval: Optional[NodataVal] = None,
    nodata: Optional[NodataVal] = 0,
    keep_2d: bool = False,
) -> ma.MaskedArray:
    """
    Extract and resample from array to target grid.

    Returns
    -------
    resampled array : array
    """
    resampling = (
        resampling if isinstance(resampling, Resampling) else Resampling[resampling]
    )
    if out_tile:  # pragma: no cover
        warnings.warn(
            DeprecationWarning("'out_tile' is deprecated and should be 'grid'")
        )
        out_grid = out_grid or out_tile
    if in_affine:  # pragma: no cover
        warnings.warn(
            DeprecationWarning(
                "'in_affine' is deprecated and should be 'array_transform'"
            )
        )
        array_transform = array_transform or in_affine

    if out_grid is None:  # pragma: no cover
        raise ValueError("grid must be defined")

    if nodataval is not None:  # pragma: no cover
        warnings.warn("'nodataval' is deprecated, please use 'nodata'")
        nodata = nodata or nodataval

    if isinstance(array, ma.MaskedArray):
        pass
    elif isinstance(array, np.ndarray):
        array = ma.MaskedArray(array, mask=array == nodata)
    elif hasattr(array, "affine") and hasattr(array, "data"):  # pragma: no cover
        array_transform = getattr(array, "affine")
        in_crs = array.crs
        array: np.ndarray = getattr(array, "data")
    elif hasattr(array, "transform") and hasattr(array, "data"):  # pragma: no cover
        array_transform = array.transform
        in_crs = array.crs
        array: np.ndarray = getattr(array, "data")
    elif isinstance(array, tuple):
        array = ma.MaskedArray(
            data=np.stack(array),
            mask=np.stack(
                [
                    (
                        band.mask
                        if isinstance(band, ma.masked_array)
                        else np.where(band == nodata, True, False)
                    )
                    for band in array
                ]
            ),
            fill_value=nodata,
        )
    else:
        raise TypeError("wrong input data type: %s" % type(array))

    if array.ndim == 2:
        if not keep_2d:
            array = ma.expand_dims(array, axis=0)
    elif array.ndim == 3:
        pass
    else:
        raise TypeError("input array must have 2 or 3 dimensions")

    if hasattr(array, "fill_value") and getattr(array, "fill_value") != nodata:
        ma.set_fill_value(array, nodata)
        array = array.filled()

    dst_shape: tuple = out_grid.shape
    if len(array.shape) == 3:
        dst_shape = (array.shape[0], *out_grid.shape)

    dst_data = np.empty(dst_shape, array.dtype)

    reproject(
        array,
        dst_data,
        src_transform=array_transform,
        src_crs=in_crs or out_grid.crs,
        src_nodata=nodata,
        dst_transform=out_grid.transform,
        dst_crs=out_grid.crs,
        dst_nodata=nodata,
        resampling=resampling,
    )
    return ma.MaskedArray(dst_data, mask=dst_data == nodata, fill_value=nodata)


def bounds_to_ranges(
    bounds: BoundsLike, transform: Affine
) -> Tuple[int, int, int, int]:
    """
    Return bounds range values from geolocated input.

        Returns
    -------
    minrow, maxrow, mincol, maxcol
    """
    return tuple(
        itertools.chain(
            *from_bounds(*bounds, transform=transform)
            .round_lengths(pixel_precision=0)
            .round_offsets(pixel_precision=0)
            .toranges()
        )
    )


def prepare_array(
    data: Union[Iterable[np.ndarray], np.ndarray, ma.MaskedArray],
    masked: bool = True,
    nodata: NodataVal = 0,
    dtype: DTypeLike = "int16",
) -> Union[np.ndarray, ma.MaskedArray]:
    """
    Turn input data into a proper array for further usage.

    Output array is always 3-dimensional with the given data type. If the output
    is masked, the fill_value corresponds to the given nodata value and the
    nodata value will be burned into the data array.

    Parameters
    ----------
    data : array or iterable
        array (masked or normal) or iterable containing arrays
    nodata : integer or float
        nodata value (default: 0) used if input is not a masked array and
        for output array
    masked : bool
        return a NumPy Array or a NumPy MaskedArray (default: True)
    dtype : string
        data type of output array (default: "int16")

    Returns
    -------
    array : array
    """
    # input is iterable
    if isinstance(data, (list, tuple)):
        return prepare_iterable(data, masked, nodata, dtype)

    # special case if a 2D single band is provided
    elif isinstance(data, np.ndarray) and data.ndim == 2:
        data = ma.expand_dims(data, axis=0)

    # input is a masked array
    if isinstance(data, ma.MaskedArray):
        return prepare_masked_array(data, masked, nodata, dtype)

    # input is a NumPy array
    elif isinstance(data, np.ndarray):
        if masked:
            return ma.masked_values(data.astype(dtype, copy=False), nodata, copy=False)
        else:
            return data.astype(dtype, copy=False)
    else:
        raise ValueError(
            "Data must be array, masked array or iterable containing arrays. "
            "Current data: %s (%s)" % (data, type(data))
        )


def prepare_iterable(
    data: Iterable[np.ndarray], masked: bool, nodata: NodataVal, dtype: DTypeLike
) -> Union[np.ndarray, ma.MaskedArray]:
    out_data = ()
    out_mask = ()
    for band in data:
        if isinstance(band, ma.MaskedArray):
            out_data += (band.data,)
            if masked:
                if band.shape == band.mask.shape:
                    out_mask += (band.mask,)
                else:
                    out_mask += (np.where(band.data == nodata, True, False),)
        elif isinstance(band, np.ndarray):
            out_data += (band,)
            if masked:
                out_mask += (np.where(band == nodata, True, False),)
        else:
            raise ValueError("input data bands must be NumPy arrays")
    if masked:
        return ma.MaskedArray(
            data=np.stack(out_data).astype(dtype, copy=False), mask=np.stack(out_mask)
        )
    else:
        return np.stack(out_data).astype(dtype, copy=False)


def prepare_masked_array(
    data: np.ndarray,
    masked: bool = True,
    nodata: NodataVal = 0,
    dtype: Optional[DTypeLike] = None,
) -> Union[np.ndarray, ma.MaskedArray]:
    dtype = dtype or data.dtype
    if masked:
        return ma.masked_values(data.astype(dtype, copy=False), nodata, copy=False)
    else:
        return ma.filled(data.astype(dtype, copy=False), nodata)


def clip_array_with_vector(
    array: np.ndarray,
    array_affine: Affine,
    geometries: List[dict],
    inverted: bool = False,
    clip_buffer: float = 0,
) -> ma.MaskedArray:
    """
    Clip input array with a vector list.

    Parameters
    ----------
    array : array
        input raster data
    array_affine : Affine
        Affine object describing the raster's geolocation
    geometries : iterable
        iterable of dictionaries, where every entry has a 'geometry' and
        'properties' key.
    inverted : bool
        invert clip (default: False)
    clip_buffer : integer
        buffer (in pixels) geometries before clipping

    Returns
    -------
    clipped array : array
    """
    # buffer input geometries and clean up
    buffered_geometries = []
    for feature in geometries:
        feature_geom = to_shape(feature["geometry"])
        if feature_geom.is_empty:
            continue
        if feature_geom.geom_type == "GeometryCollection":
            # for GeometryCollections apply buffer to every subgeometry
            # and make union
            buffered_geom = unary_union(
                [g.buffer(clip_buffer) for g in feature_geom.geoms]
            )
        else:
            buffered_geom = feature_geom.buffer(clip_buffer)
        if not buffered_geom.is_empty:
            buffered_geometries.append(buffered_geom)

    # mask raster by buffered geometries
    if buffered_geometries:
        if array.ndim == 2:
            return ma.masked_array(
                array,
                geometry_mask(
                    buffered_geometries, array.shape, array_affine, invert=inverted
                ),
            )
        elif array.ndim == 3:
            mask = geometry_mask(
                buffered_geometries,
                (array.shape[1], array.shape[2]),
                array_affine,
                invert=inverted,
            )
            return ma.masked_array(array, mask=np.stack([mask for band in array]))
        else:  # pragma: no cover
            raise ValueError("array has to be 2D or 3D")

    # if no geometries, return unmasked array
    else:
        fill = False if inverted else True
        return ma.masked_array(array, mask=np.full(array.shape, fill, dtype=bool))
