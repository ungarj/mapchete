"""
Raster file input which can be read by rasterio.

Currently limited by extensions .tif, .vrt., .png and .jp2 but could be
extended easily.
"""

from cached_property import cached_property
from copy import deepcopy
import logging
import os
import rasterio
from rasterio.vrt import WarpedVRT
from shapely.geometry import box
import warnings

from mapchete.formats import base
from mapchete.io.vector import reproject_geometry, segmentize_geometry
from mapchete.io.raster import read_raster_window, convert_raster
from mapchete import io


logger = logging.getLogger(__name__)

METADATA = {
    "driver_name": "raster_file",
    "data_type": "raster",
    "mode": "r",
    "file_extensions": ["tif", "vrt", "png", "jp2"],
}


class InputData(base.InputData):
    """
    Main input class.

    Parameters
    ----------
    input_params : dictionary
        driver specific parameters

    Attributes
    ----------
    path : string
        path to input file
    profile : dictionary
        rasterio metadata dictionary
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    METADATA = {
        "driver_name": "raster_file",
        "data_type": "raster",
        "mode": "r",
        "file_extensions": ["tif", "vrt", "png", "jp2"],
    }
    _cached_path = None
    _cache_keep = False

    def __init__(self, input_params, **kwargs):
        """Initialize."""
        super().__init__(input_params, **kwargs)
        if "abstract" in input_params:
            self.path = input_params["abstract"]["path"]
            if "cache" in input_params["abstract"]:
                if "path" in input_params["abstract"]["cache"]:
                    self._cached_path = io.absolute_path(
                        path=input_params["abstract"]["cache"]["path"],
                        base_dir=input_params["conf_dir"],
                    )
                else:  # pragma: no cover
                    raise ValueError("please provide a cache path")
                # add preprocessing task to cache data
                self.add_preprocessing_task(
                    convert_raster,
                    key=f"cache_{self.path}",
                    fkwargs=dict(
                        inp=self.path,
                        out=self._cached_path,
                        format=input_params["abstract"]["cache"].get("format", "COG"),
                    ),
                    geometry=self.bbox(),
                )
                self._cache_keep = input_params["abstract"]["cache"].get("keep", False)
        else:
            self.path = input_params["path"]

    @cached_property
    def profile(self):
        """Return raster metadata."""
        with rasterio.open(self.path, "r") as src:
            return deepcopy(src.meta)

    def open(self, tile, **kwargs):
        """
        Return InputTile object.

        Parameters
        ----------
        tile : ``Tile``

        Returns
        -------
        input tile : ``InputTile``
            tile view of input data
        """
        return InputTile(tile, self, **kwargs)

    def bbox(self, out_crs=None):
        """
        Return data bounding box.

        Parameters
        ----------
        out_crs : ``rasterio.crs.CRS``
            rasterio CRS object (default: CRS of process pyramid)

        Returns
        -------
        bounding box : geometry
            Shapely geometry object
        """
        out_crs = self.pyramid.crs if out_crs is None else out_crs
        with rasterio.open(self.path) as src:
            if src.transform.is_identity and src.gcps:
                with WarpedVRT(src) as dst:
                    src_bounds = dst.bounds
                    src_crs = src.gcps[1]
                    src_transform = dst.transform
            else:
                src_crs = src.crs
                src_bounds = src.bounds
                src_transform = src.transform
            out_bbox = bbox = box(*src_bounds)
        # If soucre and target CRSes differ, segmentize and reproject
        if src_crs != out_crs:
            # estimate segmentize value (raster pixel size * tile size)
            # and get reprojected bounding box
            return reproject_geometry(
                segmentize_geometry(bbox, src_transform[0] * self.pyramid.tile_size),
                src_crs=src_crs,
                dst_crs=out_crs,
            )
        else:
            return out_bbox

    def exists(self):
        """
        Check if data or file even exists.

        Returns
        -------
        file exists : bool
        """
        return os.path.isfile(self.path)  # pragma: no cover

    def cleanup(self):
        """Cleanup when mapchete closes."""
        if self._cached_path and not self._cache_keep:
            logger.debug("remove cached file %s", self._cached_path)
            try:
                io.fs_from_path(self._cached_path).rm(self._cached_path)
            except FileNotFoundError:
                pass


class InputTile(base.InputTile):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    kwargs : keyword arguments
        driver specific parameters

    Attributes
    ----------
    tile : tile : ``Tile``
    raster_file : ``InputData``
        parent InputData object
    resampling : string
        resampling method passed on to rasterio
    """

    def __init__(self, tile, raster_file, **kwargs):
        """Initialize."""
        self.tile = tile
        self.raster_file = raster_file
        self.path = raster_file._cached_path or raster_file.path
        if io.path_is_remote(raster_file.path):
            file_ext = os.path.splitext(self.path)[1]
            self.gdal_opts = {
                "GDAL_DISABLE_READDIR_ON_OPEN": True,
                "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": "%s,.ovr" % file_ext,
            }
        else:
            self.gdal_opts = {}

    def read(self, indexes=None, resampling="nearest", **kwargs):
        """
        Read reprojected & resampled input data.

        Returns
        -------
        data : array
        """
        return read_raster_window(
            self.path,
            self.tile,
            indexes=self._get_band_indexes(indexes),
            resampling=resampling,
            gdal_opts=self.gdal_opts,
        )

    def is_empty(self, indexes=None):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        # empty if tile does not intersect with file bounding box
        return not self.tile.bbox.intersects(
            self.raster_file.bbox(out_crs=self.tile.crs)
        )

    def _get_band_indexes(self, indexes=None):
        """Return valid band indexes."""
        if indexes:
            if isinstance(indexes, list):
                return indexes
            else:
                return [indexes]
        else:
            return range(1, self.raster_file.profile["count"] + 1)


def get_segmentize_value(input_file=None, tile_pyramid=None):
    """
    Return the recommended segmentation value in input file units.

    It is calculated by multiplyling raster pixel size with tile shape in
    pixels.

    Parameters
    ----------
    input_file : str
        location of a file readable by rasterio
    tile_pyramied : ``TilePyramid`` or ``BufferedTilePyramid``
        tile pyramid to estimate target tile size

    Returns
    -------
    segmenize value : float
        length suggested of line segmentation to reproject file bounds
    """
    warnings.warn(
        DeprecationWarning("get_segmentize_value() has moved to mapchete.io")
    )  # pragma: no cover
    return io.get_segmentize_value(input_file, tile_pyramid)  # pragma: no cover
