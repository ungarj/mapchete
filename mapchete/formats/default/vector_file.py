"""
Vector file input which can be read by fiona.

Currently limited by extensions .shp and .geojson but could be extended easily.
"""

import fiona
import logging
from shapely.geometry import box
from rasterio.crs import CRS

from mapchete.formats import base
from mapchete.io.vector import reproject_geometry, read_vector_window, convert_vector
from mapchete.io import fs_from_path, absolute_path


logger = logging.getLogger(__name__)


METADATA = {
    "driver_name": "vector_file",
    "data_type": "vector",
    "mode": "r",
    "file_extensions": ["shp", "geojson", "gpkg"],
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
        "driver_name": "vector_file",
        "data_type": "vector",
        "mode": "r",
        "file_extensions": ["shp", "geojson"],
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
                    self._cached_path = absolute_path(
                        path=input_params["abstract"]["cache"]["path"],
                        base_dir=input_params["conf_dir"],
                    )
                else:  # pragma: no cover
                    raise ValueError("please provide a cache path")
                # add preprocessing task to cache data
                self.add_preprocessing_task(
                    convert_vector,
                    key=f"cache_{self.path}",
                    fkwargs=dict(
                        inp=self.path,
                        out=self._cached_path,
                        format=input_params["abstract"]["cache"].get(
                            "format", "FlatGeobuf"
                        ),
                    ),
                    geometry=self.bbox(),
                )
                self._cache_keep = input_params["abstract"]["cache"].get("keep", False)
        else:
            self.path = input_params["path"]

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
        with fiona.open(self.path) as inp:
            inp_crs = CRS(inp.crs)
            bbox = box(*inp.bounds)
        # TODO find a way to get a good segmentize value in bbox source CRS
        return reproject_geometry(
            bbox, src_crs=inp_crs, dst_crs=out_crs, clip_to_crs_bounds=False
        )

    def cleanup(self):
        """Cleanup when mapchete closes."""
        if self._cached_path and not self._cache_keep:
            logger.debug("remove cached file %s", self._cached_path)
            try:
                fs_from_path(self._cached_path).rm(self._cached_path)
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
    vector_file : string
        path to input vector file
    """

    def __init__(self, tile, vector_file, **kwargs):
        """Initialize."""
        self.tile = tile
        self.vector_file = vector_file
        self._cache = {}
        self.path = vector_file._cached_path or vector_file.path

    def read(self, validity_check=True, clip_to_crs_bounds=False, **kwargs):
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        validity_check : bool
            also run checks if reprojected geometry is valid, otherwise throw
            RuntimeError (default: True)
        clip_to_crs_bounds : bool
            Always clip geometries to CRS bounds. (default: False)

        Returns
        -------
        data : list
        """
        return (
            []
            if self.is_empty()
            else self._read_from_cache(
                validity_check=validity_check, clip_to_crs_bounds=clip_to_crs_bounds
            )
        )

    def is_empty(self):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        if not self.tile.bbox.intersects(self.vector_file.bbox()):
            return True
        return len(self._read_from_cache(True)) == 0

    def _read_from_cache(self, validity_check=True, clip_to_crs_bounds=False):
        checked = "checked" if validity_check else "not_checked"
        if checked not in self._cache:
            self._cache[checked] = list(
                read_vector_window(
                    self.vector_file.path,
                    self.tile,
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                )
            )
        return self._cache[checked]
