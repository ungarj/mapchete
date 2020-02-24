"""
Handles writing process output into a pyramid of GeoTIFF files or a single GeoTIFF file.

output configuration parameters
-------------------------------

mandatory
~~~~~~~~~

bands: integer
    number of output bands to be written
path: string
    output directory
dtype: string
    numpy datatype

optional
~~~~~~~~

tiled: bool
    internal TIFF tiling (default: True)
blockxsize: integer
    internal tile width (default: 256)
blockysize:
    internal tile height (default: 256)
nodata: integer or float
    nodata value used for writing
compress: string
    compression method (default: lzw): lzw, jpeg, packbits, deflate, CCITTRLE,
    CCITTFAX3, CCITTFAX4, lzma
"""

from affine import Affine
from contextlib import ExitStack
import logging
import math
import numpy as np
import numpy.ma as ma
import os
import rasterio
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.shutil import copy
from rasterio.windows import from_bounds
from shapely.geometry import box
from tempfile import NamedTemporaryFile
from tilematrix import Bounds
import warnings

from mapchete.config import validate_values, snap_bounds
from mapchete.errors import MapcheteConfigError
from mapchete.formats import base
from mapchete.io import get_boto3_bucket, path_exists, path_is_remote
from mapchete.io.raster import (
    write_raster_window, prepare_array, memory_file, read_raster_no_crs,
    extract_from_array, read_raster_window
)
from mapchete.tile import BufferedTile
from mapchete.validate import deprecated_kwargs


logger = logging.getLogger(__name__)
METADATA = {
    "driver_name": "GTiff",
    "data_type": "raster",
    "mode": "rw"
}
GTIFF_DEFAULT_PROFILE = {
    "blockysize": 256,
    "blockxsize": 256,
    "tiled": True,
    "dtype": "uint8",
    "compress": "lzw",
    "interleave": "band",
    "nodata": 0
}
IN_MEMORY_THRESHOLD = int(os.environ.get("MP_IN_MEMORY_THRESHOLD", 20000 * 20000))


class OutputDataReader():
    """
    Constructor class which returns GTiffTileDirectoryOutputReader.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.tif)
    output_params : dictionary
        output parameters from Mapchete file
    nodata : integer or float
        nodata value used when writing GeoTIFFs
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    def __new__(self, output_params, **kwargs):
        """Initialize."""
        return GTiffTileDirectoryOutputReader(output_params, **kwargs)


class OutputDataWriter():
    """
    Constructor class which either returns GTiffSingleFileOutputWriter or
    GTiffTileDirectoryOutputWriter.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.tif)
    output_params : dictionary
        output parameters from Mapchete file
    nodata : integer or float
        nodata value used when writing GeoTIFFs
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    def __new__(self, output_params, **kwargs):
        """Initialize."""
        self.path = output_params["path"]
        self.file_extension = ".tif"
        if self.path.endswith(self.file_extension):
            return GTiffSingleFileOutputWriter(output_params, **kwargs)
        else:
            return GTiffTileDirectoryOutputWriter(output_params, **kwargs)


class GTiffOutputReaderFunctions():
    """Common functions."""

    METADATA = METADATA

    def empty(self, process_tile):
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : array
            empty array with data type provided in output profile
        """
        profile = self.profile(process_tile)
        return ma.masked_array(
            data=np.full(
                (profile["count"], ) + process_tile.shape,
                profile["nodata"],
                dtype=profile["dtype"]
            ),
            mask=True
        )

    def for_web(self, data):
        """
        Convert data to web output (raster only).

        Parameters
        ----------
        data : array

        Returns
        -------
        web data : array
        """
        return memory_file(
            prepare_array(
                data,
                masked=True,
                nodata=self.output_params["nodata"],
                dtype=self.profile()["dtype"]
            ),
            self.profile()
        ), "image/tiff"

    @deprecated_kwargs
    def open(self, tile, process, **kwargs):
        """
        Open process output as input for other process.

        Parameters
        ----------
        tile : ``Tile``
        process : ``MapcheteProcess``
        kwargs : keyword arguments
        """
        return InputTile(tile, process)

    def is_valid_with_config(self, config):
        """
        Check if output format is valid with other process parameters.

        Parameters
        ----------
        config : dictionary
            output configuration parameters

        Returns
        -------
        is_valid : bool
        """
        return validate_values(
            config, [
                ("bands", int),
                ("path", str),
                ("dtype", str)]
        )

    def _set_attributes(self, output_params):
        self.path = output_params["path"]
        self.file_extension = ".tif"
        self.output_params = dict(
            output_params,
            nodata=output_params.get("nodata", GTIFF_DEFAULT_PROFILE["nodata"])
        )
        self._bucket = self.path.split("/")[2] if self.path.startswith("s3://") else None


class GTiffTileDirectoryOutputReader(
    GTiffOutputReaderFunctions, base.TileDirectoryOutputReader
):

    def __init__(self, output_params, **kwargs):
        """Initialize."""
        logger.debug("output is tile directory")
        super().__init__(output_params, **kwargs)
        self._set_attributes(output_params)

    def read(self, output_tile, **kwargs):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        NumPy array
        """
        logger.debug("read %s", self.get_path(output_tile))
        try:
            return read_raster_no_crs(self.get_path(output_tile))
        except FileNotFoundError:
            return self.empty(output_tile)

    def empty(self, process_tile):
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : array
            empty array with data type provided in output profile
        """
        profile = self.profile(process_tile)
        return ma.masked_array(
            data=np.full(
                (profile["count"], ) + process_tile.shape,
                profile["nodata"],
                dtype=profile["dtype"]
            ),
            mask=True,
            fill_value=profile["nodata"]
        )

    def profile(self, tile=None):
        """
        Create a metadata dictionary for rasterio.

        Parameters
        ----------
        tile : ``BufferedTile``

        Returns
        -------
        metadata : dictionary
            output profile dictionary used for rasterio.
        """
        dst_metadata = dict(
            GTIFF_DEFAULT_PROFILE,
            count=self.output_params["bands"],
            dtype=self.output_params["dtype"],
            driver="GTiff",
            nodata=self.output_params["nodata"]
        )
        dst_metadata.pop("transform", None)
        if tile is not None:
            dst_metadata.update(
                crs=tile.crs,
                width=tile.width,
                height=tile.height,
                affine=tile.affine
            )
        else:
            for k in ["crs", "width", "height", "affine"]:
                dst_metadata.pop(k, None)
        try:
            if "compression" in self.output_params:
                warnings.warn(
                    DeprecationWarning("use 'compress' instead of 'compression'")
                )
                dst_metadata.update(compress=self.output_params["compression"])
            else:
                dst_metadata.update(compress=self.output_params["compress"])
            dst_metadata.update(predictor=self.output_params["predictor"])
        except KeyError:
            pass
        return dst_metadata


class GTiffTileDirectoryOutputWriter(
    GTiffTileDirectoryOutputReader, base.TileDirectoryOutputWriter
):
    def write(self, process_tile, data):
        """
        Write data from process tiles into GeoTIFF file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        data : ``np.ndarray``
        """
        if (
            isinstance(data, tuple) and
            len(data) == 2 and
            isinstance(data[1], dict)
        ):
            data, tags = data
        else:
            tags = {}
        data = prepare_array(
            data,
            masked=True,
            nodata=self.output_params["nodata"],
            dtype=self.profile(process_tile)["dtype"]
        )

        if data.mask.all():
            logger.debug("data empty, nothing to write")
        else:
            # in case of S3 output, create an boto3 resource
            bucket_resource = get_boto3_bucket(self._bucket) if self._bucket else None

            # Convert from process_tile to output_tiles and write
            for tile in self.pyramid.intersecting(process_tile):
                out_path = self.get_path(tile)
                self.prepare_path(tile)
                out_tile = BufferedTile(tile, self.pixelbuffer)
                write_raster_window(
                    in_tile=process_tile,
                    in_data=data,
                    out_profile=self.profile(out_tile),
                    out_tile=out_tile,
                    out_path=out_path,
                    tags=tags,
                    bucket_resource=bucket_resource
                )


class GTiffSingleFileOutputWriter(
    GTiffOutputReaderFunctions, base.SingleFileOutputWriter
):

    write_in_parent_process = True

    def __init__(self, output_params, **kwargs):
        """Initialize."""
        logger.debug("output is single file")
        self.dst = None
        super().__init__(output_params, **kwargs)
        self._set_attributes(output_params)
        if len(self.output_params["delimiters"]["zoom"]) != 1:
            raise ValueError("single file output only works with one zoom level")
        self.zoom = output_params["delimiters"]["zoom"][0]
        self.cog = output_params.get("cog", False)
        if self.cog or "overviews" in output_params:
            self.overviews = True
            self.overviews_resampling = output_params.get(
                "overviews_resampling", "nearest"
            )
            self.overviews_levels = output_params.get(
                "overviews_levels", [2**i for i in range(1, self.zoom + 1)]
            )
        else:
            self.overviews = False
        self.in_memory = output_params.get("in_memory", True)
        _bucket = self.path.split("/")[2] if self.path.startswith("s3://") else None
        self._bucket_resource = get_boto3_bucket(_bucket) if _bucket else None


    def prepare(self, process_area=None, **kwargs):
        bounds = snap_bounds(
            bounds=Bounds(
                *process_area.intersection(
                    box(*self.output_params["delimiters"]["effective_bounds"])
                ).bounds
            ),
            pyramid=self.pyramid,
            zoom=self.zoom
        ) if process_area else self.output_params["delimiters"]["effective_bounds"]
        height = math.ceil(
            (bounds.top - bounds.bottom) / self.pyramid.pixel_x_size(self.zoom)
        )
        width = math.ceil(
            (bounds.right - bounds.left) / self.pyramid.pixel_x_size(self.zoom)
        )
        logger.debug("output raster bounds: %s", bounds)
        logger.debug("output raster shape: %s, %s", height, width)
        self._profile = dict(
            GTIFF_DEFAULT_PROFILE,
            driver="GTiff",
            transform=Affine(
                self.pyramid.pixel_x_size(self.zoom),
                0,
                bounds.left,
                0,
                -self.pyramid.pixel_y_size(self.zoom),
                bounds.top
            ),
            height=height,
            width=width,
            count=self.output_params["bands"],
            crs=self.pyramid.crs,
            **{
                k: self.output_params.get(k, GTIFF_DEFAULT_PROFILE[k])
                for k in GTIFF_DEFAULT_PROFILE.keys()
            },
            bigtiff=self.output_params.get("bigtiff", "NO")
        )
        logger.debug("single GTiff profile: %s", self._profile)
        self.in_memory = (
            self.in_memory
            if self.in_memory is False
            else height * width < IN_MEMORY_THRESHOLD
        )
        # set up rasterio
        if path_exists(self.path):
            if self.output_params["mode"] != "overwrite":
                raise MapcheteConfigError(
                    "single GTiff file already exists, use overwrite mode to replace"
                )
            else:
                logger.debug("remove existing file: %s", self.path)
                os.remove(self.path)
        logger.debug("open output file: %s", self.path)
        self._ctx = ExitStack()
        # (1) use memfile if output is remote or COG
        if self.cog or path_is_remote(self.path):
            if self.in_memory:
                self._memfile = self._ctx.enter_context(MemoryFile())
                self.dst = self._ctx.enter_context(self._memfile.open(**self._profile))
            else:
                # in case output raster is too big, use tempfile on disk
                self._tempfile = self._ctx.enter_context(NamedTemporaryFile())
                self.dst = self._ctx.enter_context(
                    rasterio.open(self._tempfile.name, "w+", **self._profile)
                )
        else:
            self.dst = self._ctx.enter_context(
                rasterio.open(self.path, "w+", **self._profile)
            )

    def read(self, output_tile, **kwargs):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        NumPy array
        """
        return read_raster_window(self.dst, output_tile)

    def get_path(self, tile=None):
        """
        Determine target file path.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        path : string
        """
        return self.path

    def tiles_exist(self, process_tile=None, output_tile=None):
        """
        Check whether output tiles of a tile (either process or output) exists.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        exists : bool
        """
        if process_tile and output_tile:
            raise ValueError("just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            return any(
                not self.read(tile).mask.all()
                for tile in self.pyramid.intersecting(process_tile)
            )
        if output_tile:
            return not self.read(output_tile).mask.all()

    def write(self, process_tile, data):
        """
        Write data from process tiles into GeoTIFF file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        data = prepare_array(
            data,
            masked=True,
            nodata=self.output_params["nodata"],
            dtype=self.profile(process_tile)["dtype"]
        )

        if data.mask.all():
            logger.debug("data empty, nothing to write")
        else:
            # Convert from process_tile to output_tiles and write
            for tile in self.pyramid.intersecting(process_tile):
                out_tile = BufferedTile(tile, self.pixelbuffer)
                write_window = from_bounds(
                    *out_tile.bounds,
                    transform=self.dst.transform,
                    height=self.dst.height,
                    width=self.dst.width
                ).round_lengths(pixel_precision=0).round_offsets(pixel_precision=0)
                if _window_in_out_file(write_window, self.dst):
                    logger.debug("write data to window: %s", write_window)
                    self.dst.write(
                        extract_from_array(
                            in_raster=data,
                            in_affine=process_tile.affine,
                            out_tile=out_tile
                        ) if process_tile != out_tile else data,
                        window=write_window,
                    )

    def profile(self, tile=None):
        """
        Create a metadata dictionary for rasterio.

        Returns
        -------
        metadata : dictionary
            output profile dictionary used for rasterio.
        """
        return self._profile

    def close(self, exc_type=None, exc_value=None, exc_traceback=None):
        """Build overviews and write file."""
        try:
            # only in case no Exception was raised
            if not exc_type:
                # build overviews
                if self.overviews and self.dst is not None:
                    logger.debug(
                        "build overviews using %s resampling and levels %s",
                        self.overviews_resampling, self.overviews_levels
                    )
                    self.dst.build_overviews(
                        self.overviews_levels, Resampling[self.overviews_resampling]
                    )
                    self.dst.update_tags(
                        ns='rio_overview', resampling=self.overviews_resampling
                    )
                # write
                if self.cog:
                    if path_is_remote(self.path):
                        # remote COG: copy to tempfile and upload to destination
                        logger.debug("upload to %s", self.path)
                        # TODO this writes a memoryfile to disk and uploads the file,
                        # this is inefficient but until we find a solution to copy
                        # from one memoryfile to another the rasterio way (rasterio needs
                        # to rearrange the data so the overviews are at the beginning of
                        # the GTiff in order to be a valid COG).
                        with NamedTemporaryFile() as tmp_dst:
                            copy(
                                self.dst,
                                tmp_dst.name,
                                copy_src_overviews=True,
                                **self._profile
                            )
                            self._bucket_resource.upload_file(
                                Filename=tmp_dst.name,
                                Key="/".join(self.path.split("/")[3:]),
                            )
                    else:
                        # local COG: copy to destination
                        logger.debug("write to %s", self.path)
                        copy(
                            self.dst,
                            self.path,
                            copy_src_overviews=True,
                            **self._profile
                        )
                else:
                    if path_is_remote(self.path):
                        # remote GTiff: upload memfile or tempfile to destination
                        logger.debug("upload to %s", self.path)
                        if self.in_memory:
                            self._bucket_resource.put_object(
                                Body=self._memfile,
                                Key="/".join(self.path.split("/")[3:]),
                            )
                        else:
                            self._bucket_resource.upload_file(
                                Filename=self._tempfile.name,
                                Key="/".join(self.path.split("/")[3:]),
                            )
                    else:
                        # local GTiff: already written, do nothing
                        pass

        finally:
            self._ctx.close()


def _window_in_out_file(window, rio_file):
    return all([
        window.row_off >= 0,
        window.col_off >= 0,
        window.row_off + window.height <= rio_file.height,
        window.col_off + window.width <= rio_file.width,
    ])


class InputTile(base.InputTile):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    process : ``MapcheteProcess``
    resampling : string
        rasterio resampling method

    Attributes
    ----------
    tile : ``Tile``
    process : ``MapcheteProcess``
    resampling : string
        rasterio resampling method
    pixelbuffer : integer
    """

    def __init__(self, tile, process):
        """Initialize."""
        self.tile = tile
        self.process = process
        self.pixelbuffer = None

    def read(self, indexes=None, **kwargs):
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        indexes : integer or list
            band number or list of band numbers

        Returns
        -------
        data : array
        """
        band_indexes = self._get_band_indexes(indexes)
        arr = self.process.get_raw_output(self.tile)
        return (
            arr[band_indexes[0] - 1]
            if len(band_indexes) == 1
            else ma.concatenate([ma.expand_dims(arr[i - 1], 0) for i in band_indexes])
        )

    def is_empty(self, indexes=None):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        # empty if tile does not intersect with file bounding box
        return not self.tile.bbox.intersects(self.process.config.area_at_zoom())

    def _get_band_indexes(self, indexes=None):
        """Return valid band indexes."""
        if indexes:
            if isinstance(indexes, list):
                return indexes
            else:
                return [indexes]
        else:
            return range(1, self.process.config.output.profile(self.tile)["count"] + 1)

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Clear cache on close."""
        pass
