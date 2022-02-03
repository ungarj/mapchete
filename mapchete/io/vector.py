"""Functions handling vector data."""

import os
import logging
import fiona
from fiona.errors import DriverError, FionaError, FionaValueError
from fiona.io import MemoryFile
from retry import retry
from rasterio.crs import CRS
from shapely.geometry import box, mapping
from shapely.errors import TopologicalError
from tilematrix import clip_geometry_to_srs_bounds
from itertools import chain

from mapchete.errors import GeometryTypeError, MapcheteIOError
from mapchete.io._misc import MAPCHETE_IO_RETRY_SETTINGS
from mapchete.io._path import fs_from_path, path_exists
from mapchete.io._geometry_operations import (
    reproject_geometry,
    segmentize_geometry,
    to_shape,
    multipart_to_singleparts,
    clean_geometry_type,
    _repair,
)

__all__ = [
    "reproject_geometry",
    "segmentize_geometry",
    "to_shape",
    "multipart_to_singleparts",
    "clean_geometry_type",
]

logger = logging.getLogger(__name__)


def read_vector_window(input_files, tile, validity_check=True):
    """
    Read a window of an input vector dataset.

    Also clips geometry.

    Parameters
    ----------
    input_file : string
        path to vector file
    tile : ``Tile``
        tile extent to read data from
    validity_check : bool
        checks if reprojected geometry is valid and throws ``RuntimeError`` if
        invalid (default: True)

    Returns
    -------
    features : list
      a list of reprojected GeoJSON-like features
    """
    try:
        return [
            feature
            for feature in chain.from_iterable(
                [
                    _read_vector_window(path, tile, validity_check=validity_check)
                    for path in (
                        input_files if isinstance(input_files, list) else [input_files]
                    )
                ]
            )
        ]
    except FileNotFoundError:  # pragma: no cover
        raise
    except Exception as e:  # pragma: no cover
        raise MapcheteIOError(e)


def _read_vector_window(input_file, tile, validity_check=True):
    if tile.pixelbuffer and tile.is_on_edge():
        return chain.from_iterable(
            _get_reprojected_features(
                input_file=input_file,
                dst_bounds=bbox.bounds,
                dst_crs=tile.crs,
                validity_check=validity_check,
            )
            for bbox in clip_geometry_to_srs_bounds(
                tile.bbox, tile.tile_pyramid, multipart=True
            )
        )
    else:
        features = _get_reprojected_features(
            input_file=input_file,
            dst_bounds=tile.bounds,
            dst_crs=tile.crs,
            validity_check=validity_check,
        )
        return features


def write_vector_window(
    in_data=None,
    out_driver="GeoJSON",
    out_schema=None,
    out_tile=None,
    out_path=None,
    bucket_resource=None,
    allow_multipart_geometries=True,
):
    """
    Write features to file.

    When the output driver is 'Geobuf', the geobuf library will be used otherwise the
    driver will be passed on to Fiona.

    Parameters
    ----------
    in_data : features
    out_driver : string
    out_schema : dictionary
        output schema for fiona
    out_tile : ``BufferedTile``
        tile used for output extent
    out_path : string
        output path for file
    """
    # Delete existing file.
    try:
        os.remove(out_path)
    except OSError:
        pass

    out_features = []
    for feature in in_data:
        try:
            # clip feature geometry to tile bounding box and append for writing
            clipped = clean_geometry_type(
                to_shape(feature["geometry"]).intersection(out_tile.bbox),
                out_schema["geometry"],
            )
            if allow_multipart_geometries:
                cleaned_output_fetures = [clipped]
            else:
                cleaned_output_fetures = multipart_to_singleparts(clipped)
            for out_geom in cleaned_output_fetures:
                if out_geom.is_empty:  # pragma: no cover
                    continue

                out_features.append(
                    {"geometry": mapping(out_geom), "properties": feature["properties"]}
                )
        except Exception as e:
            logger.warning("failed to prepare geometry for writing: %s", e)
            continue

    # write if there are output features
    if out_features:
        try:
            if out_driver.lower() in ["geobuf"]:
                # write data to remote file
                with VectorWindowMemoryFile(
                    tile=out_tile,
                    features=out_features,
                    schema=out_schema,
                    driver=out_driver,
                ) as memfile:
                    logger.debug((out_tile.id, "write tile", out_path))
                    with fs_from_path(out_path).open(out_path, "wb") as dst:
                        dst.write(memfile)
            else:  # pragma: no cover
                # write data to local file
                # this part is not covered by tests as we now try to let fiona directly
                # write to S3
                with fiona.open(
                    out_path,
                    "w",
                    schema=out_schema,
                    driver=out_driver,
                    crs=out_tile.crs.to_dict(),
                ) as dst:
                    logger.debug((out_tile.id, "write tile", out_path))
                    dst.writerecords(out_features)
        except Exception as e:
            logger.error("error while writing file %s: %s", out_path, e)
            raise

    else:
        logger.debug((out_tile.id, "nothing to write", out_path))


class VectorWindowMemoryFile:
    """Context manager around fiona.io.MemoryFile."""

    def __init__(self, tile=None, features=None, schema=None, driver=None):
        """Prepare data & profile."""
        self.tile = tile
        self.schema = schema
        self.driver = driver
        self.features = features

    def __enter__(self):
        """Open MemoryFile, write data and return."""
        if self.driver.lower() == "geobuf":
            import geobuf

            return geobuf.encode(
                dict(
                    type="FeatureCollection",
                    features=[dict(f, type="Feature") for f in self.features],
                )
            )
        else:  # pragma: no cover
            # this part is excluded now for tests as we try to let fiona write directly
            # to S3
            self.fio_memfile = MemoryFile()
            with self.fio_memfile.open(
                schema=self.schema, driver=self.driver, crs=self.tile.crs
            ) as dst:
                dst.writerecords(self.features)
            return self.fio_memfile.getbuffer()

    def __exit__(self, *args):
        """Make sure MemoryFile is closed."""
        try:
            self.fio_memfile.close()
        except AttributeError:
            pass


@retry(
    logger=logger,
    exceptions=(DriverError, FionaError, FionaValueError),
    **MAPCHETE_IO_RETRY_SETTINGS
)
def _get_reprojected_features(
    input_file=None, dst_bounds=None, dst_crs=None, validity_check=False
):
    logger.debug("reading %s", input_file)
    try:
        with fiona.open(input_file, "r") as src:
            src_crs = CRS(src.crs)
            # reproject tile bounding box to source file CRS for filter
            if src_crs == dst_crs:
                dst_bbox = box(*dst_bounds)
            else:
                dst_bbox = reproject_geometry(
                    box(*dst_bounds),
                    src_crs=dst_crs,
                    dst_crs=src_crs,
                    validity_check=True,
                )
            for feature in src.filter(bbox=dst_bbox.bounds):

                try:
                    # check validity
                    original_geom = _repair(to_shape(feature["geometry"]))

                    # clip with bounds and omit if clipped geometry is empty
                    clipped_geom = original_geom.intersection(dst_bbox)

                    # reproject each feature to tile CRS
                    g = reproject_geometry(
                        clean_geometry_type(
                            clipped_geom, original_geom.geom_type, raise_exception=False
                        ),
                        src_crs=src_crs,
                        dst_crs=dst_crs,
                        validity_check=validity_check,
                    )
                    if not g.is_empty:
                        yield {
                            "properties": feature["properties"],
                            "geometry": mapping(g),
                        }
                # this can be handled quietly
                except TopologicalError as e:  # pragma: no cover
                    logger.warning("feature omitted: %s", e)

    except Exception as e:
        if path_exists(input_file):
            logger.error("error while reading file %s: %s", input_file, e)
            raise e
        else:
            raise FileNotFoundError("%s not found" % input_file)
