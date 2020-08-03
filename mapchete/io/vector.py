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

from mapchete.errors import GeometryTypeError
from mapchete.io._path import path_exists
from mapchete.io._geometry_operations import (
    reproject_geometry,
    segmentize_geometry,
    to_shape,
    multipart_to_singleparts,
    clean_geometry_type,
    _repair
)

__all__ = [
    "reproject_geometry",
    "segmentize_geometry",
    "to_shape",
    "multipart_to_singleparts",
    "clean_geometry_type"
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
    return [
        feature
        for feature in chain.from_iterable([
            _read_vector_window(path, tile, validity_check=validity_check)
            for path in (input_files if isinstance(input_files, list) else [input_files])
        ])
    ]


def _read_vector_window(input_file, tile, validity_check=True):
    if tile.pixelbuffer and tile.is_on_edge():
        return chain.from_iterable(
            _get_reprojected_features(
                input_file=input_file,
                dst_bounds=bbox.bounds,
                dst_crs=tile.crs,
                validity_check=validity_check
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
            validity_check=validity_check
        )
        return features


def write_vector_window(
    in_data=None, out_schema=None, out_tile=None, out_path=None, bucket_resource=None
):
    """
    Write features to GeoJSON file.

    Parameters
    ----------
    in_data : features
    out_schema : dictionary
        output schema for fiona
    out_tile : ``BufferedTile``
        tile used for output extent
    out_path : string
        output path for GeoJSON file
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
            # if clipped feature still
            for out_geom in multipart_to_singleparts(
                clean_geometry_type(
                    to_shape(feature["geometry"]).intersection(out_tile.bbox),
                    out_schema["geometry"]
                )
            ):
                if out_geom.is_empty:
                    continue
                out_features.append({
                    "geometry": mapping(out_geom),
                    "properties": feature["properties"]
                })
        except Exception as e:
            logger.warning("failed to prepare geometry for writing: %s", e)
            continue

    # write if there are output features
    if out_features:
        try:
            if out_path.startswith("s3://"):
                # write data to remote file
                with VectorWindowMemoryFile(
                    tile=out_tile,
                    features=out_features,
                    schema=out_schema,
                    driver="GeoJSON"
                ) as memfile:
                    logger.debug((out_tile.id, "upload tile", out_path))
                    bucket_resource.put_object(
                        Key="/".join(out_path.split("/")[3:]),
                        Body=memfile
                    )
            else:
                # write data to local file
                with fiona.open(
                    out_path, 'w', schema=out_schema, driver="GeoJSON",
                    crs=out_tile.crs.to_dict()
                ) as dst:
                    logger.debug((out_tile.id, "write tile", out_path))
                    dst.writerecords(out_features)
        except Exception as e:
            logger.error("error while writing file %s: %s", out_path, e)
            raise

    else:
        logger.debug((out_tile.id, "nothing to write", out_path))


class VectorWindowMemoryFile():
    """Context manager around fiona.io.MemoryFile."""

    def __init__(
        self, tile=None, features=None, schema=None, driver=None
    ):
        """Prepare data & profile."""
        self.tile = tile
        self.schema = schema
        self.driver = driver
        self.features = features

    def __enter__(self):
        """Open MemoryFile, write data and return."""
        self.fio_memfile = MemoryFile()
        with self.fio_memfile.open(
            schema=self.schema,
            driver=self.driver,
            crs=self.tile.crs
        ) as dst:
            dst.writerecords(self.features)
        return self.fio_memfile

    def __exit__(self, *args):
        """Make sure MemoryFile is closed."""
        self.fio_memfile.close()


@retry(
    tries=3, logger=logger, exceptions=(DriverError, FionaError, FionaValueError), delay=1
)
def _get_reprojected_features(
    input_file=None, dst_bounds=None, dst_crs=None, validity_check=False
):
    logger.debug("reading %s", input_file)
    try:
        with fiona.open(input_file, 'r') as src:
            src_crs = CRS(src.crs)
            # reproject tile bounding box to source file CRS for filter
            if src_crs == dst_crs:
                dst_bbox = box(*dst_bounds)
            else:
                dst_bbox = reproject_geometry(
                    box(*dst_bounds),
                    src_crs=dst_crs,
                    dst_crs=src_crs,
                    validity_check=True
                )
            for feature in src.filter(bbox=dst_bbox.bounds):

                try:
                    # check validity
                    original_geom = _repair(to_shape(feature['geometry']))

                    # clip with bounds and omit if clipped geometry is empty
                    clipped_geom = original_geom.intersection(dst_bbox)

                    if not clipped_geom.is_empty:
                        # reproject each feature to tile CRS
                        g = reproject_geometry(
                            clean_geometry_type(clipped_geom, original_geom.geom_type),
                            src_crs=src_crs,
                            dst_crs=dst_crs,
                            validity_check=validity_check
                        )
                        yield {
                            'properties': feature['properties'],
                            'geometry': mapping(g)
                        }
                # this can be handled quietly
                except GeometryTypeError:
                    pass
                except TopologicalError as e:  # pragma: no cover
                    logger.warning("feature omitted: %s", e)

    except Exception as e:
        if path_exists(input_file):
            logger.error("error while reading file %s: %s", input_file, e)
            raise e
        else:
            raise FileNotFoundError("%s not found" % input_file)
