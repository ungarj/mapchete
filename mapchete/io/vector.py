"""Functions handling vector data."""

import os
import logging
import fiona
from fiona.transform import transform_geom
from fiona.io import MemoryFile
from retry import retry
from rasterio.crs import CRS
from shapely.geometry import (
    box, shape, mapping, MultiPoint, MultiLineString, MultiPolygon, Polygon,
    LinearRing, LineString, base
)
from shapely.errors import TopologicalError
from shapely.validation import explain_validity
from tilematrix import clip_geometry_to_srs_bounds
from itertools import chain

from mapchete.errors import GeometryTypeError
from mapchete._validate import validate_crs

logger = logging.getLogger(__name__)


CRS_BOUNDS = {
    # http://spatialreference.org/ref/epsg/wgs-84/
    'epsg:4326': (-180., -90., 180., 90.),
    # unknown source
    'epsg:3857': (-180., -85.0511, 180., 85.0511),
    # http://spatialreference.org/ref/epsg/3035/
    'epsg:3035': (-10.6700, 34.5000, 31.5500, 71.0500)
}


def reproject_geometry(
    geometry, src_crs=None, dst_crs=None, error_on_clip=False, validity_check=True,
    antimeridian_cutting=False
):
    """
    Reproject a geometry to target CRS.

    Also, clips geometry if it lies outside the destination CRS boundary.
    Supported destination CRSes for clipping: 4326 (WGS84), 3857 (Spherical
    Mercator) and 3035 (ETRS89 / ETRS-LAEA).

    Parameters
    ----------
    geometry : ``shapely.geometry``
    src_crs : ``rasterio.crs.CRS`` or EPSG code
        CRS of source data
    dst_crs : ``rasterio.crs.CRS`` or EPSG code
        target CRS
    error_on_clip : bool
        raises a ``RuntimeError`` if a geometry is outside of CRS bounds
        (default: False)
    validity_check : bool
        checks if reprojected geometry is valid and throws ``TopologicalError``
        if invalid (default: True)
    antimeridian_cutting : bool
        cut geometry at Antimeridian; can result in a multipart output geometry

    Returns
    -------
    geometry : ``shapely.geometry``
    """
    src_crs = validate_crs(src_crs)
    dst_crs = validate_crs(dst_crs)

    def _reproject_geom(geometry, src_crs, dst_crs):
        if geometry.is_empty:
            return geometry
        else:
            out_geom = to_shape(
                transform_geom(
                    src_crs.to_dict(),
                    dst_crs.to_dict(),
                    mapping(geometry),
                    antimeridian_cutting=antimeridian_cutting
                )
            )
            return _repair(out_geom) if validity_check else out_geom

    # return repaired geometry if no reprojection needed
    if src_crs == dst_crs or geometry.is_empty:
        return _repair(geometry)

    # geometry needs to be clipped to its CRS bounds
    elif (
        dst_crs.is_epsg_code and               # just in case for an CRS with EPSG code
        dst_crs.get("init") in CRS_BOUNDS and  # if CRS has defined bounds
        dst_crs.get("init") != "epsg:4326"     # and is not WGS84 (does not need clipping)
    ):
        wgs84_crs = CRS().from_epsg(4326)
        # get dst_crs boundaries
        crs_bbox = box(*CRS_BOUNDS[dst_crs.get("init")])
        # reproject geometry to WGS84
        geometry_4326 = _reproject_geom(geometry, src_crs, wgs84_crs)
        # raise error if geometry has to be clipped
        if error_on_clip and not geometry_4326.within(crs_bbox):
            raise RuntimeError("geometry outside target CRS bounds")
        # clip geometry dst_crs boundaries and return
        return _reproject_geom(crs_bbox.intersection(geometry_4326), wgs84_crs, dst_crs)

    # return without clipping if destination CRS does not have defined bounds
    else:
        return _reproject_geom(geometry, src_crs, dst_crs)


def _repair(geom):
    repaired = geom.buffer(0) if geom.geom_type in ["Polygon", "MultiPolygon"] else geom
    if repaired.is_valid:
        return repaired
    else:
        raise TopologicalError(
            "geometry is invalid (%s) and cannot be repaired" % explain_validity(repaired)
        )


def segmentize_geometry(geometry, segmentize_value):
    """
    Segmentize Polygon outer ring by segmentize value.

    Just Polygon geometry type supported.

    Parameters
    ----------
    geometry : ``shapely.geometry``
    segmentize_value: float

    Returns
    -------
    geometry : ``shapely.geometry``
    """
    if geometry.geom_type != "Polygon":
        raise TypeError("segmentize geometry type must be Polygon")

    return Polygon(
        LinearRing([
            p
            # pick polygon linestrings
            for l in map(
                lambda x: LineString([x[0], x[1]]),
                zip(geometry.exterior.coords[:-1], geometry.exterior.coords[1:])
            )
            # interpolate additional points in between and don't forget end point
            for p in [
                l.interpolate(segmentize_value * i).coords[0]
                for i in range(int(l.length / segmentize_value))
            ] + [l.coords[1]]
        ])
    )


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
    if not isinstance(input_files, list):
        input_files = [input_files]
    return [
        feature
        for feature in chain.from_iterable([
            _read_vector_window(path, tile, validity_check=validity_check)
            for path in input_files
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


@retry(tries=3, logger=logger)
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
                except TopologicalError as e:
                    logger.warning("feature omitted: %s", e)

    except Exception as e:
        logger.error("error while reading file %s: %s", input_file, e)
        raise


def clean_geometry_type(geometry, target_type, allow_multipart=True):
    """
    Return geometry of a specific type if possible.

    Filters and splits up GeometryCollection into target types. This is
    necessary when after clipping and/or reprojecting the geometry types from
    source geometries change (i.e. a Polygon becomes a LineString or a
    LineString becomes Point) in some edge cases.

    Parameters
    ----------
    geometry : ``shapely.geometry``
    target_type : string
        target geometry type
    allow_multipart : bool
        allow multipart geometries (default: True)

    Returns
    -------
    cleaned geometry : ``shapely.geometry``
        returns None if input geometry type differs from target type

    Raises
    ------
    GeometryTypeError : if geometry type does not match target_type
    """
    multipart_geoms = {
        "Point": MultiPoint,
        "LineString": MultiLineString,
        "Polygon": MultiPolygon,
        "MultiPoint": MultiPoint,
        "MultiLineString": MultiLineString,
        "MultiPolygon": MultiPolygon
    }

    if target_type not in multipart_geoms.keys():
        raise TypeError("target type is not supported: %s" % target_type)

    if geometry.geom_type == target_type:
        return geometry

    elif allow_multipart:
        target_multipart_type = multipart_geoms[target_type]
        if geometry.geom_type == "GeometryCollection":
            return target_multipart_type([
                clean_geometry_type(g, target_type, allow_multipart)
                for g in geometry])
        elif any([
            isinstance(geometry, target_multipart_type),
            multipart_geoms[geometry.geom_type] == target_multipart_type
        ]):
            return geometry

    raise GeometryTypeError(
        "geometry type does not match: %s, %s" % (geometry.geom_type, target_type)
    )


def to_shape(geom):
    """
    Convert geometry to shapely geometry if necessary.

    Parameters
    ----------
    geom : shapely geometry or GeoJSON mapping

    Returns
    -------
    shapely geometry
    """
    return shape(geom) if isinstance(geom, dict) else geom


def multipart_to_singleparts(geom):
    """
    Yield single part geometries if geom is multipart, otherwise yield geom.

    Parameters
    ----------
    geom : shapely geometry

    Returns
    -------
    shapely single part geometries
    """
    if isinstance(geom, base.BaseGeometry):
        if hasattr(geom, "geoms"):
            for subgeom in geom:
                yield subgeom
        else:
            yield geom
