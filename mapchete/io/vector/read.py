"""Functions handling vector data."""

import logging
from contextlib import contextmanager
from itertools import chain
from typing import Generator, List, Union

import fiona
from fiona.errors import DriverError
from retry import retry
from shapely import prepare
from shapely.errors import TopologicalError
from shapely.geometry import mapping

from mapchete.errors import MapcheteIOError
from mapchete.geometry import (
    filter_by_geometry_type,
    multipart_to_singleparts,
    repair,
    reproject_geometry,
    segmentize_geometry,
    to_shape,
)
from mapchete.geometry.clip import clip_geometry_to_pyramid_bounds
from mapchete.geometry.types import (
    get_singlepart_type,
)
from mapchete.io.vector.types import FeatureCollectionProtocol
from mapchete.path import MPath
from mapchete.protocols import GridProtocol
from mapchete.settings import IORetrySettings
from mapchete.tile import BufferedTile
from mapchete.types import GeoJSONLikeFeature, MPathLike

__all__ = [
    "reproject_geometry",
    "segmentize_geometry",
    "to_shape",
    "multipart_to_singleparts",
]

logger = logging.getLogger(__name__)


@contextmanager
def fiona_read(
    path: MPathLike, mode: str = "r", **kwargs
) -> Generator[fiona.Collection, None, None]:
    """
    Wrapper around fiona.open but fiona.Env is set according to path properties.
    """
    path = MPath.from_inp(path)

    try:
        with path.fio_env() as env:
            logger.debug("reading %s with GDAL options %s", str(path), env.options)
            with fiona.open(str(path), mode=mode, **kwargs) as src:
                yield src
    except DriverError as fiona_exception:
        # look for hints from Fiona that the file does not exist
        for i in (
            "does not exist in the file system",
            "No such file or directory",
            "specified key does not exist.",
        ):
            if i in str(repr(fiona_exception)):  # pragma: no cover
                break
        # if there are no hints, investigate further
        else:
            # if file exists or exists check fails, raise original Fiona exception
            try:
                exists = path.exists()
            except Exception:  # pragma: no cover
                raise fiona_exception
            if exists:
                raise fiona_exception

        # file does not exist
        raise FileNotFoundError(f"path {str(path)} does not exist")


def read_vector_window(
    inp: Union[MPathLike, List[MPathLike]],
    grid: GridProtocol,
    validity_check: bool = True,
    clip_to_crs_bounds: bool = False,
    skip_missing_files: bool = False,
) -> List[GeoJSONLikeFeature]:
    """
    Read a window of an input vector dataset.

    Also clips geometry.

    Parameters
    ----------
    inp : string or IndexedFeatures
        path to vector file or an IndexedFeatures instance
    tile : ``Tile``
        tile extent to read data from
    validity_check : bool
        checks if reprojected geometry is valid and throws ``RuntimeError`` if
        invalid (default: True)
    clip_to_crs_bounds : bool
        Always clip geometries to CRS bounds. (default: False)

    Returns
    -------
    features : list
      a list of reprojected GeoJSON-like features
    """

    def _gen_features() -> Generator[GeoJSONLikeFeature, None, None]:
        for path in inp if isinstance(inp, list) else [inp]:
            path = MPath.from_inp(path)
            try:
                yield from _read_vector_window(
                    path,
                    grid,
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                )
            except FileNotFoundError:
                if skip_missing_files:
                    logger.debug("skip missing file %s", path)
                else:
                    raise

    try:
        return list(_gen_features())
    except FileNotFoundError:  # pragma: no cover
        raise
    except Exception as e:  # pragma: no cover
        raise MapcheteIOError(e)


def _read_vector_window(
    inp: MPath,
    grid: GridProtocol,
    validity_check: bool = True,
    clip_to_crs_bounds: bool = False,
):
    try:
        if isinstance(grid, BufferedTile) and grid.pixelbuffer and grid.is_on_edge():
            return chain.from_iterable(
                _get_reprojected_features_from_file(
                    inp=inp,
                    grid=grid,
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                )
                for bbox in clip_geometry_to_pyramid_bounds(
                    grid.bbox, grid.tile_pyramid
                )
            )
        else:
            features = _get_reprojected_features_from_file(
                inp=inp,
                grid=grid,
                validity_check=validity_check,
                clip_to_crs_bounds=clip_to_crs_bounds,
            )
            return features
    except FileNotFoundError:  # pragma: no cover
        raise
    except Exception as exc:  # pragma: no cover
        raise IOError(f"failed to read {inp}") from exc


@retry(
    logger=logger,
    **dict(IORetrySettings()),
)
def _get_reprojected_features_from_file(
    inp: MPath,
    grid: GridProtocol,
    validity_check: bool = False,
    clip_to_crs_bounds: bool = False,
) -> Generator[GeoJSONLikeFeature, None, None]:
    logger.debug("reading %s", inp)
    with fiona_read(inp) as src:
        # reproject tile bounding box to source file CRS for filter
        yield from reprojected_features(
            src,
            grid,
            validity_check=validity_check,
            clip_to_crs_bounds=clip_to_crs_bounds,
        )


def reprojected_features(
    src: FeatureCollectionProtocol,
    grid: GridProtocol,
    validity_check: bool = False,
    clip_to_crs_bounds: bool = False,
) -> Generator[GeoJSONLikeFeature, None, None]:
    if src.crs == grid.crs:
        dst_bbox = to_shape(grid)
    else:
        dst_bbox = reproject_geometry(
            to_shape(grid),
            src_crs=grid.crs,
            dst_crs=src.crs,
            validity_check=True,
        )
    prepare(dst_bbox)
    for feature in src.filter(bbox=dst_bbox.bounds):
        try:
            # check validity
            original_geom = repair(to_shape(feature["geometry"]))

            # clip with bounds and omit if clipped geometry is empty
            clipped_geom = dst_bbox.intersection(original_geom)
            for checked_geom in filter_by_geometry_type(
                clipped_geom,
                get_singlepart_type(original_geom.geom_type),
            ):
                # reproject each feature to tile CRS
                reprojected_geom = reproject_geometry(
                    checked_geom,
                    src_crs=src.crs,
                    dst_crs=grid.crs,
                    validity_check=validity_check,
                    clip_to_crs_bounds=False,
                )
                if not reprojected_geom.is_empty:
                    yield {
                        "properties": feature["properties"],
                        "geometry": mapping(reprojected_geom),
                    }
        # this can be handled quietly
        except TopologicalError as e:  # pragma: no cover
            logger.warning("feature omitted: %s", e)
