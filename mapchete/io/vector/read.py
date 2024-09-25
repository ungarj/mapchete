"""Functions handling vector data."""

import logging
from contextlib import ExitStack, contextmanager
from itertools import chain
from typing import Generator, List, Literal, Union, Optional

import fiona
from fiona.errors import DriverError
from rasterio.crs import CRS
from retry import retry
from shapely.errors import TopologicalError
from shapely.geometry import mapping, shape

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
    GeoJSONLikeFeature,
    get_singlepart_type,
)
from mapchete.io import copy
from mapchete.io.vector.indexed_features import IndexedFeatures
from mapchete.io.vector.write import fiona_write
from mapchete.path import MPath, fs_from_path
from mapchete.settings import IORetrySettings
from mapchete.tile import BufferedTile
from mapchete.types import Bounds, CRSLike, MPathLike

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
    inp: Union[MPathLike, IndexedFeatures, List[MPathLike], List[IndexedFeatures]],
    tile: BufferedTile,
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
            try:
                yield from _read_vector_window(
                    path,
                    tile,
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
    inp: Union[MPathLike, IndexedFeatures],
    tile: BufferedTile,
    validity_check: bool = True,
    clip_to_crs_bounds: bool = False,
):
    try:
        if tile.pixelbuffer and tile.is_on_edge():
            return chain.from_iterable(
                _get_reprojected_features(
                    inp=inp,
                    dst_bounds=Bounds.from_inp(bbox.bounds),
                    dst_crs=tile.crs,
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                )
                for bbox in clip_geometry_to_pyramid_bounds(
                    tile.bbox, tile.tile_pyramid
                )
            )
        else:
            features = _get_reprojected_features(
                inp=inp,
                dst_bounds=Bounds.from_inp(tile.bounds),
                dst_crs=tile.crs,
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
def _get_reprojected_features(
    inp: Union[MPathLike, IndexedFeatures],
    dst_bounds: Bounds,
    dst_crs: CRSLike,
    validity_check: bool = False,
    clip_to_crs_bounds: bool = False,
) -> Generator[GeoJSONLikeFeature, None, None]:
    logger.debug("reading %s", inp)
    with ExitStack() as exit_stack:
        if isinstance(inp, (str, MPath)):
            src = exit_stack.enter_context(fiona_read(inp))
            src_crs = CRS(src.crs)
        elif isinstance(inp, IndexedFeatures):
            src = inp
            src_crs = inp.crs
        else:  # pragma: no cover
            raise TypeError(f"input must be either a path or IndexedFeatures: {inp}")
        # reproject tile bounding box to source file CRS for filter
        if src_crs == dst_crs:
            dst_bbox = shape(dst_bounds)
        else:
            dst_bbox = reproject_geometry(
                shape(dst_bounds),
                src_crs=dst_crs,
                dst_crs=src_crs,
                validity_check=True,
            )
        for feature in src.filter(bbox=dst_bbox.bounds):
            try:
                # check validity
                original_geom = repair(to_shape(feature["geometry"]))

                # clip with bounds and omit if clipped geometry is empty
                clipped_geom = original_geom.intersection(dst_bbox)
                for checked_geom in filter_by_geometry_type(
                    clipped_geom,
                    get_singlepart_type(original_geom.geom_type),
                ):
                    # reproject each feature to tile CRS
                    reprojected_geom = reproject_geometry(
                        checked_geom,
                        src_crs=src_crs,
                        dst_crs=dst_crs,
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


def convert_vector(inp, out, overwrite=False, exists_ok=True, **kwargs):
    """
    Convert vector file to a differernt format.

    When kwargs are given, the operation will be conducted by Fiona, without kwargs,
    the file is simply copied to the destination using fsspec.

    Parameters
    ----------
    inp : str
        Path to input file.
    out : str
        Path to output file.
    overwrite : bool
        Overwrite output file. (default: False)
    skip_exists : bool
        Skip conversion if outpu already exists. (default: True)
    kwargs : mapping
        Creation parameters passed on to output file.
    """
    inp = MPath.from_inp(inp)
    out = MPath.from_inp(out)
    if out.exists():
        if not exists_ok:
            raise IOError(f"{out} already exists")
        elif not overwrite:
            logger.debug("output %s already exists and will not be overwritten")
            return
        else:
            fs_from_path(out).rm(out)
    kwargs = kwargs or {}
    if kwargs:
        logger.debug("convert vector file %s to %s using %s", str(inp), out, kwargs)
        with fiona_read(inp) as src:
            with fiona_write(out, mode="w", **{**src.meta, **kwargs}) as dst:
                dst.writerecords(src)
    else:
        logger.debug("copy %s to %s", str(inp), str(out))
        out.parent.makedirs()
        copy(inp, out, overwrite=overwrite)


def read_vector(
    inp: MPathLike,
    index: Optional[Literal["rtree"]] = "rtree",
) -> IndexedFeatures:
    with fiona_read(inp) as src:
        return IndexedFeatures(src, index=index)
