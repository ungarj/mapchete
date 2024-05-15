import logging

from mapchete.io import copy
from mapchete.io.raster.open import rasterio_open
from mapchete.path import MPath

logger = logging.getLogger(__name__)


def convert_raster(inp, out, overwrite=False, exists_ok=True, **kwargs):
    """
    Convert raster file to a differernt format.

    When kwargs are given, the operation will be conducted by rasterio, without kwargs,
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
            raise OSError(f"{str(out)} already exists")
        elif not overwrite:
            logger.debug("output %s already exists and will not be overwritten")
            return
    kwargs = kwargs or {}
    if kwargs:
        logger.debug("convert raster file %s to %s using %s", inp, out, kwargs)
        with rasterio_open(inp, "r") as src:
            with rasterio_open(out, mode="w", **{**src.meta, **kwargs}) as dst:
                dst.write(src.read())
    else:
        logger.debug("copy %s to %s", inp, (out))
        out.parent.makedirs()
        copy(inp, out, overwrite=overwrite)
