import logging

from mapchete.io import copy
from mapchete.io.vector.read import fiona_read
from mapchete.io.vector.write import fiona_write
from mapchete.path import MPath
from mapchete.types import MPathLike

logger = logging.getLogger(__name__)


def convert_vector(
    inp: MPathLike,
    out: MPathLike,
    overwrite: bool = False,
    exists_ok: bool = True,
    **kwargs,
):
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
            out.rm(ignore_errors=True)
    kwargs = kwargs or {}
    if kwargs:
        logger.debug("convert vector file %s to %s using %s", str(inp), out, kwargs)
        with fiona_read(inp) as src:
            with fiona_write(out, **{**src.meta, **kwargs}) as dst:
                dst.writerecords(src)
    else:
        logger.debug("copy %s to %s", str(inp), str(out))
        out.parent.makedirs()
        copy(inp, out, overwrite=overwrite)
