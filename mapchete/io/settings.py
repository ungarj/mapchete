import logging
import os

logger = logging.getLogger(__name__)


GDAL_HTTP_OPTS = dict(
    GDAL_DISABLE_READDIR_ON_OPEN=os.environ.get(
        "GDAL_DISABLE_READDIR_ON_OPEN", "TRUE"
    ).upper()
    == "TRUE",
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif, .ovr, .jp2, .png, .xml, .rpc",
    GDAL_HTTP_TIMEOUT=int(os.environ.get("GDAL_HTTP_TIMEOUT", "30")),
    GDAL_HTTP_MAX_RETRY=int(os.environ.get("GDAL_HTTP_MAX_RETRY", "3")),
    GDAL_HTTP_MERGE_CONSECUTIVE_RANGES=os.environ.get(
        "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES", "TRUE"
    ).upper()
    == "TRUE",
    GDAL_HTTP_RETRY_DELAY=int(os.environ.get("GDAL_HTTP_RETRY_DELAY", "5")),
)
MAPCHETE_IO_RETRY_SETTINGS = {
    "tries": int(os.environ.get("MAPCHETE_IO_RETRY_TRIES", "3")),
    "delay": float(os.environ.get("MAPCHETE_IO_RETRY_DELAY", "1")),
    "backoff": float(os.environ.get("MAPCHETE_IO_RETRY_BACKOFF", "1")),
}


def get_gdal_options(opts, is_remote=False, allowed_remote_extensions=[]):
    """
    Return a merged set of custom and default GDAL/rasterio Env options.

    If is_remote is set to True, the default GDAL_HTTP_OPTS are appended.

    Parameters
    ----------
    opts : dict or None
        Explicit GDAL options.
    is_remote : bool
        Indicate whether Env is for a remote file.

    Returns
    -------
    dictionary
    """
    user_opts = {} if opts is None else dict(**opts)
    if is_remote:
        gdal_opts = dict(GDAL_HTTP_OPTS)
        if allowed_remote_extensions:
            gdal_opts.update(CPL_VSIL_CURL_ALLOWED_EXTENSIONS=allowed_remote_extensions)
        gdal_opts.update(user_opts)
    else:
        gdal_opts = user_opts
    logger.debug("using GDAL options: %s", gdal_opts)
    return gdal_opts
