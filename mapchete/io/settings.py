import logging
import os

logger = logging.getLogger(__name__)


def _merge_gdal_defaults_with_env():
    return {k: os.environ.get(k, v) for k, v in GDAL_HTTP_DEFAULTS.items()}


# defaults sets according to the recommendations given at
# https://developmentseed.org/titiler/advanced/performance_tuning/
GDAL_HTTP_DEFAULTS = dict(
    # this will be set later on depending on the opened file
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS="",
    # 200MB
    CPL_VSIL_CURL_CACHE_SIZE=200_000_000,
    # alternative: ARRAY
    GDAL_BAND_BLOCK_CACHE="HASHSET",
    # # 200MB
    # GDAL_CACHEMAX=200, --> activating this seems to let the tests stall at some point
    # don't make LIST request
    GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
    GDAL_HTTP_TIMEOUT=30,
    GDAL_HTTP_MAX_RETRY=3,
    GDAL_HTTP_MERGE_CONSECUTIVE_RANGES=True,
    GDAL_HTTP_MULTIPLEX=True,
    GDAL_HTTP_RETRY_DELAY=5,
    GDAL_HTTP_VERSION=2,
    # let GDAL cache internally
    VSI_CACHE=True,
    # 5MB cache per file
    VSI_CACHE_SIZE=5_000_000,
)
GDAL_HTTP_OPTS = _merge_gdal_defaults_with_env()
MAPCHETE_IO_RETRY_SETTINGS = {
    "tries": int(os.environ.get("MAPCHETE_IO_RETRY_TRIES", "3")),
    "delay": float(os.environ.get("MAPCHETE_IO_RETRY_DELAY", "1")),
    "backoff": float(os.environ.get("MAPCHETE_IO_RETRY_BACKOFF", "1")),
}
