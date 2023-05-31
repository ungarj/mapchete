import logging
import os

logger = logging.getLogger(__name__)


GDAL_HTTP_OPTS = dict(
    GDAL_DISABLE_READDIR_ON_OPEN=os.environ.get(
        "GDAL_DISABLE_READDIR_ON_OPEN", "TRUE"
    ).upper()
    == "TRUE",
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".ovr, .xml, .rpc",
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
