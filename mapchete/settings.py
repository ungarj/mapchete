"""
Combine default values with environment variable values.
"""

from typing import Literal, Tuple, Type, Union

from aiohttp import ClientPayloadError, ClientResponseError
from aiohttp.client_exceptions import ServerDisconnectedError
from fiona.errors import FionaError
from fsspec.exceptions import FSTimeoutError
from pydantic import NonNegativeFloat, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict
from rasterio.errors import RasterioError

from mapchete.executor import Concurrency


# defaults sets according to the recommendations given at
# https://developmentseed.org/titiler/advanced/performance_tuning/
class GDALHTTPOptions(BaseSettings):
    # this will be set later on depending on the opened file
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS: str = ""
    # 200MB
    CPL_VSIL_CURL_CACHE_SIZE: NonNegativeInt = 200_000_000
    # alternative: ARRAY
    GDAL_BAND_BLOCK_CACHE: Literal["AUTO", "ARRAY", "HASHSET"] = "HASHSET"
    # # 200MB
    # GDAL_CACHEMAX=200, --> activating this seems to let the tests stall at some point
    # don't make LIST request
    GDAL_DISABLE_READDIR_ON_OPEN: Union[bool, Literal["EMPTY_DIR"]] = "EMPTY_DIR"
    GDAL_HTTP_TIMEOUT: NonNegativeFloat = 30
    GDAL_HTTP_MAX_RETRY: NonNegativeInt = 3
    GDAL_HTTP_MERGE_CONSECUTIVE_RANGES: bool = True
    GDAL_HTTP_MULTIPLEX: bool = True
    GDAL_HTTP_RETRY_DELAY: NonNegativeFloat = 5
    GDAL_HTTP_VERSION: int = 2
    # let GDAL cache internally
    VSI_CACHE: bool = True
    # 5MB cache per file
    VSI_CACHE_SIZE: NonNegativeInt = 5_000_000

    # read from environment
    model_config = SettingsConfigDict()


class IORetrySettings(BaseSettings):
    """Combine default retry settings with env variables.

    MAPCHETE_IO_RETRY_TRIES
    MAPCHETE_IO_RETRY_DELAY
    MAPCHETE_IO_RETRY_BACKOFF
    """

    tries: NonNegativeInt = 3
    delay: NonNegativeFloat = 1.0
    backoff: NonNegativeFloat = 1.0
    # only retry the most common exceptions which do not hint to
    # a permanent issue (such as FileNotFoundError, ...).
    exceptions: Tuple[Type[Exception], ...] = (
        AttributeError,
        BufferError,
        ConnectionError,
        InterruptedError,
        LookupError,
        NameError,
        SystemError,
        TimeoutError,
        RasterioError,
        FionaError,
        FSTimeoutError,
        ServerDisconnectedError,
        ClientResponseError,
        ClientPayloadError,
    )

    # read from environment
    model_config = SettingsConfigDict(env_prefix="MAPCHETE_IO_RETRY_")


class MapcheteOptions(BaseSettings):
    # timeout granted when fetching future results or exceptions
    future_timeout: NonNegativeFloat = 10
    tiles_exist_concurrency: Concurrency = Concurrency.threads
    reproject_geometry_engine: Literal["pyproj", "fiona"] = "pyproj"
    execute_retries: NonNegativeInt = 0
    execute_delay: NonNegativeFloat = 0

    # read from environment
    model_config = SettingsConfigDict(env_prefix="MAPCHETE_")


mapchete_options = MapcheteOptions()
