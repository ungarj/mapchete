"""
Combine default values with environment variable values.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


# defaults sets according to the recommendations given at
# https://developmentseed.org/titiler/advanced/performance_tuning/
class GDALHTTPOptions(BaseSettings):
    # this will be set later on depending on the opened file
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS: str = ""
    # 200MB
    CPL_VSIL_CURL_CACHE_SIZE: int = 200_000_000
    # alternative: ARRAY
    GDAL_BAND_BLOCK_CACHE: str = "HASHSET"
    # # 200MB
    # GDAL_CACHEMAX=200, --> activating this seems to let the tests stall at some point
    # don't make LIST request
    GDAL_DISABLE_READDIR_ON_OPEN: str = "EMPTY_DIR"
    GDAL_HTTP_TIMEOUT: int = 30
    GDAL_HTTP_MAX_RETRY: int = 3
    GDAL_HTTP_MERGE_CONSECUTIVE_RANGES: bool = True
    GDAL_HTTP_MULTIPLEX: bool = True
    GDAL_HTTP_RETRY_DELAY: int = 5
    GDAL_HTTP_VERSION: int = 2
    # let GDAL cache internally
    VSI_CACHE: bool = True
    # 5MB cache per file
    VSI_CACHE_SIZE: int = 5_000_000
    model_config = SettingsConfigDict()


class IORetrySettings(BaseSettings):
    tries: int = 3
    delay: float = 1.0
    backoff: float = 1.0
    model_config = SettingsConfigDict(env_prefix="MAPCHETE_IO_RETRY_")
