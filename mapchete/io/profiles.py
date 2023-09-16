from rasterio.profiles import Profile


class COGDeflateProfile(Profile):
    """Standard COG profile."""

    defaults = {
        "driver": "COG",
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "compress": "deflate",
        "nodata": 0,
    }


class GTiffDeflateProfile(Profile):
    """Tiled, band-interleaved, DEFLATE-compressed, 8-bit GTiff."""

    defaults = {
        "driver": "GTiff",
        "tiled": True,
        "blockysize": 512,
        "blockxsize": 512,
        "compress": "deflate",
        "predictor": 2,
        "interleave": "band",
        "nodata": 0,
    }


DEFAULT_PROFILES = {"COG": COGDeflateProfile, "GTiff": GTiffDeflateProfile}
