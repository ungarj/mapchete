#!/usr/bin/env python
"""
Mapchete output format class
"""

from copy import deepcopy

FORMATS = {
    "GTiff": {
        "data_type": "raster",
        "extension": ".tif",
        "driver": "GTiff",
        "profile": {
            'blockysize': 256,
            'blockxsize': 256,
            'tiled': True,
            'dtype': 'uint8',
            'compress': 'lzw',
            'interleave': 'band',
            'nodata': 0
        }
    },
    "PNG": {
        "data_type": "raster",
        "extension": ".png",
        "driver": "PNG",
        "profile": {
            'dtype': 'uint8',
            'nodata': None,
            'driver': 'PNG',
            'count': 3
        }
    },
    "PNG_hillshade": {
        "data_type": "raster",
        "extension": ".png",
        "driver": "PNG",
        "profile": {
            'dtype': 'uint8',
            'nodata': 0,
            'driver': 'PNG',
            'count': 4
        }
    },
    "GeoJSON": {
        "data_type": "vector",
        "extension": ".geojson",
        "driver": "GeoJSON",
        "profile": None
    },
    "PostGIS": {
        "data_type": "vector",
        "extension": None,
        "driver": "postgis",
        "profile": None
    },
    "GeoPackage": {
        "data_type": None,
        "extension": ".gpkg",
        "driver": "gpkg",
        "profile": {
            "compress": "lz4",
            "nodata": None
        }
    },
    "NumPy": {
        "data_type": "raster",
        "extension": ".numpy",
        "driver": "numpy",
        "profile": {
            "compress": "lz4",
            'dtype': 'uint8',
            "nodata": None
        }
    }
}

class MapcheteOutputFormat(object):
    """
    Main output format class which is used for Mapchete to determine how to
    write process results.
    """

    def __init__(self, output_dict):
        self._verify_params(output_dict)
        self.type = output_dict["type"]
        self.format = output_dict["format"]

        if self.format == "GeoPackage":
            raise NotImplementedError("GeoPackage is not yet supported")
        else:
            self.data_type = FORMATS[self.format]["data_type"]

        if self.format == "PostGIS":
            self.path = None
            self.db_params = output_dict["db_params"]
            self.is_db = True
            self.is_file = False
        else:
            self.path = output_dict["path"]
            self.db_params = None
            self.is_db = False
            self.is_file = True

        self.profile = deepcopy(FORMATS[self.format]["profile"])

        if self.data_type == "vector":
            self.schema = output_dict["schema"]
            self.dtype = None
            self.bands = None
            self.nodataval = None
            self.profile = None
            self.compression = None
            self.binary_type = None
        elif self.data_type == "raster":
            self.schema = None
            self.bands = output_dict["bands"]
            self.profile.update(count=self.bands)
            if self.format == "GTiff":
                for param in ["compress", "predictor"]:
                    try:
                        self.profile[param] = output_dict[param]
                    except KeyError:
                        pass
            try:
                self.dtype = output_dict["dtype"]
                self.profile["dtype"] = output_dict["dtype"]
            except KeyError:
                self.dtype = FORMATS[self.format]["profile"]["dtype"]
            try:
                self.nodataval = output_dict["nodata"]
            except KeyError:
                self.nodataval = None
            self.profile.update(nodata=self.nodataval)
            if self.format == "NumPy":
                self.compression = "lz4"
            else:
                self.compression = "lzw"
            try:
                self.compression = output_dict["compression"]
                self.profile.update(compression=self.compression)
            except KeyError:
                pass
            if self.format == "GeoPackage":
                try:
                    self.binary_type = output_dict["binary_type"]
                except KeyError:
                    self.binary_type = None

        self.extension = FORMATS[self.format]["extension"]
        self.driver = FORMATS[self.format]["driver"]

        if self.type == "geodetic":
            self.crs = {'init': (u'epsg:4326')}
            self.srid = 4326
        elif self.type == "mercator":
            self.crs = {'init': (u'epsg:3857')}
            self.srid = 3857


    def _verify_params(self, p):
        """
        Asserts all parameters are set correctly.
        """
        try:
            assert isinstance(p, dict)
        except:
            raise ValueError("no valid output format description given")

        try:
            assert p["type"] in ["geodetic", "mercator"]
        except:
            raise ValueError("tiling schema missing ('geodetic' or 'mercator')")

        try:
            assert p["format"] in FORMATS
        except:
            raise ValueError("output format parameter missing")

        if p["format"] == "GeoPackage":
            try:
                assert p["data_type"] in ["vector", "raster"]
            except:
                raise ValueError(
                    "output data type ('vector' or 'raster') required"
                )

        if p["format"] == "PostGIS":
            try:
                assert p["db_params"]
            except:
                raise ValueError("db_params required")
            con = p["db_params"]
            try:
                assert con["host"]
            except:
                raise ValueError("host required")
            try:
                assert con["port"]
            except:
                raise ValueError("port required")
            try:
                assert con["db"]
            except:
                raise ValueError("db name required")
            try:
                assert con["user"]
            except:
                raise ValueError("user required")
            try:
                assert con["password"]
            except:
                raise ValueError("password required")
            try:
                assert con["table"]
            except:
                raise ValueError("table required")
        else:
            try:
                assert p["path"]
            except:
                raise ValueError("path required")

        if FORMATS[p["format"]]["data_type"] == "vector":
            try:
                assert p["schema"]
                assert isinstance(p["schema"], dict)
            except:
                raise ValueError("output schema required")
        elif FORMATS[p["format"]]["data_type"] == "raster":
            try:
                assert p["bands"]
            except:
                raise ValueError("bands required")

            if p["format"] == "GeoPackage":
                try:
                    assert p["binary_type"] in ["numpy"]
                except:
                    raise ValueError("binary type (numpy) required")
