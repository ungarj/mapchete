import os
from pystac.version import get_stac_version
from shapely.geomtry import box, mapping

from mapchete.tile import BufferedTilePyramid


OUT_PIXEL_SIZE = 0.28e-3
UNIT_TO_METER = {"mercator": 1, "geodetic": 111319.4907932732}
MATRIX_PROPERTIES = {
    "geodetic": {
        "name": "WorldCRS84Quad",
        "url": "http://schemas.opengis.net/tms/1.0/json/examples/WorldCRS84Quad.json",
        "title": "CRS84 for the World",
        "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "supportedCRS": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "wellKnownScaleSet": "http://www.opengis.net/def/wkss/OGC/1.0/GoogleCRS84Quad",
    },
    "mercator": {
        "name": "WebMercatorQuad",
        "url": "http://schemas.opengis.net/tms/1.0/json/examples/WebMercatorQuad.json",
        "title": "Google Maps Compatible for the World",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/3857",
        "supportedCRS": "http://www.opengis.net/def/crs/EPSG/0/3857",
        "wellKnownScaleSet": "http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible",
    },
}


def create_stac_asset(
    asset_id,
    asset_basepath,
    tp,
    stac_metadata=None,
    min_zoom=0,
    max_zoom=None,
    out_json=None,
    alternative_basepath=None,
    relative_paths=None,
):
    """
    Create STAC asset metadata.
    """
    stac_metadata = stac_metadata or {}
    if max_zoom is None:
        raise ValueError("max_zoom must be set")

    tp_grid = tp.grid.type
    bands_schema = "{TileMatrix}/{TileRow}/{TileCol}.tif"
    thumbnail = "0/0/0.tif"
    if alternative_basepath:
        bands_schema = os.path.join(alternative_basepath, bands_schema)
        thumbnail = os.path.join(alternative_basepath, thumbnail)
    elif not relative_paths:
        bands_schema = os.path.join(asset_basepath, bands_schema)
        thumbnail = os.path.join(asset_basepath, thumbnail)

    stac_extensions = ["tiled-assets"]
    if "eo:bands" in stac_metadata:
        stac_extensions.append("eo")

    out = {
        "stac_version": get_stac_version(),
        "stac_extensions": stac_extensions,
        "id": asset_id,
        "type": "Feature",
        "bbox": [*tp.bounds],
        "geometry": mapping(box(*tp.bounds)),
        "properties": {
            **stac_metadata.get("properties", {}),
            "collection": asset_id,
            "tiles:tile_matrix_links": {
                MATRIX_PROPERTIES[tp_grid]["name"]: {
                    "url": MATRIX_PROPERTIES[tp_grid]["url"],
                    "limits": {
                        str(zoom): {
                            "min_tile_col": 0,
                            "max_tile_col": tp.matrix_width(zoom) - 1,
                            "min_tile_row": 0,
                            "max_tile_row": tp.matrix_height(zoom) - 1,
                        }
                        for zoom in range(min_zoom, max_zoom + 1)
                    },
                }
            },
            "tiles:tile_matrix_sets": {
                MATRIX_PROPERTIES[tp_grid]["name"]: {
                    "type": "TileMatrixSetType",
                    "title": MATRIX_PROPERTIES[tp_grid]["title"],
                    "identifier": MATRIX_PROPERTIES[tp_grid]["name"],
                    "boundingBox": {
                        "type": "BoundingBoxType",
                        "crs": MATRIX_PROPERTIES[tp_grid]["crs"],
                        "lowerCorner": [tp.bounds.left, tp.bounds.bottom],
                        "upperCorner": [tp.bounds.right, tp.bounds.top],
                    },
                    "supportedCRS": MATRIX_PROPERTIES[tp_grid]["supportedCRS"],
                    "wellKnownScaleSet": MATRIX_PROPERTIES[tp_grid][
                        "wellKnownScaleSet"
                    ],
                    "tileMatrix": [
                        {
                            "type": "TileMatrixType",
                            "identifier": str(zoom),
                            "scaleDenominator": _scale(tp_grid, tp.pixel_x_size(zoom)),
                            "topLeftCorner": [tp.bounds.left, tp.bounds.top],
                            "tileWidth": tp.tile_width(zoom),
                            "tileHeight": tp.tile_height(zoom),
                            "matrixWidth": tp.matrix_width(zoom),
                            "matrixHeight": tp.matrix_height(zoom),
                        }
                        for zoom in range(min_zoom, max_zoom + 1)
                    ],
                }
            },
        },
        "asset_templates": {
            "bands": {"href": bands_schema, "type": "image/tiff; application=geotiff"}
        },
        "assets": {
            "thumbnail": {
                "href": thumbnail,
                "title": "Overview of the whole tiled dataset.",
                "type": "image/tiff; application=geotiff",
            }
        },
    }
    if "eo:bands" in stac_metadata:
        out["asset_templates"]["bands"]["eo:bands"] = stac_metadata["eo:bands"]
        out["assets"]["thumbnail"]["eo:bands"] = stac_metadata["eo:bands"]
    if "links" in stac_metadata and out_json:
        out["links"] = stac_metadata["links"] + [{"rel": "self", "href": out_json}]
    return out


def _scale(grid, pixel_x_size):
    return UNIT_TO_METER[grid] * pixel_x_size / OUT_PIXEL_SIZE
