from datetime import datetime
import os
import pystac
from pystac.version import get_stac_version
from shapely.geometry import box, mapping

from mapchete.io.vector import reproject_geometry


OUT_PIXEL_SIZE = 0.28e-3
UNIT_TO_METER = {"mercator": 1, "geodetic": 111319.4907932732}
KNOWN_MATRIX_PROPERTIES = {
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


def create_stac_item(
    item_id=None,
    item_basepath=None,
    tile_pyramid=None,
    item_metadata=None,
    min_zoom=0,
    max_zoom=None,
    self_href=None,
    thumbnail_href=None,
    alternative_basepath=None,
    relative_paths=None,
    bands_type=None,
    thumbnail_type=None,
    unit_to_meter=1,
):
    """
    Create STAC item metadata.
    """
    if item_id is None:
        raise ValueError("item_id must be set")
    if max_zoom is None:
        raise ValueError("max_zoom must be set")
    if tile_pyramid is None:
        raise ValueError("tile_pyramid must be set")

    item_metadata = item_metadata or {}
    timestamp = (
        item_metadata.get("start_datetime")
        or item_metadata.get("end_datetime")
        or str(datetime.utcnow())
    )
    tp_grid = tile_pyramid.grid.type
    bands_schema = "{TileMatrix}/{TileRow}/{TileCol}.tif"
    bands_type = bands_type or "image/tiff; application=geotiff"
    thumbnail_href = thumbnail_href or "0/0/0.tif"
    thumbnail_type = thumbnail_type or "image/tiff; application=geotiff"
    if alternative_basepath:
        bands_schema = os.path.join(alternative_basepath, bands_schema)
        thumbnail_href = os.path.join(alternative_basepath, thumbnail_href)
    elif not relative_paths:
        if item_basepath is None:
            raise ValueError("either alternative_basepath or item_basepath must be set")
        bands_schema = os.path.join(item_basepath, bands_schema)
        thumbnail_href = os.path.join(item_basepath, thumbnail_href)

    # tiles:tile_matrix_set object:
    # http://schemas.opengis.net/tms/1.0/json/tms-schema.json
    if tp_grid in KNOWN_MATRIX_PROPERTIES:
        tile_matrix_set_identifier = KNOWN_MATRIX_PROPERTIES[tp_grid]["name"]
        crs = KNOWN_MATRIX_PROPERTIES[tp_grid]["crs"]
        tile_matrix_set = {
            "type": "TileMatrixSetType",
            "title": KNOWN_MATRIX_PROPERTIES[tp_grid]["title"],
            "identifier": tile_matrix_set_identifier,
            "supportedCRS": KNOWN_MATRIX_PROPERTIES[tp_grid]["supportedCRS"],
            "wellKnownScaleSet": KNOWN_MATRIX_PROPERTIES[tp_grid]["wellKnownScaleSet"],
        }
    else:
        tile_matrix_set_identifier = "custom"  # ???
        authority, code = tile_pyramid.crs.to_authority()
        crs = f"urn:ogc:def:crs:{authority}::{code}"
        # from http://schemas.opengis.net/tms/1.0/json/tms-schema.json
        # "required": ["type", "identifier", "supportedCRS", "tileMatrix"],
        tile_matrix_set = {
            "type": "TileMatrixSetType",
            "identifier": tile_matrix_set_identifier,
            "supportedCRS": crs,
        }
    tile_matrix_set.update(
        tileMatrix=[
            {
                "type": "TileMatrixType",
                "identifier": str(zoom),
                "scaleDenominator": _scale(
                    tp_grid, tile_pyramid.pixel_x_size(zoom), unit_to_meter
                ),
                "topLeftCorner": [tile_pyramid.bounds.left, tile_pyramid.bounds.top],
                "tileWidth": tile_pyramid.tile_width(zoom),
                "tileHeight": tile_pyramid.tile_height(zoom),
                "matrixWidth": tile_pyramid.matrix_width(zoom),
                "matrixHeight": tile_pyramid.matrix_height(zoom),
            }
            for zoom in range(min_zoom, max_zoom + 1)
        ],
        boundingBox={
            "type": "BoundingBoxType",
            "crs": crs,
            "lowerCorner": [tile_pyramid.bounds.left, tile_pyramid.bounds.bottom],
            "upperCorner": [tile_pyramid.bounds.right, tile_pyramid.bounds.top],
        },
    )

    # tiles:tile_matrix_links object:
    tile_matrix_links = {
        "url": f"#{tile_matrix_set_identifier}",
        "limits": {
            str(zoom): {
                "min_tile_col": 0,
                "max_tile_col": tile_pyramid.matrix_width(zoom) - 1,
                "min_tile_row": 0,
                "max_tile_row": tile_pyramid.matrix_height(zoom) - 1,
            }
            for zoom in range(min_zoom, max_zoom + 1)
        },
    }

    stac_extensions = ["tiled-assets"]
    if "eo:bands" in item_metadata:
        stac_extensions.append("eo")

    geometry = reproject_geometry(
        box(*tile_pyramid.bounds), src_crs=tile_pyramid.crs, dst_crs="EPSG:4326"
    )
    bbox = [*geometry.bounds]

    out = {
        "stac_version": get_stac_version(),
        "stac_extensions": stac_extensions,
        "id": item_id,
        "type": "Feature",
        "bbox": bbox,
        "geometry": mapping(geometry),
        "properties": {
            **item_metadata.get("properties", {}),
            "datetime": timestamp,
            "collection": item_id,
            "tiles:tile_matrix_links": {tile_matrix_set_identifier: tile_matrix_links},
            "tiles:tile_matrix_sets": {tile_matrix_set_identifier: tile_matrix_set},
        },
        "asset_templates": {"bands": {"href": bands_schema, "type": bands_type}},
        "assets": {
            # "thumbnail": {
            #     "href": thumbnail_href,
            #     "title": "Overview of the whole tiled dataset.",
            #     "type": thumbnail_type,
            # }
        },
    }
    if "eo:bands" in item_metadata:
        out["asset_templates"]["bands"]["eo:bands"] = item_metadata["eo:bands"]
        out["assets"]["thumbnail"]["eo:bands"] = item_metadata["eo:bands"]
    out["links"] = item_metadata.get("links", [])
    if self_href:
        out["links"].extend([{"rel": "self", "href": self_href}])

    return pystac.read_dict(out)


def _scale(grid, pixel_x_size, unit_to_meter=1):
    return UNIT_TO_METER.get(grid, unit_to_meter) * pixel_x_size / OUT_PIXEL_SIZE
