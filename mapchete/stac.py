from collections import OrderedDict
import datetime
import os
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


def tile_directory_stac_item(
    item_id=None,
    tile_pyramid=None,
    max_zoom=None,
    item_path=None,
    asset_basepath=None,
    relative_paths=False,
    min_zoom=0,
    item_metadata=None,
    bounds=None,
    bounds_crs=None,
    bands_type="image/tiff; application=geotiff",
    crs_unit_to_meter=1,
):
    """
    Create STAC item from a Tile Directory using the tiled-assets.

    Currently only works for one asset.

    Parameters
    ----------
    item_id : str
        Unique item ID. (required)
    tile_pyramid : mapchete.tile.BufferedTilePyramid
        Tile pyramid of tiled asset. (required)
    max_zoom : int
        Maximum zoom of tile directory. (required)
    item_path : str
        Path of stac item.
    asset_basepath : str
        Base path of asset.
    relative_paths : bool
        Interpret asset path schema as being relative to item. If not set, one of item_path
        or asset_basepath has to be set. (default: False)
    min_zoom : int
        Minimum zoom of tile directory. (default: 0)
    item_metadata : dict
        Optional additional item metadata to be appended.
    bounds : list or tuple
        Tile directory bounds. (default: tile_pyramid.bounds)
    bounds_crs : CRS
        CRS of bounds. (default: tile_pyramid.crs)
    bands_type : str
        Media typ of tile directory tiles. (default: "image/tiff; application=geotiff")
    crs_unit_to_meter : int or float
        Factor to convert CRS units to meter if tile pyramid grid is not "geodetic" or "mercator". (default: 1)

    Returns
    -------
    pystac.Item
    """
    try:
        import pystac
        from pystac.version import get_stac_version
    except ImportError:  # pragma: no cover
        raise ImportError(
            "dependencies for extra mapchete[stac] is required for this feature"
        )

    if item_id is None:
        raise ValueError("item_id must be set")
    if max_zoom is None:
        raise ValueError("max_zoom must be set")
    if tile_pyramid is None:
        raise ValueError("tile_pyramid must be set")

    item_metadata = _cleanup_datetime(item_metadata or {})
    timestamp = (
        item_metadata.get("properties", {}).get("start_datetime")
        or item_metadata.get("properties", {}).get("end_datetime")
        or str(datetime.datetime.utcnow())
    )
    tp_grid = tile_pyramid.grid.type
    bands_schema = "{TileMatrix}/{TileRow}/{TileCol}.tif"
    # thumbnail_href = thumbnail_href or "0/0/0.tif"
    # thumbnail_type = thumbnail_type or "image/tiff; application=geotiff"
    if asset_basepath:
        bands_schema = os.path.join(asset_basepath, bands_schema)
        # thumbnail_href = os.path.join(asset_basepath, thumbnail_href)
    elif not relative_paths:
        if item_path is None:
            raise ValueError("either alternative_basepath or item_path must be set")
        bands_schema = os.path.join(os.path.dirname(item_path), bands_schema)
        # thumbnail_href = os.path.join(os.path.dirname(item_path), thumbnail_href)

    # use bounds provided or fall back to tile pyramid bounds
    bounds = bounds or tile_pyramid.bounds
    bounds_crs = bounds_crs or tile_pyramid.crs

    # bounds in tilepyramid CRS
    left, bottom, right, top = reproject_geometry(
        box(*bounds), src_crs=bounds_crs, dst_crs=tile_pyramid.crs
    ).bounds

    # bounds in lat/lon
    geometry_4326 = reproject_geometry(
        box(*bounds), src_crs=bounds_crs, dst_crs="EPSG:4326"
    )
    bounds_4326 = [*geometry_4326.bounds]

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
                    tp_grid, tile_pyramid.pixel_x_size(zoom), crs_unit_to_meter
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
                "min_tile_col": tile_pyramid.tile_from_xy(
                    left, top, zoom, on_edge_use="rb"
                ).col,
                "max_tile_col": tile_pyramid.tile_from_xy(
                    right, bottom, zoom, on_edge_use="lt"
                ).col,
                "min_tile_row": tile_pyramid.tile_from_xy(
                    left, top, zoom, on_edge_use="rb"
                ).row,
                "max_tile_row": tile_pyramid.tile_from_xy(
                    right, bottom, zoom, on_edge_use="lt"
                ).row,
            }
            for zoom in range(min_zoom, max_zoom + 1)
        },
    }

    stac_extensions = ["tiled-assets"]
    if "eo:bands" in item_metadata:
        stac_extensions.append("eo")

    out = {
        "stac_version": get_stac_version(),
        "stac_extensions": stac_extensions,
        "id": item_id,
        "type": "Feature",
        "bbox": bounds_4326,
        "geometry": mapping(geometry_4326),
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
        # out["assets"]["thumbnail"]["eo:bands"] = item_metadata["eo:bands"]
    out["links"] = item_metadata.get("links", [])
    if item_path:
        out["links"].extend([{"rel": "self", "href": item_path}])

    return pystac.read_dict(out)


def _scale(grid, pixel_x_size, unit_to_meter=1):
    return UNIT_TO_METER.get(grid, unit_to_meter) * pixel_x_size / OUT_PIXEL_SIZE


def _cleanup_datetime(d):
    """Convert datetime objects in dictionary to strings."""
    return OrderedDict(
        (k, _cleanup_datetime(v))
        if isinstance(v, dict)
        else (k, str(v))
        if isinstance(v, datetime.date)
        else (k, v)
        for k, v in d.items()
    )
