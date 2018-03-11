"""
Create various index files for a process output.

Available index types:
- VRT (Virtual Raster Dataset)
    A .vrt file can be loaded into QGIS
- Shapeindex:
    Works like gdaltindex command and is useful when using process output with
    Mapserver later on.
- GeoJSON index:
    Is the same as Shapeindex, just in GeoJSON format.
- textfile with tiles list
    If process output is online (e.g. a public endpoint of an S3 container),
    this file can be passed on to wget to download all process output.

All index types are generated once per zoom level. For example VRT will
generate VRT files 3.vrt, 4.vrt and 5.vrt for zoom levels 3, 4 and 5.

"""

from copy import deepcopy
import fiona
import logging
import os
from shapely.geometry import mapping

logger = logging.getLogger(__name__)

spatial_schema = {
    "geometry": "Polygon",
    "properties": {
        "tile_id": "str:254",
        "zoom": "int",
        "row": "int",
        "col": "int"}}


def zoom_index_gen(
    mp=None,
    out_dir=None,
    zoom=None,
    geojson=False,
    shapefile=False,
    txt=False,
    vrt=False,
    fieldname=None,
    basepath=None,
    for_gdal=True,
    overwrite=False
):
    """
    Generate indexes for given zoom level.

    Parameters
    ----------
    mp : Mapchete object
        process output to be indexed
    out_dir : path
        optionally override process output directory
    zoom : int
        zoom level to be processed
    vrt : bool
        generate VRT file (default: False)
    geojson : bool
        generate GeoJSON index (default: False)
    shapefile : bool
        generate Shapefile index (default: False)
    txt : bool
        generate tile path list textfile (default: False)
    fieldname : str
        field name which contains paths of tiles (default: "location")
    basepath : str
        if set, use custom base path instead of output path
    for_gdal : bool
        use GDAL compatible remote paths, i.e. add "/vsicurl/" before path
        (default: True)
    overwrite : bool
        don't check if tile index already exists (default False)

    """
    if not any([geojson, shapefile, vrt]):
        raise ValueError(
            "one of 'geojson', 'shapefile' or 'vrt' must be provided")
    if vrt:
        raise NotImplementedError("writing VRTs is not yet enabled")

    try:
        # get index writers for all enabled formats
        index_writers = []
        if geojson:
            index_writers.append(
                GeoJSONWriter(
                    basepath=_get_index_path(out_dir, zoom, "geojson"),
                    overwrite=overwrite,
                    crs=mp.config.output_pyramid.crs,
                    fieldname=fieldname))
        logger.debug(index_writers)

        # iterate through output tiles
        for tile in mp.config.output_pyramid.tiles_from_geom(
            mp.config.area_at_zoom(zoom), zoom
        ):
            logger.debug("analyze tile %s", tile)
            # TODO: generate tile_path depending on basepath & for_gdal option
            tile_path = mp.config.output.get_path(tile)

            # in overwrite mode, simply check whether output tile exists
            # and pass on to writers
            if overwrite and mp.config.output.tiles_exist(output_tile=tile):
                for index in index_writers:
                    index.write(tile, tile_path)

            # not in overwrite, first check whether entry already exists
            # in index files and only call output.tiles_exist() if
            # necessary
            else:
                not_yet_added = [
                    index for index in index_writers
                    if not index.entry_exists(tile)]
                if not_yet_added and mp.config.output.tiles_exist(
                    output_tile=tile
                ):
                    for index in not_yet_added:
                        index.write(tile, tile_path)

            yield tile

    finally:
        for writer in index_writers:
            logger.debug("close %s", writer)
            try:
                writer.close()
            except Exception as e:
                logger.error(
                    "writer %s could not be closed: %s", e, str(writer))


def _get_index_path(out_dir, zoom, ext):
    return os.path.join(out_dir, str(zoom) + "." + ext)


class GeoJSONWriter():
    """Writer for GeoJSON index file."""
    def __init__(
        self, basepath=None, overwrite=False, crs=None, fieldname=None,
    ):
        logger.debug("initialize GeoJSON writer")
        self.path = basepath
        if os.path.isfile(self.path):
            with fiona.open(self.path) as src:
                self.existing = list(src)
            os.remove(self.path)
        else:
            self.existing = []
        self.new_entries = 0
        self.fieldname = fieldname
        schema = deepcopy(spatial_schema)
        schema["properties"][fieldname] = "str:254"
        self.file_obj = fiona.open(
            self.path,
            "w",
            driver="GeoJSON",
            crs=crs,
            schema=schema)

    def write(self, tile, path):
        logger.debug("write %s to %s", path, self)
        self.file_obj.write(
            {
                "geometry": mapping(tile.bbox),
                "properties": {
                    "tile_id": str(tile.id),
                    "zoom": str(tile.zoom),
                    "row": str(tile.row),
                    "col": str(tile.col),
                    self.fieldname: path}})
        self.new_entries += 1

    def entry_exists(self, tile):
        exists = len(
            list(filter(
                lambda f: f['properties']['tile_id'] == str(tile.id),
                self.existing
            ))) > 0
        logger.debug("%s exists: %s", tile, exists)
        return exists

    def close(self):
        logger.debug("%s new entries in %s", self.new_entries, self)
        self.file_obj.close()

    def __repr__(self):
        return "GeoJSONWriter(%s)" % self.path
