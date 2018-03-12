"""
Create various index files for a process output.

Available index types:
- GeoPackage and GeoJSON index:
    Works like gdaltindex command and is useful when using process output with
    Mapserver later on.
- textfile with tiles list
    If process output is online (e.g. a public endpoint of an S3 container),
    this file can be passed on to wget to download all process output.

All index types are generated once per zoom level. For example GeoPackage will
generate GPKG files 3.gpkg, 4.gpkg and 5.gpkg for zoom levels 3, 4 and 5.

"""

from copy import deepcopy
import fiona
import logging
import os
from shapely.geometry import mapping

from mapchete.io import path_is_remote

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
    gpkg=False,
    txt=False,
    fieldname=None,
    basepath=None,
    for_gdal=True
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
    geojson : bool
        generate GeoJSON index (default: False)
    gpkg : bool
        generate GeoPackage index (default: False)
    txt : bool
        generate tile path list textfile (default: False)
    fieldname : str
        field name which contains paths of tiles (default: "location")
    basepath : str
        if set, use custom base path instead of output path
    for_gdal : bool
        use GDAL compatible remote paths, i.e. add "/vsicurl/" before path
        (default: True)
    """
    try:
        # get index writers for all enabled formats
        index_writers = []
        if geojson:
            index_writers.append(
                VectorFileWriter(
                    driver="GeoJSON",
                    out_path=_index_file_path(out_dir, zoom, "geojson"),
                    crs=mp.config.output_pyramid.crs,
                    fieldname=fieldname))
        if gpkg:
            index_writers.append(
                VectorFileWriter(
                    driver="GPKG",
                    out_path=_index_file_path(out_dir, zoom, "gpkg"),
                    crs=mp.config.output_pyramid.crs,
                    fieldname=fieldname))
        if txt:
            index_writers.append(
                TextFileWriter(
                    out_path=_index_file_path(out_dir, zoom, "txt")))

        logger.debug(index_writers)

        # iterate through output tiles
        for tile in mp.config.output_pyramid.tiles_from_geom(
            mp.config.area_at_zoom(zoom), zoom
        ):
            logger.debug("analyze tile %s", tile)
            # TODO: generate tile_path depending on basepath & for_gdal option
            tile_path = _tile_path(
                orig_path=mp.config.output.get_path(tile),
                basepath=basepath, for_gdal=for_gdal)

            # check if tile was not already inserted into all available writers
            # and write into indexes if output tile exists
            not_yet_added = [
                index for index in index_writers
                if not index.entry_exists(tile=tile, path=tile_path)]
            if (
                not_yet_added and
                mp.config.output.tiles_exist(output_tile=tile)
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


def _index_file_path(out_dir, zoom, ext):
    return os.path.join(out_dir, str(zoom) + "." + ext)


def _tile_path(orig_path, basepath, for_gdal):
    path = (
        os.path.join(basepath, "/".join(orig_path.split("/")[-3:])) if basepath
        else orig_path)
    if for_gdal and path_is_remote(path):
        return "/vsicurl/" + path
    else:
        return path


class VectorFileWriter():
    """Base class for GeoJSONWriter and GeoPackageWriter."""

    def __init__(
        self, out_path=None, crs=None, fieldname=None, driver=None
    ):
        logger.debug("initialize %s writer", driver)
        self.path = out_path
        self.driver = driver
        if os.path.isfile(self.path):
            with fiona.open(self.path) as src:
                self.existing = {f["properties"]["tile_id"]: f for f in src}
            os.remove(self.path)
        else:
            self.existing = {}
        self.new_entries = 0
        self.fieldname = fieldname
        schema = deepcopy(spatial_schema)
        schema["properties"][fieldname] = "str:254"
        self.file_obj = fiona.open(
            self.path, "w", driver=self.driver, crs=crs, schema=schema)
        self.file_obj.writerecords(self.existing.values())

    def __repr__(self):
        return "VectorFileWriter(%s)" % self.path

    def write(self, tile, path):
        logger.debug("write %s to %s", path, self)
        if self.entry_exists(tile=tile):
            return
        self.file_obj.write({
            "geometry": mapping(tile.bbox),
            "properties": {
                "tile_id": str(tile.id),
                "zoom": str(tile.zoom),
                "row": str(tile.row),
                "col": str(tile.col),
                self.fieldname: path}})
        self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        exists = str(tile.id) in self.existing.keys()
        logger.debug("%s exists: %s", tile, exists)
        return exists

    def close(self):
        logger.debug("%s new entries in %s", self.new_entries, self)
        self.file_obj.close()


class TextFileWriter():
    """Writes tile paths into text file."""
    def __init__(self, out_path=None):
        self.path = out_path
        logger.debug("initialize TXT writer")
        if os.path.isfile(self.path):
            with open(self.path) as src:
                self.existing = [l for l in src]
        else:
            self.existing = []
        self.new_entries = 0
        self.file_obj = open(self.path, "w")
        for l in self.existing:
            self.file_obj.write(l)

    def __repr__(self):
        return "TextFileWriter(%s)" % self.path

    def write(self, tile, path):
        logger.debug("write %s to %s", path, self)
        if self.entry_exists(path=path):
            return
        self.file_obj.write(path + "\n")
        self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        exists = path + "\n" in self.existing
        logger.debug("%s exists: %s", tile, exists)
        return exists

    def close(self):
        logger.debug("%s new entries in %s", self.new_entries, self)
        self.file_obj.close()
