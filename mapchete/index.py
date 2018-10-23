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

import concurrent.futures
from contextlib import ExitStack
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
        "col": "int"
    }
}


def zoom_index_gen(
    mp=None,
    out_dir=None,
    zoom=None,
    geojson=False,
    gpkg=False,
    shapefile=False,
    txt=False,
    fieldname=None,
    basepath=None,
    for_gdal=True,
    threading=False,
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
    """
    with ExitStack() as es:
        # get index writers for all enabled formats
        index_writers = []
        if geojson:
            index_writers.append(
                es.enter_context(
                    VectorFileWriter(
                        driver="GeoJSON",
                        out_path=_index_file_path(out_dir, zoom, "geojson"),
                        crs=mp.config.output_pyramid.crs,
                        fieldname=fieldname
                    )
                )
            )
        if gpkg:
            index_writers.append(
                es.enter_context(
                    VectorFileWriter(
                        driver="GPKG",
                        out_path=_index_file_path(out_dir, zoom, "gpkg"),
                        crs=mp.config.output_pyramid.crs,
                        fieldname=fieldname
                    )
                )
            )
        if shapefile:
            index_writers.append(
                es.enter_context(
                    VectorFileWriter(
                        driver="ESRI Shapefile",
                        out_path=_index_file_path(out_dir, zoom, "shp"),
                        crs=mp.config.output_pyramid.crs,
                        fieldname=fieldname
                    )
                )
            )
        if txt:
            index_writers.append(
                es.enter_context(
                    TextFileWriter(out_path=_index_file_path(out_dir, zoom, "txt"))
                )
            )

        logger.debug("use the following index writers: %s", index_writers)

        def _worker(tile):
            # if there are indexes to write to, check if output exists
            tile_path = _tile_path(
                orig_path=mp.config.output.get_path(tile),
                basepath=basepath,
                for_gdal=for_gdal
            )
            indexes = [
                i for i in index_writers if not i.entry_exists(tile=tile, path=tile_path)
            ]
            if indexes:
                output_exists = mp.config.output.tiles_exist(output_tile=tile)
            else:
                output_exists = None
            return tile, tile_path, indexes, output_exists

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for task in concurrent.futures.as_completed(
                (
                    executor.submit(_worker, i)
                    for i in mp.config.output_pyramid.tiles_from_geom(
                        mp.config.area_at_zoom(zoom), zoom
                    )
                )
            ):
                tile, tile_path, indexes, output_exists = task.result()
                # only write entries if there are indexes to write to and output exists
                if indexes and output_exists:
                    logger.debug("%s exists", tile_path)
                    logger.debug("write to %s indexes" % len(indexes))
                    for index in indexes:
                        index.write(tile, tile_path)
                # yield tile for progress information
                yield tile


def _index_file_path(out_dir, zoom, ext):
    return os.path.join(out_dir, str(zoom) + "." + ext)


def _tile_path(orig_path, basepath, for_gdal):
    path = (
        os.path.join(basepath, "/".join(orig_path.split("/")[-3:])) if basepath
        else orig_path
    )
    if for_gdal and path.startswith(("http://", "https://")):
        return "/vsicurl/" + path
    elif for_gdal and path.startswith("s3://"):
        return path.replace("s3://", "/vsis3/")
    else:
        return path


class VectorFileWriter():
    """Base class for GeoJSONWriter and GeoPackageWriter."""

    def __init__(
        self, out_path=None, crs=None, fieldname=None, driver=None
    ):
        self._append = "a" in fiona.supported_drivers[driver]
        logger.debug("initialize %s writer with append %s", driver, self._append)
        self.path = out_path
        self.driver = driver
        self.fieldname = fieldname
        self.new_entries = 0
        schema = deepcopy(spatial_schema)
        schema["properties"][fieldname] = "str:254"

        if self._append:
            if os.path.isfile(self.path):
                logger.debug("read existing entries")
                with fiona.open(self.path, "r") as src:
                    self._existing = {f["properties"]["tile_id"]: f for f in src}
                self.file_obj = fiona.open(self.path, "a")
            else:
                self.file_obj = fiona.open(
                    self.path, "w", driver=self.driver, crs=crs, schema=schema
                )
                self._existing = {}
        else:
            if os.path.isfile(self.path):
                logger.debug("read existing entries")
                with fiona.open(self.path, "r") as src:
                    self._existing = {f["properties"]["tile_id"]: f for f in src}
                fiona.remove(self.path, driver=driver)
            else:
                self._existing = {}
            self.file_obj = fiona.open(
                self.path, "w", driver=self.driver, crs=crs, schema=schema
            )
            self.file_obj.writerecords(self._existing.values())

    def __repr__(self):
        return "VectorFileWriter(%s)" % self.path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def write(self, tile, path):
        if not self.entry_exists(tile=tile):
            logger.debug("write %s to %s", path, self)
            self.file_obj.write({
                "geometry": mapping(tile.bbox),
                "properties": {
                    "tile_id": str(tile.id),
                    "zoom": str(tile.zoom),
                    "row": str(tile.row),
                    "col": str(tile.col),
                    self.fieldname: path
                }
            })
            self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        exists = str(tile.id) in self._existing.keys()
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
                self._existing = [l for l in src]
        else:
            self._existing = []
        self.new_entries = 0
        self.file_obj = open(self.path, "w")
        for l in self._existing:
            self.file_obj.write(l)

    def __repr__(self):
        return "TextFileWriter(%s)" % self.path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def write(self, tile, path):
        if not self.entry_exists(path=path):
            logger.debug("write %s to %s", path, self)
            self.file_obj.write(path + "\n")
            self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        exists = path + "\n" in self._existing
        logger.debug("%s exists: %s", tile, exists)
        return exists

    def close(self):
        logger.debug("%s new entries in %s", self.new_entries, self)
        self.file_obj.close()
