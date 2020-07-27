"""
Create various index files for a process output.

Available index types:

- GeoPackage and GeoJSON index:
  Works like gdaltindex command and is useful when using process output with
  Mapserver later on.
- textfile with tiles list
  If process output is online (e.g. a public endpoint of an S3 container),
  this file can be passed on to wget to download all process output.
- VRT
  Virtual raster dataset format by GDAL. This enables GIS tools to read multiple
  files at once, e.g. QGIS can open a zoom VRT so the user doesn't have to open
  all GeoTIFF files from a certain zoom level.

All index types are generated once per zoom level. For example GeoPackage will
generate GPKG files 3.gpkg, 4.gpkg and 5.gpkg for zoom levels 3, 4 and 5.

"""

from contextlib import ExitStack
from copy import deepcopy
import fiona
import logging
import operator
import os
from rasterio.dtypes import _gdal_typename
from shapely.geometry import mapping
import xml.etree.ElementTree as ET
from xml.dom import minidom

from mapchete.config import get_zoom_levels
from mapchete.io import (
    path_exists, path_is_remote, get_boto3_bucket, raster, relative_path, tiles_exist
)

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
    vrt=False,
    fieldname="location",
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
    vrt : bool
        GDAL-style VRT file (default: False)
    fieldname : str
        field name which contains paths of tiles (default: "location")
    basepath : str
        if set, use custom base path instead of output path
    for_gdal : bool
        use GDAL compatible remote paths, i.e. add "/vsicurl/" before path
        (default: True)
    """
    for zoom in get_zoom_levels(process_zoom_levels=zoom):
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
            if vrt:
                index_writers.append(
                    es.enter_context(
                        VRTFileWriter(
                            out_path=_index_file_path(out_dir, zoom, "vrt"),
                            output=mp.config.output,
                            out_pyramid=mp.config.output_pyramid
                        )
                    )
                )

            logger.debug("use the following index writers: %s", index_writers)

            # all output tiles for given process area
            logger.debug("determine affected output tiles")
            output_tiles = set(
                mp.config.output_pyramid.tiles_from_geom(
                    mp.config.area_at_zoom(zoom), zoom
                )
            )

            # check which tiles exist in any index
            logger.debug("check which tiles exist in index(es)")
            existing_in_any_index = set(
                tile for tile in output_tiles
                if any(
                    [
                        i.entry_exists(
                            tile=tile,
                            path=_tile_path(
                                orig_path=mp.config.output.get_path(tile),
                                basepath=basepath,
                                for_gdal=for_gdal
                            )
                        )
                        for i in index_writers
                    ]
                )
            )

            logger.debug("{}/{} tiles found in index(es)".format(
                len(existing_in_any_index), len(output_tiles))
            )
            # tiles which do not exist in any index
            for tile, output_exists in tiles_exist(
                mp.config, output_tiles=output_tiles.difference(existing_in_any_index)
            ):
                tile_path = _tile_path(
                    orig_path=mp.config.output.get_path(tile),
                    basepath=basepath,
                    for_gdal=for_gdal
                )
                indexes = [
                    i for i in index_writers
                    if not i.entry_exists(tile=tile, path=tile_path)
                ]
                if indexes and output_exists:
                    logger.debug("%s exists", tile_path)
                    logger.debug("write to %s indexes" % len(indexes))
                    for index in indexes:
                        index.write(tile, tile_path)
                # yield tile for progress information
                yield tile

            # tiles which exist in at least one index
            for tile in existing_in_any_index:
                tile_path = _tile_path(
                    orig_path=mp.config.output.get_path(tile),
                    basepath=basepath,
                    for_gdal=for_gdal
                )
                indexes = [
                    i for i in index_writers
                    if not i.entry_exists(tile=tile, path=tile_path)
                ]
                if indexes:
                    logger.debug("%s exists", tile_path)
                    logger.debug("write to %s indexes" % len(indexes))
                    for index in indexes:
                        index.write(tile, tile_path)
                # yield tile for progress information
                yield tile


def _index_file_path(out_dir, zoom, ext):
    return os.path.join(out_dir, str(zoom) + "." + ext)


def _tile_path(orig_path=None, basepath=None, for_gdal=True):
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
    """Writes GeoJSON or GeoPackage files."""

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
                self.sink = fiona.open(self.path, "a")
            else:
                self.sink = fiona.open(
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
            self.sink = fiona.open(
                self.path, "w", driver=self.driver, crs=crs, schema=schema
            )
            self.sink.writerecords(self._existing.values())

    def __repr__(self):
        return "VectorFileWriter(%s)" % self.path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def write(self, tile, path):
        if not self.entry_exists(tile=tile):
            logger.debug("write %s to %s", path, self)
            self.sink.write({
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
        self.sink.close()


class TextFileWriter():
    """Writes tile paths into text file."""
    def __init__(self, out_path=None):
        self.path = out_path
        self._bucket = self.path.split("/")[2] if self.path.startswith("s3://") else None
        self.bucket_resource = get_boto3_bucket(self._bucket) if self._bucket else None
        logger.debug("initialize TXT writer")
        if path_exists(self.path):
            if self._bucket:
                key = "/".join(self.path.split("/")[3:])
                for obj in self.bucket_resource.objects.filter(Prefix=key):
                    if obj.key == key:
                        self._existing = {
                            l + '\n'
                            for l in obj.get()['Body'].read().decode().split('\n')
                            if l
                        }
            else:
                with open(self.path) as src:
                    self._existing = {l for l in src}
        else:
            self._existing = {}
        self.new_entries = 0
        if self._bucket:
            self.sink = ""
        else:
            self.sink = open(self.path, "w")
        for l in self._existing:
            self._write_line(l)

    def __repr__(self):
        return "TextFileWriter(%s)" % self.path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _write_line(self, line):
        if self._bucket:
            self.sink += line
        else:
            self.sink.write(line)

    def write(self, tile, path):
        if not self.entry_exists(path=path):
            logger.debug("write %s to %s", path, self)
            self._write_line(path + '\n')
            self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        exists = path + "\n" in self._existing
        logger.debug("tile %s with path %s exists: %s", tile, path, exists)
        return exists

    def close(self):
        logger.debug("%s new entries in %s", self.new_entries, self)
        if self._bucket:
            key = "/".join(self.path.split("/")[3:])
            logger.debug("upload %s", key)
            self.bucket_resource.put_object(Key=key, Body=self.sink)
        else:
            self.sink.close()


class VRTFileWriter():
    """Generates GDAL-style VRT file."""
    def __init__(self, out_path=None, output=None, out_pyramid=None):
        # see if lxml is installed before checking all output tiles
        from lxml.builder import ElementMaker
        self.path = out_path
        self._tp = out_pyramid
        self._output = output
        self._bucket = self.path.split("/")[2] if self.path.startswith("s3://") else None
        self.bucket_resource = get_boto3_bucket(self._bucket) if self._bucket else None
        logger.debug("initialize VRT writer for %s", self.path)
        if path_exists(self.path):
            if self._bucket:
                key = "/".join(self.path.split("/")[3:])
                for obj in self.bucket_resource.objects.filter(Prefix=key):
                    if obj.key == key:
                        self._existing = {
                            k: v for k, v in self._xml_to_entries(
                                obj.get()['Body'].read().decode()
                            )
                        }
            else:
                with open(self.path) as src:
                    self._existing = {k: v for k, v in self._xml_to_entries(src.read())}
        else:
            self._existing = {}
        logger.debug("%s existing entries", len(self._existing))
        self.new_entries = 0
        self._new = {}

    def __repr__(self):
        return "VRTFileWriter(%s)" % self.path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _path_to_tile(self, path):
        return self._tp.tile(*map(int, os.path.splitext(path)[0].split("/")[-3:]))

    def _add_entry(self, tile=None, path=None):
        self._new[tile] = path

    def _xml_to_entries(self, xml_string):
        for entry in next(
            ET.ElementTree(ET.fromstring(xml_string)).getroot().iter("VRTRasterBand")
        ).iter("ComplexSource"):
            path = next(entry.iter("SourceFilename")).text
            yield (self._path_to_tile(path), path)

    def write(self, tile, path):
        if not self.entry_exists(tile=tile, path=path):
            logger.debug("write %s to %s", path, self)
            self._add_entry(tile=tile, path=path)
            self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        path = relative_path(path=path, base_dir=os.path.split(self.path)[0])
        exists = path in self._existing
        logger.debug("tile %s with path %s exists: %s", tile, path, exists)
        return exists

    def close(self):
        from lxml.builder import ElementMaker

        logger.debug("%s new entries in %s", self.new_entries, self)
        if not self._new:
            logger.debug("no entries to write")
            return

        # combine existing and new entries
        all_entries = {**self._existing, **self._new}
        logger.debug("writing a total of %s entries", len(all_entries))

        # get VRT attributes
        vrt_affine, vrt_shape = raster.tiles_to_affine_shape(list(all_entries.keys()))
        vrt_dtype = _gdal_typename(self._output.profile()["dtype"])
        vrt_nodata = self._output.output_params["nodata"]

        # build XML
        E = ElementMaker()
        vrt = E.VRTDataset(
            E.SRS(self._tp.crs.wkt),
            E.GeoTransform(", ".join(map(str, vrt_affine.to_gdal()))),
            *[
                E.VRTRasterBand(
                    E.NoDataValue(str(vrt_nodata)),
                    E.ColorInterp("Gray"),
                    *[
                        E.ComplexSource(
                            E.SourceFilename(
                                _tile_path(orig_path=path, for_gdal=True) if
                                path_is_remote(path) else
                                relative_path(
                                    path=path, base_dir=os.path.split(self.path)[0]
                                ),
                                relativeToVRT="0" if path_is_remote(path) else "1"
                            ),
                            E.SourceBand(str(b_idx)),
                            E.SourceProperties(
                                RasterXSize=str(tile.shape.width),
                                RasterYSize=str(tile.shape.height),
                                DataType=vrt_dtype,
                                BlockXSize=str(
                                    self._output.profile().get(
                                        "blockxsize", self._tp.tile_size
                                    )
                                ),
                                BlockYSize=str(
                                    self._output.profile().get(
                                        "blockysize", self._tp.tile_size
                                    )
                                ),
                            ),
                            E.SrcRect(
                                xOff="0",
                                yOff="0",
                                xSize=str(tile.shape.width),
                                ySize=str(tile.shape.height),
                            ),
                            E.DstRect(
                                xOff=str(
                                    list(raster.bounds_to_ranges(
                                        out_bounds=tile.bounds,
                                        in_affine=vrt_affine,
                                        in_shape=vrt_shape
                                    ))[2]
                                ),
                                yOff=str(
                                    list(raster.bounds_to_ranges(
                                        out_bounds=tile.bounds,
                                        in_affine=vrt_affine,
                                        in_shape=vrt_shape
                                    ))[0]
                                ),
                                xSize=str(tile.shape.width),
                                ySize=str(tile.shape.height),
                            ),
                            E.NODATA(str(vrt_nodata))
                        )
                        for tile, path in sorted(
                            all_entries.items(), key=operator.itemgetter(1)
                        )
                    ],
                    dataType=vrt_dtype,
                    band=str(b_idx)
                )
                for b_idx in range(1, self._output.profile()["count"] + 1)
            ],
            rasterXSize=str(vrt_shape.width),
            rasterYSize=str(vrt_shape.height),
        )
        # generate pretty XML and write
        xmlstr = minidom.parseString(ET.tostring(vrt)).toprettyxml(indent="  ")
        if self._bucket:
            key = "/".join(self.path.split("/")[3:])
            logger.debug("upload %s", key)
            self.bucket_resource.put_object(Key=key, Body=xmlstr)
        else:
            logger.debug("write to %s", self.path)
            with open(self.path, "w") as dst:
                dst.write(xmlstr)
