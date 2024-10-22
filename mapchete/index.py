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

import logging
import operator
import xml.etree.ElementTree as ET
from contextlib import ExitStack
from copy import deepcopy
from xml.dom import minidom

import fiona
from rasterio.dtypes import _gdal_typename
from shapely.geometry import mapping

from mapchete.config.parse import get_zoom_levels
from mapchete.io import (
    MPath,
    fiona_open,
    fs_from_path,
    path_exists,
    raster,
    relative_path,
    tiles_exist,
    vector,
)
from mapchete.path import batch_sort_property

logger = logging.getLogger(__name__)

spatial_schema = {
    "geometry": "Polygon",
    "properties": {"tile_id": "str:254", "zoom": "int", "row": "int", "col": "int"},
}


def zoom_index_gen(
    mp=None,
    out_dir=None,
    zoom=None,
    tile=None,
    geojson=False,
    gpkg=False,
    shapefile=False,
    flatgeobuf=False,
    txt=False,
    vrt=False,
    fieldname="location",
    basepath=None,
    for_gdal=True,
):
    """
    Generate indexes for given zoom level.
    """
    if tile and zoom:  # pragma: no cover
        raise ValueError("tile and zoom cannot be used at the same time")

    zoom = tile.zoom if tile else zoom
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
                            fieldname=fieldname,
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
                            fieldname=fieldname,
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
                            fieldname=fieldname,
                        )
                    )
                )
            if flatgeobuf:
                index_writers.append(
                    es.enter_context(
                        VectorFileWriter(
                            driver="FlatGeobuf",
                            out_path=_index_file_path(out_dir, zoom, "fgb"),
                            crs=mp.config.output_pyramid.crs,
                            fieldname=fieldname,
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
                            out_pyramid=mp.config.output_pyramid,
                        )
                    )
                )

            logger.debug("use the following index writers: %s", index_writers)

            if tile:
                output_tiles_batches = (
                    mp.config.output_pyramid.tiles_from_bounds_batches(
                        mp.config.process_pyramid.tile(*tile).bounds,
                        zoom,
                        batch_by=batch_sort_property(
                            mp.config.output_reader.tile_path_schema
                        ),
                    )
                )
            else:
                output_tiles_batches = mp.config.output_pyramid.tiles_from_geom_batches(
                    mp.config.area_at_zoom(zoom),
                    zoom,
                    batch_by=batch_sort_property(
                        mp.config.output_reader.tile_path_schema
                    ),
                    exact=True,
                )

            for output_tile, exists in tiles_exist(
                mp.config, output_tiles_batches=output_tiles_batches
            ):
                tile_path = _tile_path(
                    orig_path=mp.config.output.get_path(output_tile),
                    basepath=basepath,
                    for_gdal=for_gdal,
                )
                # get indexes where tile entry does not exist
                indexes = [
                    index_writer
                    for index_writer in index_writers
                    if not index_writer.entry_exists(tile=output_tile, path=tile_path)
                ]

                if indexes and exists:
                    logger.debug("%s exists", tile_path)
                    logger.debug("write to %s indexes", len(indexes))
                    for index in indexes:
                        index.write(output_tile, tile_path)

                # yield tile for progress information
                yield output_tile


def _index_file_path(out_dir, zoom, ext):
    return MPath.from_inp(out_dir) / f"{str(zoom)}.{ext}"


def _tile_path(orig_path=None, basepath=None, for_gdal=True):
    path = (
        MPath.from_inp(basepath).joinpath(*orig_path.elements[-3:])
        if basepath
        else MPath.from_inp(orig_path)
    )
    if for_gdal:
        return path.as_gdal_str()
    else:
        return str(path)


class VectorFileWriter:
    """Writes GeoJSON or GeoPackage files."""

    def __init__(self, out_path=None, crs=None, fieldname=None, driver=None):
        self.path = MPath.from_inp(out_path)
        self._append = (
            "a" in fiona.supported_drivers[driver] and not self.path.is_remote()
        )
        logger.debug("initialize %s writer with append %s", driver, self._append)
        self.driver = driver
        self.fieldname = fieldname
        self.new_entries = 0
        self.schema = deepcopy(spatial_schema)
        self.schema["properties"][fieldname] = "str:254"
        self.crs = crs

    def __repr__(self):
        return "VectorFileWriter(%s)" % self.path

    def __enter__(self):
        self.es = ExitStack().__enter__()
        if self._append:
            if self.path.exists():
                logger.debug("read existing entries")
                with fiona_open(self.path, "r") as src:
                    self._existing = {f["properties"]["tile_id"]: f for f in src}
                self.sink = self.es.enter_context(vector.fiona_write(self.path, "a"))
            else:
                self.sink = self.es.enter_context(
                    vector.fiona_write(
                        self.path,
                        "w",
                        driver=self.driver,
                        crs=self.crs.to_dict(),
                        schema=self.schema,
                    )
                )
                self._existing = {}
        else:  # pragma: no cover
            if self.path.exists():
                logger.debug("read existing entries")
                with fiona_open(self.path, "r") as src:
                    self._existing = {f["properties"]["tile_id"]: f for f in src}
                if not self.path.is_remote():
                    fiona.remove(str(self.path), driver=self.driver)
            else:
                self._existing = {}
            self.sink = self.es.enter_context(
                vector.fiona_write(
                    self.path,
                    "w",
                    driver=self.driver,
                    crs=self.crs,
                    schema=self.schema,
                )
            )
            self.sink.writerecords(self._existing.values())
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        try:
            self.es.__exit__(exc_type, exc_value, exc_traceback)
        finally:
            self.close()

    def write(self, tile, path):
        if not self.entry_exists(tile=tile):
            logger.debug("write %s to %s", path, self)
            self.sink.write(
                {
                    "geometry": mapping(tile.bbox),
                    "properties": {
                        "tile_id": str(tile.id),
                        "zoom": str(tile.zoom),
                        "row": str(tile.row),
                        "col": str(tile.col),
                        self.fieldname: path,
                    },
                }
            )
            self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        exists = str(tile.id) in self._existing.keys()
        logger.debug("%s exists: %s", tile, exists)
        return exists

    def close(self):
        logger.debug("%s new entries in %s", self.new_entries, self)
        self.sink.close()


class TextFileWriter:
    """Writes tile paths into text file."""

    def __init__(self, out_path=None):
        self.path = out_path
        logger.debug("initialize TXT writer")
        self.fs = fs_from_path(out_path)
        if path_exists(self.path):
            with self.fs.open(self.path, "r") as src:
                self._existing = {line for line in src.readlines()}
        else:
            self._existing = {}
        self.new_entries = 0
        self.sink = self.fs.open(self.path, "w")
        for line in self._existing:
            self._write_line(line)

    def __repr__(self):
        return "TextFileWriter(%s)" % self.path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _write_line(self, line):
        self.sink.write(line)

    def write(self, tile, path):
        if not self.entry_exists(path=path):
            logger.debug("write %s to %s", path, self)
            self._write_line(path + "\n")
            self.new_entries += 1

    def entry_exists(self, tile=None, path=None):
        exists = path + "\n" in self._existing
        logger.debug("tile %s with path %s exists: %s", tile, path, exists)
        return exists

    def close(self):
        logger.debug("%s new entries in %s", self.new_entries, self)
        self.sink.close()


class VRTFileWriter:
    """Generates GDAL-style VRT file."""

    def __init__(self, out_path=None, output=None, out_pyramid=None):
        # see if lxml is installed before checking all output tiles

        self.path = out_path
        self._tp = out_pyramid
        self._output = output
        self.fs = fs_from_path(out_path)
        logger.debug("initialize VRT writer for %s", self.path)
        if path_exists(self.path):
            with self.fs.open(self.path) as src:
                self._existing = {
                    k: MPath.from_inp(v) for k, v in self._xml_to_entries(src.read())
                }
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
        return self._tp.tile(
            *map(int, MPath.from_inp(path).without_suffix().elements[-3:])
        )

    def _add_entry(self, tile=None, path=None):
        self._new[tile] = MPath.from_inp(path)

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
        path = relative_path(path=path, base_dir=self.path.dirname)
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
                                (
                                    _tile_path(orig_path=path, for_gdal=True)
                                    if path.is_remote()
                                    else str(
                                        path.relative_path(start=self.path.dirname)
                                    )
                                ),
                                relativeToVRT="0" if path.is_remote() else "1",
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
                                    list(
                                        raster.bounds_to_ranges(
                                            bounds=tile.bounds,
                                            transform=vrt_affine,
                                        )
                                    )[2]
                                ),
                                yOff=str(
                                    list(
                                        raster.bounds_to_ranges(
                                            bounds=tile.bounds,
                                            transform=vrt_affine,
                                        )
                                    )[0]
                                ),
                                xSize=str(tile.shape.width),
                                ySize=str(tile.shape.height),
                            ),
                            E.NODATA(str(vrt_nodata)),
                        )
                        for tile, path in sorted(
                            all_entries.items(), key=operator.itemgetter(1)
                        )
                    ],
                    dataType=vrt_dtype,
                    band=str(b_idx),
                )
                for b_idx in range(1, self._output.profile()["count"] + 1)
            ],
            rasterXSize=str(vrt_shape.width),
            rasterYSize=str(vrt_shape.height),
        )
        # generate pretty XML and write
        xmlstr = minidom.parseString(ET.tostring(vrt)).toprettyxml(indent="  ")
        logger.debug("write to %s", self.path)
        with self.fs.open(self.path, "w") as dst:
            dst.write(xmlstr)
