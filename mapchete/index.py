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
import operator
import os
from shapely.geometry import mapping
import xml.etree.ElementTree as ET
from xml.dom import minidom

from mapchete.io import path_exists, get_boto3_bucket, raster, relative_path

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
                            l + '\n'
                            for l in obj.get()['Body'].read().decode().split('\n')
                            if l
                        }
            else:
                with open(self.path) as src:
                    self._existing = {i for i in self._xml_to_entries(src.read())}
        else:
            self._existing = {}
        self.new_entries = 0
        self.sink = {}
        for l in self._existing:
            self._add_entry(path=l)

    def __repr__(self):
        return "VRTFileWriter(%s)" % self.path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _add_entry(self, tile=None, path=None):
        if tile is None:
            tile = self._tp.tile(*map(int, os.path.splitext(path)[0].split("/")[-3:]))
        self.sink[tile] = path

    def _xml_to_entries(self, xml_string):
        vrt_dataset = ET.ElementTree(ET.fromstring(xml_string)).getroot()
        vrt_rasterband = next(vrt_dataset.iter("VRTRasterBand"))
        for entry in vrt_rasterband.iter("ComplexSource"):
            yield next(entry.iter("SourceFilename")).text

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
        logger.debug("%s new entries in %s", self.new_entries, self)
        if not self.sink:
            logger.debug("no entries to write")
            return

        # get VRT Affine and shape
        vrt_affine, vrt_shape = raster.tiles_to_affine_shape(
            list(self.sink.keys()), clip_to_pyramid_bounds=False
        )
        vrt_crs = self._tp.crs.to_string()
        vrt_geotransform = vrt_affine.to_gdal()
        vrt_dtype = self._output.profile()["dtype"]
        vrt_nodata = self._output.nodata
        vrt_blockxsize = self._output.profile().get("blockxsize", self._tp.tile_size)
        vrt_blockysize = self._output.profile().get("blockysize", self._tp.tile_size)

        # VRT metadata
        root = ET.Element(
            "VRTDataset",
            attrib={
                "rasterXSize": str(vrt_shape.width),
                "rasterYSize": str(vrt_shape.height)
            }
        )
        srs = ET.SubElement(root, "SRS")
        srs.text = vrt_crs
        geotransform = ET.SubElement(root, "GeoTransform")
        geotransform.text = ", ".join(map(str, vrt_geotransform))

        # iterate through bands
        for b_idx in range(1, self._output.profile()["count"] + 1):
            band = ET.SubElement(
                root,
                "VRTRasterBand",
                attrib={"dataType": vrt_dtype, "band": str(b_idx)}
            )
            nodatavalue = ET.SubElement(band, "NoDataValue")
            nodatavalue.text = str(vrt_nodata)
            color = ET.SubElement(band, "ColorInterp")
            color.text = "Gray"
            # iterate through tiles for each band
            vrt_maxrow = 0
            vrt_maxcol = 0
            for tile, path in sorted(self.sink.items(), key=operator.itemgetter(1)):
                complexsource = ET.SubElement(band, "ComplexSource")
                source_filename = ET.SubElement(
                    complexsource,
                    "SourceFilename",
                    attrib={"relativeToVRT": "1"}
                )
                source_filename.text = relative_path(
                    path=path, base_dir=os.path.split(self.path)[0]
                )
                source_band = ET.SubElement(complexsource, "SourceBand")
                source_band.text = str(b_idx)
                ET.SubElement(
                    complexsource,
                    "SourceProperties",
                    attrib={
                        "RasterXSize": str(tile.shape.width),
                        "RasterYSize": str(tile.shape.height),
                        "DataType": vrt_dtype,
                        "BlockXSize": str(vrt_blockxsize),
                        "BlockYSize": str(vrt_blockysize),
                    }
                )
                # source data rectangle
                ET.SubElement(
                    complexsource,
                    "SrcRect",
                    attrib={
                        "xOff": str(0),
                        "yOff": str(0),
                        "xSize": str(tile.shape.width),
                        "ySize": str(tile.shape.height),
                    }
                )
                # target data window within VRT
                minrow, _, mincol, _ = raster.bounds_to_ranges(
                    out_bounds=tile.bounds,
                    in_affine=vrt_affine,
                    in_shape=vrt_shape
                )
                ET.SubElement(
                    complexsource,
                    "DstRect",
                    attrib={
                        "xOff": str(mincol),
                        "yOff": str(minrow),
                        "xSize": str(tile.shape.width),
                        "ySize": str(tile.shape.height),
                    }
                )
                vrt_maxrow = max([vrt_maxrow, minrow + tile.shape.height])
                vrt_maxcol = max([vrt_maxcol, mincol + tile.shape.width])
                source_nodatavalue = ET.SubElement(complexsource, "NODATA")
                source_nodatavalue.text = str(vrt_nodata)
        # generate pretty XML and write
        xmlstr = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
        if self._bucket:
            key = "/".join(self.path.split("/")[3:])
            logger.debug("upload %s", key)
            self.bucket_resource.put_object(Key=key, Body=xmlstr)
        else:
            logger.debug("write to %s", self.path)
            with open(self.path, "w") as dst:
                dst.write(xmlstr)
