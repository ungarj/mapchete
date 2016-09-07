#!/usr/bin/env python
"""
Main core handling Mapchete Hosts and Mapchete Processes.
"""

import py_compile
import os
import imp
import traceback
from flask import send_file
from PIL import Image
import io
import rasterio
import numpy as np
import numpy.ma as ma
from tempfile import NamedTemporaryFile
import logging
import logging.config
from sqlalchemy import (create_engine, MetaData, Column, Integer, String, Float,
    Table)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from geoalchemy2 import Geometry
import warnings

from tilematrix import TilePyramid, MetaTilePyramid, Tile
from mapchete.io_utils import (RasterFileTile, RasterProcessTile,
    VectorFileTile, VectorProcessTile, NumpyTile, read_raster_window,
    write_raster, write_vector)
from mapchete.commons import hillshade, extract_contours, clip_array_with_vector

LOGGER = logging.getLogger("mapchete")
SQL_DATATYPES = {
    "str": String,
    "int": Integer,
    "float": Float
}

class Mapchete(object):
    """
    Class handling MapcheteProcesses and MapcheteConfigs. Main acces point to
    get, retrieve MapcheteTiles or seed entire pyramids.
    """
    def __repr__(self):
        return "<objec 'Mapchete'>"

    def __str__(self):
        return 'Mapchete: %s' % self.config.mapchete_file

    def __init__(
        self,
        config
        ):
        """
        Initialize with a .mapchete file and optional zoom & bound parameters.
        """
        try:
            self.config = config
            self.output = self.config.output
            base_tile_pyramid = TilePyramid(self.output.type)
            self.tile_pyramid = MetaTilePyramid(
                base_tile_pyramid,
                self.config.metatiling
            )
        except:
            raise
        try:
            py_compile.compile(self.config.process_file, doraise=True)
        except:
            raise
        self.process_name = os.path.splitext(
            os.path.basename(self.config.process_file)
        )[0]
        if self.output.is_db:
            self._init_db_tables()

        if self.output.format == "GeoPackage":
            self._init_gpkg()

    def tile(self, tile):
        """
        Takes a Tile object and adds process specific metadata.
        """
        return MapcheteTile(self, tile)

    def get_work_tiles(self, zoom=None):
        """
        Determines the tiles affected by zoom levels, bounding box and input
        data.
        Returns (yields) valid Tile objects.
        """
        try:
            if zoom or zoom == 0:
                bbox = self.config.process_area(zoom)
                for tile in self.tile_pyramid.tiles_from_geom(bbox, zoom):
                    yield self.tile(tile)
            else:
                for zoom in reversed(self.config.zoom_levels):
                    bbox = self.config.process_area(zoom)
                    for tile in self.tile_pyramid.tiles_from_geom(bbox, zoom):
                        yield self.tile(tile)
        except Exception as e:
            LOGGER.error("error getting work tiles: %s" % traceback.print_exc())
            raise StopIteration()

    def execute(self, tile, overwrite=True):
        """
        Processes and saves tile.
        Returns a tuple of three values:
        - Tile ID: zoom, row, col
        - status: "exists", "empty", "failed" or "processed"
        - error message: optional or None
        """
        # Do nothing if tile exists or overwrite is turned off.
        if not overwrite and tile.exists():
            return tile.id, "exists", None
        try:
            new_process = imp.load_source(
                self.process_name + "Process",
                self.config.process_file
            )
            tile_process = new_process.Process(
                config=self.config,
                tile=tile,
                params=self.config.at_zoom(tile.zoom)
            )
        except:
            return tile.id, "failed", traceback.print_exc()

        # Generate tile using the user defined process.
        if not self.config.baselevel or (
            tile.zoom == self.config.baselevel["zoom"]
            ):
            try:
                result = tile_process.execute()
            except:
                return tile.id, "failed", traceback.print_exc()
            finally:
                tile_process = None

            message = None
            if result:
                if result == "empty":
                    status = "empty"
                else:
                    status = "custom"
                    message = result
            else:
                status = "processed"
            return tile.id, status, message

        # If baselevel interpolation is activated, generate from neighbor zooms.
        else:
            if tile.zoom < self.config.baselevel["zoom"]:
                # determine tiles from next zoom level
                # TODO create option for on demand processing if subtile is not
                # available.

                # create temporary VRT and create new tile from resampled
                # subtiles.
                children_paths = [
                    child.path
                    for child in tile.get_children()
                    if child.exists()
                ]
                if len(children_paths) == 0:
                    return tile.id, "empty", None
                temp_vrt = NamedTemporaryFile()
                build_vrt = "gdalbuildvrt %s %s > /dev/null" %(
                    temp_vrt.name,
                    ' '.join(children_paths)
                    )
                try:
                    os.system(build_vrt)
                    assert os.path.isfile(temp_vrt.name)
                except:
                    build_vrt = "gdalbuildvrt %s %s" %(
                        temp_vrt.name,
                        ' '.join(children_paths)
                        )
                    os.system(build_vrt)
                    return tile.id, "failed", "GDAL VRT building"
                try:
                    assert os.path.isfile(temp_vrt.name)
                    bands = tuple(read_raster_window(
                        temp_vrt.name,
                        tile,
                        resampling=self.config.baselevel["resampling"]
                    ))
                except:
                    return tile.id, "failed", traceback.print_exc()
                try:
                    write_raster(
                        tile_process,
                        self.output.profile,
                        bands
                    )
                    return tile.id, "processed", None
                except:
                    return tile.id, "failed", traceback.print_exc()

            elif tile.zoom > self.config.baselevel["zoom"]:
                # determine tiles from previous zoom level
                # check if tiles exist and if not, execute subtiles
                parent = tile.get_parent()
                if not overwrite and parent.exists():
                    # LOGGER.info((tile.id, "exists", None))
                    pass
                else:
                    pass
                if not parent.exists():
                    # TODO create option for on demand processing
                    return tile.id, "empty", "source tile does not exist"
                try:
                    bands = tuple(read_raster_window(
                        parent.path,
                        tile,
                        resampling=self.config.baselevel["resampling"]
                    ))
                    write_raster(
                        tile_process,
                        self.output.profile,
                        bands
                    )
                    return tile.id, "processed", None
                except:
                    return tile.id, "failed", traceback.print_exc()

    def get(self, tile, overwrite=False):
        """
        Returns tile as file object (image, for mapchete_serve). Also processes
        tile if necessary.
        """
        # return empty image if nothing to do at zoom level
        if tile.zoom not in self.config.zoom_levels:
            return self._empty_image()

        # return/process tile or crop/process metatile
        if self.config.metatiling > 1:
            metatile = MapcheteTile(
                self,
                self.tile_pyramid.tile(
                    tile.zoom,
                    int(tile.row/self.config.metatiling),
                    int(tile.col/self.config.metatiling),
                    )
                )

            # if overwrite is on or metatile doesn't exist, generate
            if overwrite or not metatile.exists():
                try:
                    messages = self.execute(metatile)
                except:
                    LOGGER.error(messages)
                    raise
                LOGGER.info(messages)
                # return empty image if process messaged empty
                if messages[1] == "empty":
                    return self._empty_image()
                if messages[1] == "failed":
                    LOGGER.error(messages)
                    raise IOError(messages)
                else:
                    return send_file(
                        self._cropped_metatile(metatile, tile),
                        mimetype='image/png'
                    )
            else:
                # return cropped image
                try:
                    LOGGER.info(
                        (metatile.id, tile.id, "return cropped metatile")
                    )
                    return send_file(
                        self._cropped_metatile(metatile, tile),
                        mimetype='image/png'
                    )
                except Exception as e:
                    LOGGER.error(tile.id, "failed", e)
                    raise

        # return/process tile with no metatiling
        else:
            tile = MapcheteTile(self, tile)
            # if overwrite is on or metatile doesn't exist, generate
            if overwrite or not tile.exists():
                try:
                    messages = self.execute(tile)
                except:
                    raise
                # return empty image if process messaged empty
                if messages[1] == "empty":
                    return self._empty_image()
            return send_file(tile.path, mimetype='image/png')

    def read(self, tile, indexes=None, pixelbuffer=0, resampling="nearest"):
        """
        Reads source tile to numpy array.
        """
        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.output.profile["count"]+1)

        return tile.read(
            indexes=band_indexes,
            pixelbuffer=pixelbuffer,
            resampling=resampling
            )

    def _empty_image(self):
        """
        Creates transparent PNG
        """
        size = self.tile_pyramid.tilepyramid.tile_size
        empty_image = Image.new('RGBA', (size, size))
        return empty_image.tobytes()

    def _cropped_metatile(self, metatile, tile):
        """
        Crops metatile to tile.
        """
        metatiling = self.tile_pyramid.metatiles
        # calculate pixel boundary
        left = (tile.col % metatiling) * tile.width
        right = left + tile.width
        top = (tile.row % metatiling) * tile.height
        bottom = top + tile.height
        # open buffer image and crop metatile
        img = Image.open(metatile.path)
        cropped = img.crop((left, top, right, bottom))
        out_img = io.BytesIO()
        cropped.save(out_img, 'PNG')
        out_img.seek(0)
        return out_img

    def _init_db_tables(self):
        """
        Initializes target tables in database.
        """
        db_url = 'postgresql://%s:%s@%s:%s/%s' %(
            self.output.db_params["user"],
            self.output.db_params["password"],
            self.output.db_params["host"],
            self.output.db_params["port"],
            self.output.db_params["db"]
        )
        engine = create_engine(db_url)
        for zoom in self.config.zoom_levels:
            config = self.config.at_zoom(zoom)
            geom_type = config["output"].schema["geometry"]
            table = config["output"].db_params["table"]
            schema = config["output"].schema["properties"]

            column_types = {
                column: SQL_DATATYPES[schema[column]]
                for column in schema
                }

            metadata = MetaData(bind=engine)
            Table(
                table,
                metadata,
                Column('id', Integer, primary_key=True),
                Column('zoom', Integer, index=True),
                Column('row', Integer, index=True),
                Column('col', Integer, index=True),
                Column('geom', Geometry(
                    geom_type,
                    srid=self.tile_pyramid.srid
                    )
                ),
                *(
                    Column(column, dtype)
                    for column, dtype in column_types.iteritems()
                    )
                )
            metadata.create_all()
        engine.dispose()

    def _init_gpkg(self):
        """
        Creates .gpkg file and initializes tables.
        """
        pass


class MapcheteTile(Tile):
    """
    Defines a tile object which stores common tile parameters (see
    tilematrix.tile) as well as Mapchete functions such as get(), read() or
    execute().
    """
    def __init__(
        self,
        mapchete,
        tile,
        pixelbuffer=0,
        resampling="nearest"
        ):
        self.tile_pyramid = mapchete.tile_pyramid
        Tile.__init__(self, self.tile_pyramid, tile.zoom, tile.row, tile.col)
        self.process = mapchete
        self.config = mapchete.config.at_zoom(tile.zoom)
        self.output = mapchete.output
        self.nodata = self.output.nodataval
        self.indexes = self.output.bands
        self.dtype = self.output.dtype
        self.path = self._get_path()

    def get_parent(self):
        """
        Returns MapcheteTile from previous zoomlevel.
        """
        if self.zoom == 0:
            return None
        else:
            return MapcheteTile(
                self.process,
                self.tile_pyramid.tile(
                    self.zoom-1,
                    int(self.row/2),
                    int(self.col/2),
                    )
                )

    def get_children(self):
        """
        Returns MapcheteTiles from next zoomlevel.
        """
        return [
            MapcheteTile(self.process, tile)
            for tile in self.tile_pyramid.tile(*self.id).get_children()
            ]

    def profile(self, pixelbuffer=0):
        """
        Returns a pixelbuffer specific metadata set for a raster tile.
        """
        out_meta = self.output.profile
        # create geotransform
        px_size = self.tile_pyramid.pixel_x_size(self.zoom)
        left = self.bounds(pixelbuffer=pixelbuffer)[0]
        top = self.bounds(pixelbuffer=pixelbuffer)[3]
        tile_geotransform = (left, px_size, 0.0, top, 0.0, -px_size)
        out_meta.update(
            width=self.width,
            height=self.height,
            transform=tile_geotransform,
            affine=self.affine(pixelbuffer=pixelbuffer)
        )
        return out_meta

    def exists(self):
        """
        Returns True if file exists or False if not.
        """
        if self.output.is_db:
            # config = self.process.config.at_zoom(self.zoom)
            table = self.config["output"].db_params["table"]
            # connect to db
            db_url = 'postgresql://%s:%s@%s:%s/%s' %(
                self.config["output"].db_params["user"],
                self.config["output"].db_params["password"],
                self.config["output"].db_params["host"],
                self.config["output"].db_params["port"],
                self.config["output"].db_params["db"]
            )
            engine = create_engine(db_url, poolclass=NullPool)
            meta = MetaData()
            meta.reflect(bind=engine)
            TargetTable = Table(
                table,
                meta,
                autoload=True,
                autoload_with=engine
            )
            Session = sessionmaker(bind=engine)
            session = Session()
            result = session.query(TargetTable).filter_by(
                zoom=self.zoom,
                row=self.row,
                col=self.col
            ).first()
            session.close()
            engine.dispose()
            if result:
                return True
            else:
                return False

        else:
            return os.path.isfile(self.path)

    def read(self, indexes=None, pixelbuffer=0, resampling="nearest"):
        """
        Reads input as numpy array. Is the equivalent function of
        read_raster_window in tilematrix.io.
        """
        # TODO fix: function does not always seem to return the correct number
        # of bands.
        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        if self.is_empty(pixelbuffer=pixelbuffer):
            out_empty = ()
            for index in band_indexes:
                yield self._empty_band(pixelbuffer=pixelbuffer)

        if pixelbuffer > 0:
            pass
            # determine tiles.
            # check if tiles exist
            # if not, run execute
            # read per band intersecting tiles
            # mosaick tiles
        else:
            # read bare files without buffer
            log_message = self.process.execute(self, overwrite=False)
            with rasterio.open(self.path, "r") as src:
                nodataval = src.nodata
                # Quick fix because None nodata is not allowed.
                if not nodataval:
                    nodataval = 0
                for index in band_indexes:
                    out_band = src.read(index, masked=True)
                    out_band = ma.masked_equal(out_band, nodataval)
                    yield out_band

    def _empty_band(self, pixelbuffer=0):
        """
        Creates empty, masked array.
        """
        shape = (
            self.profile(pixelbuffer)["width"],
            self.profile(pixelbuffer)["height"]
        )
        zeros = np.zeros(
            shape=(shape),
            dtype=self.output.profile["dtype"]
        )
        return ma.masked_array(
            zeros,
            mask=True
        )

    def is_empty(self, pixelbuffer=0):
        """
        Returns true if tile bounding box does not intersect with process area.
        Note: This is just a quick test. It could happen, that a tile can be
        empty anyway but this cannot be known at this stage.
        """
        process_area = self.config.process_area(self.zoom)
        if self.bbox(pixelbuffer=pixelbuffer).intersects(process_area):
            return False
        else:
            return True

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        # TODO cleanup
        pass

    def _get_path(self):
        """
        Returns full tile path.
        """
        if self.output.is_file:
            zoomdir = os.path.join(self.output.path, str(self.zoom))
            rowdir = os.path.join(zoomdir, str(self.row))
            tile_name = os.path.join(
                rowdir,
                str(self.col) + self.output.extension
            )
            return tile_name
        else:
            return None

    def prepare_paths(self):
        """
        If format is file based, this function shall create the desired
        directories.
        If it is a GeoPackage, it shall initialize the SQLite file.
        """
        if self.output.is_file:
            zoomdir = os.path.join(self.output.path, str(self.zoom))
            if os.path.exists(zoomdir):
                pass
            else:
                try:
                    os.makedirs(zoomdir)
                except:
                    pass
            rowdir = os.path.join(zoomdir, str(self.row))
            if os.path.exists(rowdir):
                pass
            else:
                try:
                    os.makedirs(rowdir)
                except:
                    pass
        else:
            pass


class MapcheteProcess():
    """
    Main process class. Visible as "self" from within a user process file.
    Includes functions reading and writing data.
    """

    def __init__(
        self,
        tile,
        config=None,
        params=None
        ):
        """
        Process initialization.
        """
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.tile = tile
        self.tile_pyramid = tile.tile_pyramid
        self.params = params
        self.config = config
        self.output = self.tile.output
        try:
            self.pixelbuffer = params["pixelbuffer"]
        except KeyError:
            self.pixelbuffer = 0

    def open(
        self,
        input_file,
        pixelbuffer=0,
        resampling="nearest"
        ):
        """
        Returns either a fiona vector dictionary, a RasterFileTile or a
        MapcheteTile object.
        """
        if pixelbuffer:
            warnings.warn(
                "pixelbuffer keyword in self.open() will be removed"
                )
        if self.pixelbuffer:
            pixelbuffer = self.pixelbuffer
        if isinstance(input_file, dict):
            raise ValueError("input cannot be dict")
        # TODO add proper check for input type.
        if isinstance(input_file, str):
            extension = os.path.splitext(input_file)[1][1:]
            if extension in ["shp", "geojson"]:
                return VectorFileTile(
                    input_file,
                    self.tile,
                    pixelbuffer=pixelbuffer
                )
            else:
                # if zoom < baselevel, iterate per zoom level until baselevel
                # and resample tiles using their 4 parents.
                return RasterFileTile(
                    input_file,
                    self.tile,
                    pixelbuffer=pixelbuffer,
                    resampling=resampling
                )
        else:
            # if input is a Mapchete process
            if input_file.output.data_type == "vector":
                return VectorProcessTile(
                    input_file,
                    self.tile,
                    pixelbuffer=pixelbuffer
                )
            elif input_file.output.data_type == "raster":
                if input_file.output.format == "NumPy":
                    return NumpyTile(
                        input_file,
                        self.tile,
                        pixelbuffer=pixelbuffer,
                        resampling=resampling
                    )
                return RasterProcessTile(
                    input_file,
                    self.tile,
                    pixelbuffer=pixelbuffer,
                    resampling=resampling
                )

    def write(
        self,
        data,
        pixelbuffer=0
        ):
        if self.output.data_type == "vector":
            write_vector(
                self,
                self.tile.config,
                data,
                pixelbuffer=pixelbuffer,
                overwrite=self.config.overwrite
            )
        elif self.output.data_type == "raster":
            write_raster(
                self,
                self.tile.profile(pixelbuffer=pixelbuffer),
                data,
                pixelbuffer=pixelbuffer
            )

    def hillshade(
        self,
        elevation,
        azimuth=315.0,
        altitude=45.0,
        z=1.0,
        scale=1.0
        ):
        """
        Calculates hillshading from elevation data. Returns an array with the
        same shape as the input array.
        - elevation: input array
        - azimuth: horizontal angle of light source (315: North-West)
        - altitude: vertical angle of light source (90 would result in slope
                    shading)
        - z: vertical exaggeration
        - scale: scale factor of pixel size units versus height units (insert
                 112000 when having elevation values in meters in a geodetic
                 projection)
        """
        return hillshade(elevation, self, azimuth, altitude, z, scale)

    def contours(
        self,
        elevation,
        interval=100,
        field='elev'
        ):
        """
        Extracts contour lines from elevation data. Returns contours as GeoJSON-
        like pairs of properties and geometry.
        - elevation: input array
        - interval: elevation value interval
        - field: output field name containing elevation value
        """
        return extract_contours(
            elevation,
            self.tile,
            interval=interval,
            pixelbuffer=self.pixelbuffer,
            field=field
            )

    def clip(
        self,
        array,
        geometries,
        inverted=False,
        clip_buffer=0
        ):
        """
        Returns input array clipped by geometries.
        - inverted: bool, invert clipping
        - clip_buffer: int (in pixels), buffer geometries befor applying clip
        """
        return clip_array_with_vector(
            array,
            self.tile.affine(self.pixelbuffer),
            geometries,
            inverted=inverted,
            clip_buffer=clip_buffer*self.tile.pixel_x_size
            )
