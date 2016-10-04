"""Mapchete."""

import os
import py_compile

import mapchete


class Mapchete(object):
    """
    Main entry point to every processing job.

    From here, the process tiles can be determined and executed.
    """

    def __init__(self, config):
        """Initialize Mapchete job."""
        self.config = config
        self.output = config.output
        try:
            py_compile.compile(self.config.process_file, doraise=True)
        except:
            raise
        self.process_name = os.path.splitext(
            os.path.basename(self.config.process_file)
        )[0]

    def get_process_tiles(self, zoom):
        """
        Return process tiles.

        Tiles intersecting with the input data bounding boxes as well as
        process bounds, if provided, are considered process tiles. This is to
        avoid iterating through empty tiles.

        - zoom: zoom level
        """
        raise NotImplementedError

    def execute(self, process_tile, overwrite=True):
        """
        Run the Mapchete process on a given process tile.

        - process_tile: Member of the process tile pyramid (not necessarily
            the output pyramid, if output has a different metatiling setting)
        - overwrite: overwrite existing data (default: True)
        """
        raise NotImplementedError

    def view_output(self, output_tile, overwrite=True):
        """
        Run the Mapchete process and return output as a file object.

        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        - overwrite: overwrite existing data (default: True)
        """
        raise NotImplementedError

    def raw_output(self, output_tile, overwrite=True):
        """
        Run the Mapchete process and return output raw data.

        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        - overwrite: overwrite existing data (default: True)
        """
        raise NotImplementedError

    def write(self, data, process_tile, overwrite=True):
        """
        Writes data into output format.

        - data:
            for vector output: a GeoJSON-like iterable with attributes and
                geometries
            for raster output: a (masked) NumPy array or a tuple of (masked)
                NumPy arrays
        - process_tile: the respective process tile
        - overwrite: overwrite existing data (default: True)
        """
        # use self.config.output.write() function
        raise NotImplementedError

class MapcheteProcess(object):
    """
    Actual process running on a tile.

    In fact, it is a Tile.
    Its attributes and methods can be accessed via "self" from within a
    Mapchete process Python file.
    """

    def __init__(self):
        """Initialize Mapchete process."""
        raise NotImplementedError

    def open(self, input_file, **kwargs):
        """
        Return appropriate InputTile object.

        - input_file: file path or file name from configuration file
        - **kwargs: driver specific parameters (e.g. resampling)
        """
        raise NotImplementedError

    def hillshade(
        self, elevation, azimuth=315.0, altitude=45.0, z=1.0, scale=1.0
    ):
        """
        Calculate hillshading from elevation data.

        Returns an array with the same shape as the input array.
        - elevation: input array
        - azimuth: horizontal angle of light source (315: North-West)
        - altitude: vertical angle of light source (90 would result in slope
                    shading)
        - z: vertical exaggeration
        - scale: scale factor of pixel size units versus height units (insert
                 112000 when having elevation values in meters in a geodetic
                 projection)
        """
        return mapchete.commons.hillshade(
            elevation, self, azimuth, altitude, z, scale)

    def contours(
        self, elevation, interval=100, field='elev'
    ):
        """
        Extract contour lines from elevation data.

        Returns contours as GeoJSON-like pairs of properties and geometry.
        - elevation: input array
        - interval: elevation value interval
        - field: output field name containing elevation value
        """
        return mapchete.commons.contours(
            elevation, self.tile, interval=interval,
            pixelbuffer=self.pixelbuffer, field=field)

    def clip(
        self, array, geometries, inverted=False, clip_buffer=0
    ):
        """
        Return input array clipped by geometries.

        - inverted: bool, invert clipping
        - clip_buffer: int (in pixels), buffer geometries befor applying clip
        """
        return mapchete.commons.clip_array_with_vector(
            array, self.tile.affine(self.pixelbuffer), geometries,
            inverted=inverted, clip_buffer=clip_buffer*self.tile.pixel_x_size)
