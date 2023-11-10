from mapchete.commons import clip as commons_clip
from mapchete.commons import contours as commons_contours
from mapchete.commons import hillshade as commons_hillshade
from mapchete.validate import deprecated_kwargs


class MapcheteProcess(object):
    """
    Process class inherited by user process script.

    Its attributes and methods can be accessed via "self" from within a
    Mapchete process Python file.

    Parameters
    ----------
    tile : BufferedTile
        Tile process should be run on
    config : MapcheteConfig
        process configuration
    params : dictionary
        process parameters

    Attributes
    ----------
    identifier : string
        process identifier
    title : string
        process title
    version : string
        process version string
    abstract : string
        short text describing process purpose
    tile : BufferedTile
        Tile process should be run on
    tile_pyramid : BufferedTilePyramid
        process tile pyramid
    output_pyramid : BufferedTilePyramid
        output tile pyramid
    params : dictionary
        process parameters
    """

    def __init__(
        self, tile=None, params=None, input=None, output_params=None, config=None
    ):
        """Initialize Mapchete process."""
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""

        self.tile = tile
        self.tile_pyramid = tile.tile_pyramid
        if config is not None:
            input = config.get_inputs_for_tile(tile)
            params = config.params_at_zoom(tile.zoom)
        self.params = dict(params, input=input)
        self.input = input
        self.output_params = output_params

    def write(self, data, **kwargs):
        """Deprecated."""
        raise DeprecationWarning(
            "Please return process output data instead of using self.write()."
        )

    def read(self, **kwargs):
        """
        Read existing output data from a previous run.

        Returns
        -------
        process output : NumPy array (raster) or feature iterator (vector)
        """
        raise DeprecationWarning(
            "Read existing output from within a process is deprecated"
        )

    @deprecated_kwargs
    def open(self, input_id, **kwargs):
        """
        Open input data.

        Parameters
        ----------
        input_id : string
            input identifier from configuration file or file path
        kwargs : driver specific parameters (e.g. resampling)

        Returns
        -------
        tiled input data : InputTile
            reprojected input data within tile
        """
        if input_id not in self.input:
            raise ValueError("%s not found in config as input" % input_id)
        return self.input[input_id]

    def hillshade(self, elevation, azimuth=315.0, altitude=45.0, z=1.0, scale=1.0):
        """
        Calculate hillshading from elevation data.

        Parameters
        ----------
        elevation : array
            input elevation data
        azimuth : float
            horizontal angle of light source (315: North-West)
        altitude : float
            vertical angle of light source (90 would result in slope shading)
        z : float
            vertical exaggeration factor
        scale : float
            scale factor of pixel size units versus height units (insert 112000
            when having elevation values in meters in a geodetic projection)

        Returns
        -------
        hillshade : array
        """
        return commons_hillshade.hillshade(
            elevation, self.tile, azimuth, altitude, z, scale
        )

    def contours(self, elevation, interval=100, field="elev", base=0):
        """
        Extract contour lines from elevation data.

        Parameters
        ----------
        elevation : array
            input elevation data
        interval : integer
            elevation value interval when drawing contour lines
        field : string
            output field name containing elevation value
        base : integer
            elevation base value the intervals are computed from

        Returns
        -------
        contours : iterable
            contours as GeoJSON-like pairs of properties and geometry
        """
        return commons_contours.extract_contours(
            elevation, self.tile, interval=interval, field=field, base=base
        )

    def clip(self, array, geometries, inverted=False, clip_buffer=0):
        """
        Clip array by geometry.

        Parameters
        ----------
        array : array
            raster data to be clipped
        geometries : iterable
            geometries used to clip source array
        inverted : bool
            invert clipping (default: False)
        clip_buffer : int
            buffer (in pixels) geometries before applying clip

        Returns
        -------
        clipped array : array
        """
        return commons_clip.clip_array_with_vector(
            array,
            self.tile.affine,
            geometries,
            inverted=inverted,
            clip_buffer=clip_buffer * self.tile.pixel_x_size,
        )
