from mapchete import MapcheteProcess


class Process(MapcheteProcess):
    """Main process class."""

    def __init__(self, **kwargs):
        """Process initialization."""
        # init process
        MapcheteProcess.__init__(self, **kwargs)
        self.identifier = "my_process_id",
        self.title = "My long process title",
        self.version = "0.1",
        self.abstract = "short description on what my process does"

    def execute(self):
        # Reading and writing data works like this:
        with self.open("file1", resampling="bilinear") as raster_file:
            if raster_file.is_empty():
                return "empty"
                # This assures a transparent tile instead of a pink error tile
                # is returned when using mapchete serve.
            dem = raster_file.read()
        return dem
