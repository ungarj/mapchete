from mapchete.formats.base._base import InputTile
from mapchete.formats.base.raster import RasterInputDriver, RasterOutputDriver
from mapchete.path import MPath


class SingleFileInput(InputTile):
    path: MPath


class SingleFileInputData(RasterInputDriver):
    path: MPath

    def __init__(self, params: dict, **kwargs):
        super().__init__(params, **kwargs)


class SingleFileOutputReader(RasterOutputDriver):
    path: MPath

    def __init__(self, params, readonly=False):
        """Initialize."""
        super().__init__(params, readonly=readonly)
        self.path = MPath(params["path"])


class SingleFileOutputWriter(SingleFileOutputReader):
    pass
