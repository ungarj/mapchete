import logging
from typing import List, Optional

from mapchete.formats.tile_directory import TileDirectory
from mapchete.io import MatchingMethod
from mapchete.io.vector import read_vector_window
from mapchete.tile import BufferedTile

logger = logging.getLogger(__name__)


class VectorTileDirectory:
    def open(self, tile: BufferedTile, **kwargs):
        return VectorTileDirectoryInput(
            tile,
            basepath=self.path,
            file_extension=self._ext,
            profile=self._profile,
            td_pyramid=self.td_pyramid,
            read_as_tiledir_func=self._read_as_tiledir_func,
            min_zoom=self._min_zoom,
            max_zoom=self._max_zoom,
            resampling=self._resampling,
            **kwargs,
        )


class VectorTileDirectoryInput:
    def read(
        self,
        tile_directory_zoom: Optional[int] = None,
        matching_method: MatchingMethod = MatchingMethod.gdal,
        matching_max_zoom: Optional[int] = None,
        matching_precision: int = 8,
        fallback_to_higher_zoom: bool = False,
        validity_check: bool = False,
        **kwargs,
    ) -> List[dict]:
        logger.debug("reading data from CRS %s to CRS %s", self.crs, self.tile.tp.crs)
        tiles_paths = self._get_tiles_paths(
            tile_directory_zoom=tile_directory_zoom,
            fallback_to_higher_zoom=fallback_to_higher_zoom,
            matching_method=matching_method,
            matching_precision=matching_precision,
            matching_max_zoom=matching_max_zoom,
        )
        if tiles_paths:
            return read_vector_window(
                [path for _, path in tiles_paths],
                self.tile,
                validity_check=validity_check,
                skip_missing_files=True,
            )
        else:  # pragma: no cover
            return []
