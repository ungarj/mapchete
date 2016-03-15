# #!/usr/bin/env python
#
# from shapely.geometry import Polygon
# from functools import partial
# from multiprocessing import Pool
#
# from tilematrix import TilePyramid, get_best_zoom_level, file_bbox
#
# from .io_utils import read_raster, read_pyramid, write_raster
#
# class MapchetePyramid(object):
#     """
#     Configures and seeds a tile pyramid.
#     """
#     def __init__(
#         self,
#         input_raster,
#         pyramid_type,
#         output,
#         resampling="nearest",
#         zoom=None,
#         bounds=None,
#         overwrite=False
#         ):
#         self.input_raster = input_raster
#         self.pyramid_type = pyramid_type
#         self.tile_pyramid = TilePyramid(pyramid_type)
#         self.tile_pyramid.set_format("GTiff")
#         self.tile_pyramid.format.profile.update(count=3)
#         self.output_name = output
#         self.resampling = resampling
#         self.minzoom, self.maxzoom = self._get_zoom(zoom)
#         self.bbox = self._get_bbox(bounds)
#         # bit hacky.
#         self.config = type('temp', (object,), {})()
#         self.config.output_name = output
#
#
#     def seed(self, zoom=None):
#         """
#         Seeds pyramid in current configuration.
#         """
#         if zoom:
#             if isinstance(zoom, int) and zoom >= 0:
#                 zoom_levels = [zoom]
#             else:
#                 raise ValueError("invalid zoom level %s" % zoom)
#         else:
#             zoom_levels = reversed(range(self.minzoom, self.maxzoom+1))
#         logs = []
#         for zoom in zoom_levels:
#             work_tiles = self.tile_pyramid.tiles_from_geom(self.bbox, zoom)
#             for tile in work_tiles:
#                 self._worker(tile)
#             f = partial(self._worker, tile_pyramid=self.tile_pyramid)
#             pool = Pool()
#             try:
#                 output = pool.map_async(f, work_tiles)
#                 pool.close()
#             except KeyboardInterrupt:
#                 pool.terminate()
#                 sys.exit()
#             except:
#                 raise
#             finally:
#                 pool.close()
#                 pool.join()
#
#
#     def _worker(self, tile):
#         """
#         Worker processing one tile.
#         """
#         self.tile = tile
#         zoom, row, col = tile
#         if zoom == self.maxzoom:
#             # print self.tile_pyramid.tile_bbox(*tile)
#             print "raster"
#             metadata, data = read_raster(
#                 self,
#                 self.input_raster,
#                 bands=self.tile_pyramid.format.profile["count"],
#                 pixelbuffer=0,
#                 resampling=self.resampling,
#             )
#         else:
#             print "pyramid"
#             metadata, data = read_pyramid(
#                 self.tile,
#                 self.output_name,
#                 self.tile_pyramid,
#                 src_zoom=zoom+1,
#                 dst_pixelbuffer=0,
#                 resampling=self.resampling
#             )
#
#         print metadata
#         write_raster(
#             self,
#             metadata,
#             data,
#             pixelbuffer=0
#         )
#
#
#     def _get_zoom(self, zoom):
#         """
#         Determines minimum and maximum zoomlevel.
#         """
#         if not zoom:
#             minzoom = 0
#             maxzoom = get_best_zoom_level(self.input_raster, self.pyramid_type)
#         elif len(zoom) == 1:
#             minzoom = zoom[0]
#             maxzoom = zoom[0]
#         elif len(zoom) == 2:
#             if zoom[0] < zoom[1]:
#                 minzoom = zoom[0]
#                 maxzoom = zoom[1]
#             else:
#                 minzoom = zoom[1]
#                 maxzoom = zoom[0]
#         else:
#             raise ValueError("invalid number of zoom levels provided")
#         return minzoom, maxzoom
#
#
#     def _get_bbox(self, bounds):
#         """
#         Determine processing bounding box by intersecting file bounding box with
#         user defined bounds (if available).
#         """
#         files_area = file_bbox(self.input_raster, self.tile_pyramid)
#         if bounds:
#             assert len(bounds) == 4
#             left, bottom, right, top = bounds
#             ul = left, top
#             ur = right, top
#             lr = right, bottom
#             ll = left, bottom
#             user_bbox = Polygon([ul, ur, lr, ll])
#             out_area = files_area.intersection(user_bbox)
#         else:
#             out_area = files_area
#         return out_area
