Mapchete(mapchete_file, zoom=None, bounds=None)
* MapcheteConfig(mapchete_file, zoom=None, bounds=None)
  * config_path (to .mapchete file)
  * process_path (to .py file)
  * zoom_levels (list of provided zoom levels)
  * process_bounds
  * process_area(zoom) (iterator of process area at zoom)
  * input_files(zoom)
  * at_zoom(zoom) (compiles params for MapcheteProcess)
  * is_valid_at_zoom()
* MapcheteProcess
  * execute()
  * identifier
  * version
  * abstract
  * tile
  * MetaTilePyramid
  * params (zoom-dependent parameters)
  * config (link to MapcheteConfig)
* subprocesses
  * list(Mapchete, ...)
MapcheteTile:
  * inherits from tilematrix.Tile
  * path (if not gpkg)
  * exists
  * format
  * nodata
  * process --> MapcheteProcess
  * get(overwrite=True)
    --> returns file
  * read(overwrite=True)
    --> returns data
  * execute(overwrite=True)
    --> applies process
* read()
* get_work_tiles()
<!-- * get(tile, overwrite=True)
* execute(tile, overwrite=True)
* exists(tile) -->
* seed()

MapchetePyramid(input_file, pyramid_type, zoom=None, bounds=None, resampling="nearest")
* seed()
