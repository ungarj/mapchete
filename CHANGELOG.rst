#########
Changelog
#########


---------------------
2023.9.0 - 2023-09-05
---------------------

* packaging

  * limit dependent versions to `"aiobotocore>=1.1.2,<=2.5.4"` and `"s3fs<2023.9.0"`

* core

  * make sure opened/written files are removed upon exception (#576)
  * CLI: apply tiled-assets hack also to create-item CLI (#577)
  * provide path schema to configure how tile paths are created (#581)
  * `IndexedFeatures`: allow reprojection of object bounds to a target CRS (#585)


---------------------
2023.8.1 - 2023-08-09
---------------------

* packaging

  * require `Shapely>=2.0.0` (#572)
  * remmove `cached_property` package requirement (#573)
  * add `isort` to pre-commit (#573)

* core

  * fix `ReferencedRaster.to_file()` on 2D arrays (#574)


---------------------
2023.8.0 - 2023-08-09
---------------------

* packaging

  * add `pydantic<2.0.0` as dependency


* CI

  * also test on Python 3.11 (#562)

* core

  * enable adding default read parameters to TileDirectory input (#565)
  * configuration schema (#564)

    * add `pydantic<2.0.0` as dependency
    * `mapchete.config.ProcessConfig` now defines the mapchete process configuration schema
    * process function parameters should now go into the `process_parameters` section of the configuration
    * add `mapchete.config.ProcessFunc` abstraction class to load and handle user process functions

  * CLI: fix passing on storage options; add storage options to convert command (#568)
  * update STACTA file schema to STAC 1.0.0 (#569)
  * added `ReferencedRaster.to_file()` (#570)
  * added `read_raster(tile=...)` kwarg to resample incoming raster (#570)


---------------------
2023.7.1 - 2023-07-18
---------------------

* core

  * CLI: add promts to ease using mapchete create (#558)
  * clip source tile to pyramid bounds before reprojecting to avoid bumping into antimeridian error (#561)


---------------------
2023.7.0 - 2023-07-04
---------------------

* core

  * better handle dask CancelledErrors (#555) 
  * fix copy() of larger files (#552) 
  * fix STACTA read & add tests (#551)

* packaging

  * pin pystac version to 1.7.3 as it does not yet implement STAC version… 


---------------------
2023.6.5 - 2023-06-14
---------------------

* core

  * pre-calculate effective area analog to effective bounds (#550)


---------------------
2023.6.4 - 2023-06-13
---------------------

* core

  * `MPath`: don't store session objects; fix allowed extensions string (#549)

* testing

  * add pickling tests (#549)


---------------------
2023.6.3 - 2023-06-12
---------------------

* core

  * allow `ReferencedRaster` to accept arrays with more dimensions than 3 (#548)
  * `MPath.from_inp()`: allow parsing objects using `__fspath__` interface (#547)


---------------------
2023.6.2 - 2023-06-12
---------------------

* core

  * don't let MPath.makedirs() decide whether to only create parent directories or not (#546)

* testing

  * ProcessFixture now processes all preprocessing tasks using SequentialExecutor (#546)


---------------------
2023.6.1 - 2023-06-06
---------------------

* core

  * only try to generate endpoint URL for rio/fio Sessions if a custom endpoint URL was provided in the first place (#541)

* testing

  * add tests for AWS S3 raster file (#541)
  * better catch pytest fixture errors if docker-compose is not running or AWS credentials are not set (#541)


---------------------
2023.6.0 - 2023-06-05
---------------------

* core

  * allow providing values in mapchete configuration from environmental variables (e.g. `key: ${SOME_ENV_VAR}`) (#511)
  * enable setting individual storage options for `fsspec` and other I/O modules (`rasterio`, `fiona`, ...) for each input and output (#511)
  * introduce `mapchete.path` module including `MPath` class based on `os.PathLike` (#511)
  * use `MPath` for all internal path representations (#511)
  * introduce `mapchete.io.settings` module (#511)

* allow range requests on serve (#539)

* packaging

  * run isort on imports (#538)

* testing

  * require and run `docker-compose` before tests to provide S3 and HTTP endpoints (#511)


---------------------
2023.4.1 - 2023-04-20
---------------------

* packaging

  * add all `s3` extra dependencies to `complete`


---------------------
2023.4.0 - 2023-04-20
---------------------

* core

  * `to_shape()`: enable handling full feature dicts in `__geo_interface__` (#531)
  * add `object_geometry()` method, which works like `object_bounds()` (#531)
  * add `types` module containing `Bounds` and `ZoomLevel` classes (#532)
  * remove `validate_zoom()`
  * `mapchete.io._misc.get_boto3_bucket()`: function is now deprecated
  * `mapchete.io._geometry_operations.reproject_geometry`: account for new fiona transform_geom behavior
  * replace remaining `boto3` bucket calls with `fsspec`
  * `mapchete.io.raster`: use same logic to extract `FileNotFoundError` for `read_raster_window` and `read_raster_no_crs`; replace deprecated `IOError` with `OSError`

* packaging

  * remove direct `s3fs` dependency but keep `boto3` and `aiobotocore` as direct dependencies for `s3` extra

* testing

  * fix test case to reflect bug which prevents dask from updating overview tile using the task graph (#530)


---------------------
2023.1.1 - 2023-01-26
---------------------

* core

  * use threaded concurrency on default when calling `mapchete cp` (#526)
  * removing check whether preprocessing task result is already set to avoid random KeyErrors (#529)

* testing

  * add test cases for continue mode (#527)
  * add test to eplicitly test rasterio_write functionality (#528)


---------------------
2023.1.0 - 2023-01-03
---------------------

* core

  * use new `importlib.metadata` interface to select entry points (#521)
  * add filename which caused read error to MapcheteIOError when calling `read_raster_window()` and `read_vector_window()` (#522)


----------------------
2022.12.1 - 2022-12-20
----------------------

* core

  * always use dask executor if defined, even if there is only one task or worker (#517)
  * try to provide more useful information if dask task exception cannot be recovered (#519)

* CI

  * schedule tests every monday and thursday (#518) 


----------------------
2022.12.0 - 2022-12-16
----------------------

* core

  * extend capabilities of `mapchete.io.raster.ReferencedRaster` (#513)
  * allow executing remote mapchete files (#514)
  * adapt to `Shapely 2.0` (#515)

* packaging

  * replace `setuptools` with `hatch` (#516)


----------------------
2022.11.2 - 2022-11-30
----------------------

* core

  * use group prefix for preprocessing tasks (#512)

* CLI

  * pass on `max_workers` to dask executor (#508)


----------------------
2022.11.1 - 2022-11-23
----------------------

* core

  * dask `chunksize` and `max_submitted_tasks` fix (#506)


----------------------
2022.11.0 - 2022-11-21
----------------------

* core

  * GTiff driver: force blocksize being int (#496)
  * fix TileDirectory read error from exotic CRSes (#498)
  * split up `raster.io.RasterioRemoteWriter` class to memory and tempfile subclasses (#500)
  * make sure dask_compute_graph and dask_chunksize are passed on (#502)

* CLI

  * print task details also when using dask executor when `--verbose` flag is active (#501)

* packaging

  * exclude vulnerable rasterio dependency (#490)
  * add python-dateutil to package requirements (#495)
  * rename `master` branch to `main`

* tests

  * add Python 3.10 to tests


---------------------
2022.9.1 - 2022-09-15
---------------------

* packaging

  * remove shapely version <1.8.1 constraint from setup.py


---------------------
2022.9.1 - 2022-09-15
---------------------

* core

  * adapt tiles_count() to handle new shapely behavior; use pytest.mark.parametrize for some tests

* tests

  * make slowest tests faster; reuse DaskExecutor() where possible


---------------------
2022.9.0 - 2022-09-14
---------------------

* core

  * extend capabilities of IndexedFeatures to detect geometries
  * reuse `FileSystem`` object if provided in `makedirs()``
  * add `object_bounds()` to determine object geometry bounds
  * adapt code to changes introduced by `fiona 1.9a2`


---------------------
2022.7.0 - 2022-07-11
---------------------

* core

  * raster drivers `read()` functions now conform with the rasterio behavior in that only if a band index is given as integer, it will return a 2D array. Previously, it also returned an 2D array if a list with only one band index was given. #473
  * empty vector files (read by Fiona) don't fail if the bounds cannot determined in case the file does not contain any features.
  * move metadata parser and driver helper functions into `mapchete.formats.loaders` and `mapchete.formats.tools` packages (#475)


---------------------
2022.6.0 - 2022-06-10
---------------------

* core

  * don't log in info when executor closes #463
  * provide task id in exception if task failes #464
  * allow overwriting certain GDAL settings which are automatically applied when reading from remote data #467

* packaging/CI

  * add ubuntu 22.04 to test matrix #468
  * add project logo and repaired readthedocs build #469


---------------------
2022.4.1 - 2022-04-28
---------------------

* core

  * explicitly check whether futures were cancelled
  * `mapchete stac create-item`: also expand custom zoom levels
  * don't throw exception if STAC file cannot be written or updated
  * add `mapchete stac create-protoype-files` command to write STACTA prototype datasets required by GDAL


---------------------
2022.4.0 - 2022-04-01
---------------------

* core

  * avoid `Future.result()` calls when using dask

---------------------
2022.3.3 - 2022-03-30
---------------------

* core

  * `commands.cp`: fix `overwrite` mode on local filesystems
  * cache input vector file bounding box to improve performance on large GeoJSON files
  * add preliminary RPC referenced raster data support (also requires rasterio fix: https://github.com/rasterio/rasterio/pull/2419)
  * add `dask_propagate_results` flag to `compute()` to activate/deactivate task result propagation on dask clusters

* packaging/CI

  * avoid using GEOS 3.10 which causes shapely bugs


---------------------
2022.3.2 - 2022-03-16
---------------------

* core

  * fix zoom levels getting processed in the wrong order
  * fix process tiles generation which yielded a different number than estimated from `tiles_count()`
  * add fallback if `rasterio.crs.CRS` cannot convert CRS to SRS authority (issue in newer PROJ versions)

* packaging/CI

  * add Python 3.9 to test matrix


---------------------
2022.3.1 - 2022-03-11
---------------------

* core

  * automatically write/update STAC JSON file for TileDirectory output


---------------------
2022.3.0 - 2022-03-09
---------------------

* core

  * use batches when yielding completed futures from dask
  * fix ``KeyError`` when removing futures from threads executor


---------------------
2022.2.2 - 2022-02-25
---------------------

* core

  * avoid infinite recursion when retrying reprojection with clipped geometry
  * attach missing dask client loop to `distributed.as_completed` call
  * fixed infinite future yield when using `DaskExecutor.as_completed()`
  * expose `--dask-no-task-graph` flag to `execute` and `convert` commands

---------------------
2022.2.1 - 2022-02-23
---------------------

* core

  * implement dask task graphs when using dask as executor
  * enable setting executor after ``Job`` was initialized
  * fix minor bugs in ``DaskExecutor.as_completed()``:

    * running futures count
    * pass on Exception after cancel signal when client is closed while waiting for remaining futures

* add caching options for vector file and raster file inputs


---------------------
2022.2.0 - 2022-02-03
---------------------

* core

  * fix feature clip when reading from vector input outside of CRS bounds
  * separately count submitted tasks instead of relying on ``distributed.as_completed().count()``
  * add GCPs referenced input raster support (#418)


---------------------
2022.1.2 - 2022-01-31
---------------------

* core

  * try to make dask executor more resilient by adding timeouts (accessible via the ``MP_FUTURES_TIMEOUT`` environment setting) and retries if a future call times out


---------------------
2022.1.1 - 2022-01-19
---------------------

* core

  * fix ``ConcurrentFuturesExecutor.as_completed()`` when waiting for finished futures

* testing

  * split up ``reproject_geometry`` tests for CRS bounds clipping


---------------------
2022.1.0 - 2022-01-18
---------------------

* core

  * fix #404: log useful information on info
  * replace ``cascacded_union()`` with ``unary_union()`` to handle Shapely warning
  * fix ``fsspec.ls`` call
  * make geometry clip to CRS bounds in ``reproject_geometry()`` optional
  * add thread concurrency for rows in ``tiles_exist``


----------------------
2021.12.3 - 2021-12-16
----------------------

* core

  * fix #395: area intersects with bounds


----------------------
2021.12.2 - 2021-12-14
----------------------

* core

  * let ``ditributed.as_completed()`` gather future results


----------------------
2021.12.1 - 2021-12-14
----------------------

* core

  * explicitly call ``Future.release()`` before yielding result from ``DaskExecutor``


----------------------
2021.12.0 - 2021-12-02
----------------------

* core

  * make sure I/O retry settings read from environment are properly casted to int/float


----------------------
2021.11.3 - 2021-11-23
----------------------

* core

  * expose ``chunksize`` parameter of dask executor to ``execute`` and ``convert`` commands.
  * clean up ``DaskExecutor.as_completed()`` code & log messages
  * ``index``: use ``tiles_exist()`` row generators to speed up index creation


----------------------
2021.11.2 - 2021-11-16
----------------------

* core

  * dask: submit tasks in chunks to scheduler


----------------------
2021.11.1 - 2021-11-16
----------------------

* core

  * use generators to start processing as soon as possible and to reduce memory consumption when processing large areas
  * add ``preprocessing_tasks`` ``tiles_tasks`` attributes to ``Job``
  * reduce code smells

* package

  * fix ``http`` extra in ``setup.py``


----------------------
2021.11.0 - 2021-11-03
----------------------

* CLI

  * ``convert``: enable ``--output-pyramid`` to read custom grid from JSON file
  * ``stac create-item``: add ``--relative-paths`` flag for asset URL

* package

  * add ``mapchete.testing`` including convenience tools
  * use temporary directories for outputs where possible when testing processes


----------------------
2021.10.3 - 2021-10-19
----------------------

* core

  * ``mapchete.Executor``: fix call in ``DaskExecutor.as_completed()`` by not providing current client as loop


----------------------
2021.10.2 - 2021-10-19
----------------------

* core

  * ``mapchete.Executor``: add ``max_submitted_futures`` option to ``DaskExecutor.as_completed()`` to cap the number of tasks sent to the scheduler; also immediately yield finished tasks once they are available


----------------------
2021.10.1 - 2021-10-08
----------------------

* core

  * ``mapchete.Executor``: make sure futures are removed after yield; track time spent to submit tasks
  * ``mapchete.Executor``: remove task submission delay introduced for debugging
  * ``mapchete.commands.convert``: set process metatiling so output tiles cannot be larger than process metatiles
  * ``mapchete.commmands.convert``: fix overwrite flag on remote single GeoTIFFs
  * ``mapchete.commands.cp``: add ``point`` and ``point-crs`` options
  * don't write all STAC information into metadata.json
  * don't initialize ``InputTile`` objects when processing baselevel tile


----------------------
2021.10.0 - 2021-10-01
----------------------

* packaging
  
  * change version numbering scheme to ``YYYY.MM.x``

* core

  * make sure ``Executor.running_futures`` is cleared after each ``as_completed()`` and ``cancel()`` call
  * fix ``fargs`` and ``fkwargs`` ommission in ``Executor.map()``

* testing

  * skip COG tests if driver is not available in GDAL


-----------------
0.44 - 2021-09-30
-----------------

* core

  * add warnings when using Python<3.7 with usage of start methods other than ``spawn``
  * ``mapchete.Executor`` keep track of running and finished futures and remove finished futures to release memory (on local machine as well as on dask cluster)

* CLI

  * add ``mapchete stac create-item`` command to create a STAC tiled-assets file


-----------------
0.43 - 2021-09-17
-----------------

* core

  * `mapchete.io.vector.reproject_geometry()`:

    * use `pyproj` to determine CRS bounds to clip geometries when reprojecting
    * enable geometry segmentation before geometry is clipped (`segmentize_on_clip=False` and `segmentize_fraction=100` args)

  * suppress `rasterio` warnings when reading rasters (too many `rasterio.errors.NodataShadowWarning`)

* packaging

  * add `pyproj` to dependencies


-----------------
0.42 - 2021-08-27
-----------------

* core

  * add option for input drivers to let processing-heavy tasks be handled by ``mapchete.Executor`` by implementing ``InputData.add_preprocessing_task()`` and ``InputData.get_preprocessing_task_result()`` methods
  * check futures submitted to ``mapchete.Executor`` for exceptions before yielding
  * remove deprecated ``multi`` (now called ``workers``), ``distributed`` and ``max_chunksize`` arguments

* CLI

  * remove deprecated ``--max-chunksize`` option
  * replace "tiles" with "tasks" in progress


-----------------
0.41 - 2021-08-17
-----------------

* core

  * add ``mapchete.commands`` package
  * add ``dask`` as optional ``Executor``
  * expose futures in ``Executor`` class to facilitate job cancellation
  * use ``spawn`` as default multiprocessing start method (#351)
  * fix missing worker logs (#185)
  * rename ``mapchete.cli.utils`` to ``mapchete.cli.options``
  * enable providing process code from within process config

* packaging

  * updated API doc

* CLI

  * CLI: change ``--multi`` option to ``--worker``
  * enable optional concurrency for ``mapchete cp``


-----------------
0.40 - 2021-06-24
-----------------

* core

  * fix overviews creation in sinlge GTiff output (#325)

* packaging

  * drop Python 3.6 support


-----------------
0.39 - 2021-06-08
-----------------

* core

  * fix warnings by removing deprecated calls (#336)
  * fix tiles count (#334)
  * default drivers

    * GTiff

      * pass on custom creation options to GTiff output driver / rasterio (#328)
      * change default GTiff profile (#335, #332)

        * compression: deflate
        * predictor: 2
        * blocksize: 512

    * GeoJSON

      * add LineString geometry type to available output schema (#338)

    * FlatGeobuf

      * add tiled FlatGeobuf output driver (#321)

  * CLI

    * ``cp`` and ``rm``

      * add fsspec filesystem creation options ``--src-fs-opts``, ``--dst-fs-opts`` and ``--fs-opts`` (#339)

  * default processes

    * ``convert``

      * print user warning if deprecated input name is used (#340)

* packaging

  * add black & flake8 code formatting tools (#337)


-----------------
0.38 - 2020-12-10
-----------------

* core

  * allow multipart geometries in GeoJSON (#300)
  * add ``Geobuf`` output format as alternative to store vector data (#302)
  * CLI:

    * ``convert``

      * enable converting vector data (#302)
      * add ``--output-geometry-type`` option for vector data output (#302)
      * fix omission of ``--output-metatiling`` (#302)

    * add ``rm`` command  (#306)

  * add ``mapchete.formats.driver_metadata()`` (#302)
  * add ``mapchete.formats.data_type_from_extension()`` (#302)
  * enable guessing data type (raster or vector) when reading from Tile Directories (#302)
  * ``mapchete.io.clean_geometry_type()``: add ``raise_exception`` flag to disable raising and returning an empty geometry instead (#302)
  * fix issue with ``rasterio>1.1.4`` (fix tile_to_zoom_level()) (#308)

* packaging

  * don't parse requirements.txt in setup.py (#301)
  * add test requirements (#302)


-----------------
0.37 - 2020-11-25
-----------------

* core

  * make retry settings configurable via environment (#296)

    * MAPCHETE_IO_RETRY_TRIES (default: 3)
    * MAPCHETE_IO_RETRY_DELAY (default: 1)
    * MAPCHETE_IO_RETRY_BACKOFF (default: 1)

  * fix non-overlapping bounds if provided as extra kwarg (#295)
  * don't pass on init bounds to mapchete input (#295)


-----------------
0.36 - 2020-11-24
-----------------

* core

  * create local output directory for single GTiff output files (#285)
  * add process area parameter (#287)
  * use optimized GDAL settings for baselayer creation (#291)
  * raise generic MapcheteIOError on read fails (#292)

* CLI

  * add more baselayers in ``serve`` (#278)
  * add ``cp`` command (#282)
  * enable ``serve`` to host multiple mapchete files (#289)
  * enable ``index`` to accept tile directories (#290)
  * expose multiprocessing start method as option in ``execute`` (#293)


-----------------
0.35 - 2020-08-04
-----------------
* fix index updates on remote TileDirectories (#274)
* pass on chunksize to multiprocessing & use ``Pool.__exit__()`` to close (#276)
* use GitHub actions instead of Travis CI
* update Fiona dependency to ``1.8.13.post1``


-----------------
0.34 - 2020-07-08
-----------------
* speed up extension loading by using ``importlib-metadata`` and ``importlib-resources`` instead of ``pkg_resources`` (#267)
* use ``boto`` paging to reduce requests to S3 bucket (#268)


-----------------
0.33 - 2020-03-24
-----------------
* use init_bounds instead of pyramid bounds on readonly mode (#257)
* clean up log messages (fix #251)


-----------------
0.32 - 2020-02-24
-----------------
* default process bounds are now bounds of the process pyramid instead of union of inputs (#242)
* fix overview pixelbuffer error at Antimeridian (#241)
* increased rasterio dependency to version ``1.0.28``
* add hillshade and contour extraction to registered default processes (#237)
* enable ``bigtiff`` and ``cog`` settings for single GTiff outputs (#247)
* enable ``--cog`` option for ``mapchete convert`` (#247)
* enable ``--bidx`` option (band subset) for ``mapchete convert`` (#248)
* only initialize inputs if necessary (#242)
* use ``rio-cogeo`` logic to determine whether to use a memory dataset or a temp file when writing a single GTiff (#217)


-----------------
0.31 - 2019-12-03
-----------------
* don't raise exception when one of the registered processes cannot be imported (#225)
* don't close pool between zoom levels (#227)
* ``_validate`` module renamed to ``validate`` (#230)
* fix inverted hillshade & misleading tile reference (#229)
* fix custom nodata values in overviews (#235)


-----------------
0.30 - 2019-10-22
-----------------
* fixed raise of ``FileNotFounderror`` on ``mapchete.io.raster.read_raster_no_crs()``
* fixed overview ``get_parent()`` on zoom 0 in batch processing
* sort processes alphabetically in ``mapchete processes``
* always raise ``FileNotFoundError`` if input file does not exist
* wait for 1 second between retry attempts on file read error
* added ``--overviews`` and ``--overviews-resampling-method`` to ``mapchete convert``
* fixed overview generation when output pixelbuffer was provided (#220)
* remote reading fixes (#219)

  * add GDAL HTTP options
  * handle ``AccessDenied`` exception which could occur if after an ``RasterioIOError`` a check is run if the file even exists
* increased required minimum NumPy version to 1.16
* pass on output parameters to mapchete process (#215, fixes #214)


-----------------
0.29 - 2019-07-12
-----------------
* fixed convert on single remote files (#205)
* fixed ``FileNotFoundError`` on ``driver_from_file()`` (#201)
* fixed zoom level order when processing multiple zooms (#207)
* inputs get intialized as readonly if only overviews are built (#140)
* AWS secrets get obfuscated in logs (#203)


-----------------
0.28 - 2019-06-18
-----------------

* breaking changes

  * output drivers must now provide ``OutputDataWriter`` and ``OutputDataReader`` classes instead of a single ``OutputData`` class
  * ``OutputDataWriter.close()`` method must accept ``exc_type=None, exc_value=None, exc_traceback=None`` keywords
  * ``mapchete pyramid`` CLI was removed and is replaced by the more versatile ``mapchete convert`` (#157, #192)
  * all CLI multiword options are separated by an hyphen (``-``) instead of underscore (``_``) (#189)

* overview tiles get also updated if child baselevel tile changes (#179)
* on ``batch_process()`` check which process output exists and only use parallelization for process tiles which will be processed (#179)
* fixed ``area_at_zoom()`` when using input groups (#181)
* fixed single GeoTIFF output bounds should use process area (#182)
* fixed YAML warning (#167)
* inputs preserve order (#176)
* enabled writing into single GeoTIFF files (#175)
* enabled multiprocessing spawn method (#173)
* extracted ``execute()`` logic to ``TileProcess`` class (#173)
* process workers now only receive objects and parameters they need (#173)
* parsing mapchete input does not fail if zoom levels do not match
* enable other projections again for GeoJSON output (closing #151)
* let rasterio & fiona decide whether single file can be opened (#186)
* provide option to show less content on CLI mapchete processes (#165)
* automatically detect loggers from registered mapchete packages and user process files
* enable drivers which do not handle pure NumPy arrays or feature lists
* ``OutputData`` classes have new ``output_valid()``, ``output_cleaned()`` and ``extract_subset()`` methods
* ``copy=False`` flag has been added to all NumPy ``.astype()`` calls to avoid unnecessary copying of arrays in memory
* extra requirements have been removed from ``requirements.txt``
* setup.py uses now ``find_packages()`` function to detect subpackages
* minimum required NumPy version is now 1.15


-----------------
0.27 - 2019-01-03
-----------------

* enable reading from output tile directories which have a different CRS
* enable GeoPackage as single file input
* fixed antimeridian shift check
* added retry decorator to read functions & added ``get_gdal_options()`` and
  ``read_raster_no_crs()`` functions
* pass on ``antimeridian_cutting`` from ``reproject_geometry()`` to underlying Fiona
  function
* fix transform shape on non-square tiles (#145)
* fixed VRT NODATA property, use GDAL typenames
* ``mapchete index`` shows progress bar for all tiles instead per zoom level and takes
  ``--point`` parameter
* tile directories now requires ``resampling`` in ``open()``, not in ``read()``
* added ``mapchete.processes.convert``
* use WKT CRS when writing VRT (closing #148)
* updated license year
* ``clean_geometry_type()`` raises ``GeometryTypeError`` if types do not match instead of
  returning ``None``
* default log level now is ``logging.WARNING``, not ``logging.ERROR``


-----------------
0.26 - 2018-11-27
-----------------

* enable VRT creation for indexes
* added ``--vrt`` flag and ``--idx_out_dir`` option to ``mapchete execute``
* renamed ``--out_dir`` to ``--idx_out_dir`` for ``mapchete index``
* ``BufferedTile`` shape (``height``, ``width``) and bounds (``left``, ``bottom``,
  ``right`` and ``top``) properties now return correct values
* ``BufferedTile.shape`` now follows the order ``(height, width)`` (update from
  ``tilematrix 0.18``)
* ``ReferencedRaster`` now also has a ``bounds`` property, take caution when unpacking
  results of ``create_mosaic()``!
* ``create_mosaic()``: use tile columns instead of tile bounding box union to determine
  whether tiles are passing the Antimeridian; fixes #141


-----------------
0.25 - 2018-10-29
-----------------

* use ``concurrent.futures`` instead of ``multiprocessing``
* make some dependencies optional (Flask, boto3, etc.)
* speed up ``count_tiles()``
* ``execute()`` function does not require explicit ``**kwargs`` anymore


-----------------
0.24 - 2018-10-23
-----------------

* breaking changes:

  * all Python versions < 3.5 are not supported anymore!

* default drivers now can handle S3 bucket outputs
* file based output drivers write output metadata into ``metadata.json``
* output directories can be used as input for other processes if they have a
  ``metadata.json``
* if Fiona driver has 'append' mode enabled, index entries get appended instead of writing
  a whole new file


-----------------
0.23 - 2018-08-21
-----------------

* breaking change:

  * for CLI utilities when providing minimum and maximum zoom, it has to have the form of
    ``5,6`` instead of ``5 6``

* remove deprecated ``memoryfile`` usage for ``write_raster_window()``
* fix ``s3`` path for ``mapchete index``
* add ``snap_bounds``, ``clip_bounds`` functions & ``effective_bounds`` to config
* made user processes importable as modules (#115)
* changed ``process_file`` paremeter to ``process``
* added ``mapchete.processes`` entry point to allow other packages add their processes
* switched from argparse to click
* ``execute`` and ``index`` commands accept now more than one mapchete files
* added ``mapchete.cli.commands`` entry point to allow other packages have ``mapchete``
  subcommands


-----------------
0.22 - 2018-05-31
-----------------

* don't pass on ``mapchete_file`` to ``execute()`` kwargs
* apply workaround for tqdm: https://github.com/tqdm/tqdm/issues/481


-----------------
0.21 - 2018-05-30
-----------------

* breaking change:

  * old-style Process classes are not supported anymore

* user process accepts kwargs from custom process parameters
* process_file is imported once when initializing the process (#114)
* when validating, import process_file to quickly reveal ``ImporError``
* fixed ``execute --point``
* also check for ``s3`` URLs when adding GDAL HTTP options
* default ``max_chunksize`` to 1 (#113)


-----------------
0.20 - 2018-04-07
-----------------

* fixed geometry reprojection for LineString and MultiLineString geometries (use buffer
  buffer to repair geometries does not work for these types)
* added ``RasterWindowMemoryFile()`` context manager around ``rasterio.io.MemoryFile``
  (#105)
* passing on dictionary together with numpy array from user process will write the
  dictionary as GeoTIFF tag (#101)
* added ``--wkt_geometry`` to ``execute`` which enables providing process bounds via WKT
* added ``--point`` to ``execute`` which enables providing a point location to be
  processed
* added ``--no_pbar`` to ``execute`` to disable progress bar
* ``mapchete index`` command now can create vector index files (``GeoJSON`` and
  ``GeoPackage``) and a text file containing output tile paths
* ``output.tiles_exist()`` now has two keyword arguments ``process_tile`` and
  ``output_tile`` to enable check for both tile types
* restructuring internal modules (core and config), no API changes


-----------------
0.19 - 2018-02-16
-----------------

* made logging functionality now library friendly (#102)
* added ``mapchete.log`` module with functions simplifying logging for user processes and
  driver plugins
* ``mapchete execute``

  * ``--logfile`` flag writes log files with debug level
  * ``--debug`` disables progress bar & prints debug log output
  * ``--verbose`` enables printing of process tile information while showing the
    progress bar
  * ``--max_chunksize`` lets user decide which maximum chunk size is used by
    ``multiprocessing``

* batch processing module

  * ``mapchete._batch`` functionality absorbed into main module
  * writing output is now handled by workers instead by main process
  * new function ``Mapchete.batch_processor()`` is a generator which processes all of
    the process tiles and returns information (i.e. processing & write times)
  * ``Mapchete.batch_process()`` consumes ``Mapchete.batch_processor()`` without
    returning anything
  * ``quiet`` and ``debug`` flags are deprecated and removed

* ``get_segmentize_value()`` moved from ``mapchete.formats.defaults.raster_file`` to
  ``mapchete.io``
* use GDAL options for remote files (closing #103) per default:

  * ``GDAL_DISABLE_READDIR_ON_OPEN=True``
  * ``GDAL_HTTP_TIMEOUT=30``

* introduced ``mapchete.io.path_is_remote()``


-----------------
0.18 - 2018-02-02
-----------------

* verstion 0.17 was not properly deployed, therefore nev version


-----------------
0.17 - 2018-02-02
-----------------

* ``write_raster_window`` now returns a ``rasterio.MemoryFile()`` if path is
  ``"memoryfile"``
* refactoring of ``MapcheteConfig`` (#99):

  * mapchete configuration changes:

    * ``process_zoom`` and ``process_minzoom``, ``process_maxzoom`` now have to be set via
      ``zoom_levels`` parameter
    * process pyramid now has to be set via a ``pyramid`` dictionary at root element (#78)
    * pyramid type is now called ``grid`` instead of ``type``
    * tile pyramids can now have custom grids (see
      https://github.com/ungarj/tilematrix/blob/master/doc/tilematrix.md#tilepyramid)
    * ``process_bounds`` are now called ``bounds``

  * API changes:

    * new attributes:

      * ``init_zoom_levels`` is a subset of ``zoom_levels`` and indicates initialization
        zoom levels via the ``zoom`` kwarg
      * ``init_bounds`` is a subset of ``bounds`` and indicates initialization bounds via
        the ``bounds`` kwarg

    * deprecated attributes:

      * ``crs`` is now found at ``process_pyramid.crs``
      * ``metatiling`` is now found at ``process_pyramid.metatiling``
      * ``pixelbuffer`` is now found at ``process_pyramid.pixelbuffer``
      * ``inputs`` was renamed to ``input``
      * ``process_bounds`` was renamed to ``bounds``

    * deprecated methods:

      * ``at_zoom()`` now called ``params_at_zoom()``
      * ``process_area()`` now called ``area_at_zoom()``
      * ``process_bounds()`` now called ``bounds_at_zoom()``


-----------------
0.16 - 2018-01-12
-----------------

* added ``TileDirectory`` as additional input option (#89)
* make all default output formats available in ``serve`` (#63)
* remove Pillow from dependencies (related to #63)


-----------------
0.15 - 2018-01-02
-----------------

* enabled optional ``cleanup()`` function for ``InputData`` objects when ``Mapchete`` is
  closed.


-----------------
0.14 - 2018-01-02
-----------------

* added python 3.4, 3.5 and 3.6 support


-----------------
0.13 - 2017-12-21
-----------------

* driver using ``InputData`` function must now accept ``**kwargs``
* fixed ``resampling`` issue introduced with inapropriate usage of ``WarpedVRT`` in
  ``read_raster_window()``
* ``str`` checks now use ``basestring`` to also cover ``unicode`` encodings
* ``read_raster_window()`` now accepts GDAL options which get passed on to
  ``rasterio.Env()``
* all resampling methods from ``rasterio.enums.Resampling`` are now available (#88)


-----------------
0.12 - 2017-11-23
-----------------

* adapt chunksize formula to limit ``multiprocessing`` chunksize between 0 and 16; this
  resolves occuring ``MemoryError()`` and some performance impediments, closing #82
* GeoTIFF output driver: use ``compress`` (like in rasterio) instead of ``compression`` &
  raise ``DeprecationWarning`` when latter is used


-----------------
0.11 - 2017-11-09
-----------------

* ``vector.reproject_geometry()`` throws now ``shapely.errors.TopologicalError`` instead
  of ``RuntimeError`` if reprojected geometry is invalid
* ``vector.reproject_geometry()`` now uses ``fiona.transform.transform_geom()`` internally
* pass on delimiters (zoom levels & process bounds) to drivers ``InputData`` object
* when a tile is specified in ``mapchete execute``, process bounds are clipped to tile
  bounds
* better estimate ``chunksize`` for multiprocessing in tile processing & preparing inputs
* add nodata argument to ``read_raster_window()`` to fix ``rasterio.vrt.WarpedVRT``
  resampling issue


-----------------
0.10 - 2017-10-23
-----------------

* better memory handling by detatching process output data from ``BufferedTile`` objects
* breaking API changes:

  * ``Mapchete.execute()`` returns raw data instead of tile with data attribute
  * ``Mapchete.read()`` returns raw data instead of tile with data attribute
  * ``Mapchete.get_raw_output()`` returns raw data instead of tile with data attribute
  * ``Mapchete.write()`` requires process_tile and data as arguments
  * same valid for all other ``read()`` and ``write()`` functions in drivers &
    ``MapcheteProcess`` object
  * formats ``is_empty()`` function makes just a basic intersection check but does not
    actually look into the data anymore
  * formats ``read()`` functions are not generators anymore but follow the rasterio style
    (2D array when one band index is given, 3D arrays for multiple band indices)

* new ``MapcheteNodataTile`` exception to indicate an empty process output
* raster_file & geotiff Input cache removed
* ``get_segmentize_value()`` function is now public
* use ``rasterio.vrt.WarpedVRT`` class to read raster windows
* source rasters without nodata value or mask are now handled properly (previously a
  default nodata value of 0 was assumed)


----------------
0.9 - 2017-10-04
----------------

* removed GDAL from dependencies by reimplementing ogr ``segmentize()`` using shapely
* use ``cascaded_union()`` instead of ``MultiPolygon`` to determine process area


----------------
0.8 - 2017-09-22
----------------

* process file now will accept a simple ``execute(mp)`` function
* current version number is now accessable at ``mapchete.__version`` (#77)
* added ``--version`` flag to command line tools


----------------
0.7 - 2017-09-20
----------------

* fixed PNG alpha band handling
* added generic ``MapcheteEmptyInputTile`` exception
* internal: available pyramid types are now loaded dynamically from ``tilematrix``
* closed #25: use HTTP errors instead of generating pink tiles in ``mapchete serve``


----------------
0.6 - 2017-09-08
----------------

* ``input_files`` config option now raises a deprecation warning and will be replaced with
  ``input``
* abstract ``input`` types are now available which is necessary for additional non-file
  based input drivers such as DB connections
* improved antimeridian handling in ``create_mosaic()`` (#69)
* improved baselevel generation performance (#74)


----------------
0.5 - 2017-05-07
----------------

* introduced iterable input data groups
* introduced pytest & test coverage of 92%
* adding Travis CI and coveralls integrations
* automated pypi deploy
* introduced ``mapchete.open()`` and ``batch_process()``
* progress bar on batch process
* proper logging & custom exceptions
* documentation on readthedocs.io


----------------
0.4 - 2017-03-02
----------------

* introduced pluggable format drivers (#47)
* ``mapchete formats`` subcommand added; lists available input & output formats
* completely refactored internal module structure
* removed ``self.write()`` function; process outputs now have to be passed on
  via ``return`` (#27)
* ``baselevel`` option now works for both upper and lower zoom levels
* added compression options for GTiff output
* make documentation and docstrings compatible for readthedocs.org


----------------
0.3 - 2016-09-20
----------------

* added new overall ``mapchete`` command line tool, which will replace
  ``mapchete_execute``, ``mapchete_serve`` and ``raster2pyramid``
* added ``mapchete create`` subcommand, which creates a dummy process
  (.mapchete & .py files)
* if using an input file from command line, the configuration input_file
  parameter must now be set to 'from_command_line' instead of 'cli'
* input files can now be opened directly using their identifier instead of
  self.params["input_files"]["identifier"]


----------------
0.2 - 2016-09-07
----------------

* fixed installation bug (io_utils module could not be found)
* rasterio's CRS() class now handles CRSes
* fixed tile --> metatile calculations
* fixed vector file read over antimeridian
* rewrote reproject_geometry() function


----------------
0.1 - 2016-08-23
----------------

* added vector data read
* added vector output (PostGIS & GeoJSON)
* added NumPy tile output
* added spherical mercator support
* tile with buffers next to antimeridian get full data
* combined output\_ ... parameters to output object in mapchete config files


-----
0.0.2
-----

* renamed ``mapchete_execute.py`` command to ``mapchete_execute``
* renamed ``mapchete_serve.py`` command to ``mapchete_serve``
* added ``raster2pyramid`` command
* added ``--tile`` flag in ``mapchete_execute`` for single tile processing
* added ``--port`` flag in ``mapchete_serve`` to customize port
* added ``clip_array_with_vector`` function for user-defined processes


-----
0.0.1
-----

* basic functionality of mapchete_execute
* parallel processing
* parsing of .mapchete files
* reading and writing of raster data
