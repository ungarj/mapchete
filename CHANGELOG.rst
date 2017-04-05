#########
Changelog
#########

---
0.5
---
* introduced iterable input data groups
* introduced pytest
* adding Travis CI and coveralls integrations

---
0.4
---

* introduced pluggable format drivers (#47)
* ``mapchete formats`` subcommand added; lists available input & output formats
* completely refactored internal module structure
* removed ``self.write()`` function; process outputs now have to be passed on
  via ``return`` (#27)
* ``baselevel`` option now works for both upper and lower zoom levels
* added compression options for GTiff output
* make documentation and docstrings compatible for readthedocs.org

---
0.3
---

* added new overall ``mapchete`` command line tool, which will replace
  ``mapchete_execute``, ``mapchete_serve`` and ``raster2pyramid``
* added ``mapchete create`` subcommand, which creates a dummy process
  (.mapchete & .py files)
* if using an input file from command line, the configuration input_file
  parameter must now be set to 'from_command_line' instead of 'cli'
* input files can now be opened directly using their identifier instead of self.params["input_files"]["identifier"]

---
0.2
---

* fixed installation bug (io_utils module could not be found)
* rasterio's CRS() class now handles CRSes
* fixed tile --> metatile calculations
* fixed vector file read over antimeridian
* rewrote reproject_geometry() function

---
0.1
---

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
