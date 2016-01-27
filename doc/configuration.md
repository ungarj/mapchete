# Process configuration file

The idea behind introducing process configuration files is to clearly separate
the process parameters from the process code itself. This enables using the
same process code with different settings.

### Configurable by zoom level
A process can be configured to use different parameters in different zoom
levels. This comes handy when for example processing a hill shade for multiple
scales (zoom levels) where DEM exaggeration can be adjusted for each scale.

* ```input_files: list of input files```
* user defined parameters (e.g. DEM exxageration, resampling methods, etc.)

### Globally configured
Some parameters are required globally, such as the output format and location.
* ```output_path: path to file```
* ```output_format: one of the available output formats```
* user defined parameters


## Predefined parameters
Some parameters are required for processing such as the input files, output
file and type as well as zoom level(s) and bounds.

* ```input_files: list of input files```

  Input files have to be defined within this parameter as they are used to
  calculate the process bounding box if no ```process_bounds``` are set. Note:
  this can lead to unintended AOI results as well as performance issues. Also,
  due to the nature of zoom level dependent input files, the automatically
  generated process bounds can vary for each zoom level.

* ```process_bounds: left bottom right top```

  Values defining the output bounding box. If this parameter is not explicitly
  set, the program uses the union of all ```input_files```.

* ```process_zoom: zoom level```

  Zoom level to be processed. Will be overwritten if ```process_minzoom``` and
  ```process_maxzoom``` are set.

* ```process_minzoom: zoom level```, ```process_maxzoom: zoom level```

  Range of zoom levels to be processed.
