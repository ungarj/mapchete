# Process structure

## examples

### definition

#### calling script/tool
```python

input_files = {
   "ASTER": "path/to/aster",
   "SRTM": "path/to/srtm",
   "GTOPO": "path/to/gtopo"
    }

output_gpkg = "path/to/gpkg"


merge_dem = MapcheteProcess(
    input_data = input_files,
    base_zoom = 12
    )

for tile in tiles:
    result = merge_dem.execute(tile)
    gpkg_save(result, tile)
```

#### process file
```python
from mapchete import MapcheteProcess

MapcheteProcess.init(
    input_data=[],
    parameter1=...,
    parameter2=...,
    tile=(zoom, col, row),
    )
MapcheteProcess.execute()
```



<!-- ### execution
```python
# everything
MapcheteProcess.execute()
# just a subset
MapcheteProcess.execute(bounds=(1, 3, 2, 5))
MapcheteProcess.execute(tile=(zoom, col, row))
MapcheteProcess.execute(metatile=(zoom, col, row))
``` -->

## responsibilities

### mapchete
* spatial subsetting
* process inputs (e.g. input files)
* process outputs (format & location)

### process

#### ```__init__()```
* input files
* input parameters
* output file/directory (format & location)

#### ```execute()```
* processing data
* return result
