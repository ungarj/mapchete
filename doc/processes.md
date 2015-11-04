# Process structure

## examples

### definition
```python
MapcheteProcess
```

### execution
```python
# everything
MapcheteProcess.execute()
# just a subset
MapcheteProcess.execute(bounds=(1, 3, 2, 5))
MapcheteProcess.execute(tile=(zoom, col, row))
MapcheteProcess.execute(metatile=(zoom, col, row))
```

## responsibilities

### mapchete
* spatial subsetting
* process inputs (e.g. input files)
* process outputs (format & location)

### process

#### .__init__()
* process inputs (e.g. input files)
* process outputs (format & location)

#### .execute()
* processing data
* return result
