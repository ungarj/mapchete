# How to create a mapchete process

A mapchete process has two parts. First, the process itself has to be implemented by creating a file with a ```MapcheteProcess``` class. This class requires besides some initial properties an ```execute()``` function, where you can define what shall happen with your data.

Second, a mapchete process requires a configuration where all the necessary information is collected such as the location to your process Python file, the output location and format, and any user-defined parameters desired. This configuration file has a YAML syntax and has to have a ```.mapchete``` file extension.

If you have both ready, you can point either ```mapchete_serve``` or ```mapchete_execute``` to your process configuration (```.mapchete```) file to either view your process output in a browser or batch process a larger area.

```shell
mapchete_serve my_process.mapchete
```
Starts a local web server on port 5000 with a simple OpenLayers interface.
```shell
mapchete_execute my_process.mapchete --zoom 5 10
```
Executes your process on zoom level 5 to 10.


## process file
```python
from mapchete import MapcheteProcess

class Process(MapcheteProcess):
    """Main process class"""
    def __init__(self, **kwargs):
        """Process initialization"""
        # init process
        MapcheteProcess.__init__(
            self,
            identifier = "my_process_id",
            title="My long process title",
            version = "0.2",
            abstract="short description on what my process does",
        )

    def execute(self):
        # insert magic here

```
