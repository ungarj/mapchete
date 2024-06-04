from importlib import metadata

entry_points = metadata.entry_points()
commands = entry_points.select(group="mapchete.cli.commands")
drivers = entry_points.select(group="mapchete.formats.drivers")
processes = entry_points.select(group="mapchete.processes")
