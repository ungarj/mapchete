try:
    from importlib import metadata
except ImportError:  # pragma: no cover
    # <PY38 use backport
    import importlib_metadata as metadata

entry_points = metadata.entry_points()
if hasattr(entry_points, "select"):  # for Python 3.10 and higher
    commands = entry_points.select(group="mapchete.cli.commands")
    drivers = entry_points.select(group="mapchete.formats.drivers")
    processes = entry_points.select(group="mapchete.processes")
else:  # for Python 3.9 and lower
    commands = entry_points.get("mapchete.cli.commands", {})
    drivers = entry_points.get("mapchete.formats.drivers", {})
    processes = entry_points.get("mapchete.processes", {})
