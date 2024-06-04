try:
    from importlib import metadata
except ImportError:  # pragma: no cover
    # backport for Python 3.8 and lower
    import importlib_metadata as metadata

entry_points = metadata.entry_points()
if hasattr(entry_points, "select"):  # for Python 3.10 and higher
    commands = entry_points.select(group="mapchete.cli.commands")
    drivers = entry_points.select(group="mapchete.formats.drivers")
    processes = entry_points.select(group="mapchete.processes")
# for Python 3.9 and lower
else:  # pragma: no cover
    commands = entry_points.get("mapchete.cli.commands", {})  # type: ignore
    drivers = entry_points.get("mapchete.formats.drivers", {})  # type: ignore
    processes = entry_points.get("mapchete.processes", {})  # type: ignore
