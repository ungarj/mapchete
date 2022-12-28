try:
    from importlib import metadata
except ImportError:  # pragma: no cover
    # <PY38 use backport
    import importlib_metadata as metadata

entry_points = metadata.entry_points()
commands = entry_points.select(group="mapchete.cli.commands")
drivers = entry_points.select(group="mapchete.formats.drivers")
processes = entry_points.select(group="mapchete.processes")
