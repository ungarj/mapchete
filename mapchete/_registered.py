try:
    from importlib import metadata
except ImportError:  # pragma: no cover
    # <PY38 use backport
    import importlib_metadata as metadata

commands = metadata.entry_points().get("mapchete.cli.commands", ())
drivers = metadata.entry_points().get("mapchete.formats.drivers", ())
processes = metadata.entry_points().get("mapchete.processes", ())
