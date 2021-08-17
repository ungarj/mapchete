import logging

from mapchete._registered import processes

logger = logging.getLogger(__name__)


def registered_processes(process_name=None):
    """
    Return registered process modules.

    Parameters
    ----------
    process_name : str
        Python path of process.

    Returns
    -------
    module
    """

    def _import():
        # try to load processes
        for v in processes:
            logger.debug("try to load %s", v)
            try:
                loaded = v.load()
                if process_name is None or process_name == loaded.__name__:
                    yield loaded
            except Exception as e:  # pragma: no cover
                logger.warning("%s could not be imported: %s", str(v), str(e))

    # sort processes alphabetically
    imported = list(_import())
    imported.sort(key=lambda x: x.__name__)
    logger.debug("%s processes found", len(imported))
    return imported


def process_names_docstrings(process_name=None):
    """
    Return registered process module names and docstrings.

    Parameters
    ----------
    process_name : str
        Python path of process

    Returns
    -------
    list of tuples
        tuples contain containing process name and process docstring
    """
    return [
        (process.__name__, process.execute.__doc__)
        for process in registered_processes(process_name=process_name)
    ]
