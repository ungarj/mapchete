from mapchete.formats.loaders import (
    load_input_reader,
    load_output_reader,
    load_output_writer,
    detect_driver_classes,
    available_input_formats,
    available_output_formats,
    driver_metadata,
    driver_from_extension,
    driver_from_file,
    data_type_from_extension,
)
from mapchete.formats.metadata import (
    dump_metadata,
    load_metadata,
    read_output_metadata,
    write_output_metadata,
    compare_metadata_params,
)
from mapchete.formats.models import DriverClassMetadata, DriverMetadata

__all__ = [
    "available_input_formats",
    "available_output_formats",
    "load_input_reader",
    "load_output_reader",
    "load_output_writer",
    "driver_metadata",
    "driver_from_extension",
    "driver_from_file",
    "data_type_from_extension",
    "dump_metadata",
    "load_metadata",
    "read_output_metadata",
    "write_output_metadata",
    "compare_metadata_params",
    "driver",
    "detect_driver_classes",
    "DriverClassMetadata",
    "DriverMetadata",
]


def driver(**kwargs):
    """
    Decorator indicating a driver class.
    """

    def wrapper(obj):
        obj.__mp_driver__ = DriverClassMetadata(**kwargs)
        return obj

    return wrapper
