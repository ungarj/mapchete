"""Mapchete process file template."""


def execute(mp):
    """
    Insert your python code here.

    Access input data specified in the .mapchete file:

    with mp.open("<input_id>") as src:
        data = src.read()

    For vector data a list of features is returned, for raster data a numpy
    array. Data is already reprojected.

    To write the process output simply return a feature list or numpy array:

    return modified_data

    Please note the returned data type has to match the output type specified
    in the .mapchete file.
    """
