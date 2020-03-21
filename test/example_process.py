"""Example process file."""


def execute(mp, file1):
    """User defined process."""
    # Reading and writing data works like this:
    if file1.is_empty():
        return "empty"
        # This assures a transparent tile instead of a pink error tile
        # is returned when using mapchete serve.
    dem = file1.read(resampling="bilinear")
    return dem
