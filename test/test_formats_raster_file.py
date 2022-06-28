from mapchete.formats.default.raster_file import InputData


def test_read_indexes_shape(cleantopo_br_tiledir, cleantopo_br_tif):
    input_data = InputData({"path": cleantopo_br_tif})
    input_tile = input_data.open(cleantopo_br_tiledir.first_process_tile())

    # no indexes --> 3D array
    three_d_arr = input_tile.read()
    assert three_d_arr.ndim == 3

    # list index --> 3D array
    three_d_arr = input_tile.read([1])
    assert three_d_arr.ndim == 3

    # int index --> 2D array
    two_d_arr = input_tile.read(1)
    assert two_d_arr.ndim == 2
