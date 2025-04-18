# @pytest.mark.integration
# def test_output_s3_single_gtiff_error(output_s3_single_gtiff_error):
#     # the process file will raise an exception on purpose
#     with pytest.raises(AssertionError):
#         with output_s3_single_gtiff_error.mp() as mp:
#             mp.execute_tile(output_s3_single_gtiff_error.first_process_tile())
#     # make sure no output has been written
#     assert not path_exists(mp.config.output.path)
