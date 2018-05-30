

def execute(
    mp,
    some_float_parameter,
    some_string_parameter,
    some_integer_parameter,
    some_bool_parameter
):
    """User defined process."""
    assert some_integer_parameter == 12
    assert some_float_parameter == 5.3
    assert some_string_parameter == 'string1'
    assert some_bool_parameter is True
    return "empty"
