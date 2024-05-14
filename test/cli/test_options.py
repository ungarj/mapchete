from mapchete.cli import options


def test_fs_opt_extractor():
    kwargs = options._cb_key_val(
        None,
        None,
        [
            "str=bar",
            "int=2",
            "float=1.5",
            "bool1=true",
            "bool2=FALSE",
            "bool3=yes",
            "bool4=no",
            "none=none",
            "none2=null",
        ],
    )
    assert isinstance(kwargs, dict)
    assert kwargs["str"] == "bar"
    assert kwargs["int"] == 2
    assert kwargs["float"] == 1.5
    assert kwargs["bool1"] is True
    assert kwargs["bool2"] is False
    assert kwargs["bool3"] is True
    assert kwargs["bool4"] is False
    assert kwargs["none"] is None
    assert kwargs["none2"] is None
