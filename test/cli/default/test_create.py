from test.cli.default import run_cli

import oyaml as yaml
import pytest


def test_create(mp_tmpdir, cleantopo_br_tif):
    """Run mapchete create and execute."""
    temp_mapchete = mp_tmpdir / "temp.mapchete"
    temp_process = mp_tmpdir / "temp.py"
    out_format = "GTiff"
    # create from template
    run_cli(
        [
            "create",
            "--mapchete-file",
            str(temp_mapchete),
            "--process-file",
            str(temp_process),
            "--out-format",
            out_format,
            "--pyramid-type",
            "geodetic",
        ],
        expected_exit_code=0,
    )
    # edit configuration
    with temp_mapchete.open("r") as config_file:
        config = yaml.safe_load(config_file)
        config["output"].update(bands=1, dtype="uint8", path=str(mp_tmpdir))
    with temp_mapchete.open("w") as config_file:
        config_file.write(yaml.dump(config, default_flow_style=False))


def test_create_existing(mp_tmpdir):
    """Run mapchete create and execute."""
    temp_mapchete = mp_tmpdir / "temp.mapchete"
    temp_process = mp_tmpdir / "temp.py"
    out_format = "GTiff"
    # create files from template
    args = [
        "create",
        "--mapchete-file",
        temp_mapchete,
        "--process-file",
        temp_process,
        "--out-format",
        out_format,
        "--pyramid-type",
        "geodetic",
    ]
    run_cli(args)
    # try to create again
    with pytest.raises((IOError, OSError)):  # for python 2 and 3
        run_cli(args, expected_exit_code=-1)
