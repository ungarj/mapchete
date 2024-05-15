from test.cli.default import run_cli


def test_formats(capfd):
    """Output of mapchete formats command."""
    run_cli(["formats"])
    err = capfd.readouterr()[1]
    assert not err
    run_cli(["formats", "-i"])
    err = capfd.readouterr()[1]
    assert not err
    run_cli(["formats", "-o"])
    err = capfd.readouterr()[1]
    assert not err
