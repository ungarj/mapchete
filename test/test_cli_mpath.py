import pytest

from mapchete.cli.mpath import mpath

from .test_cli import run_cli


@pytest.mark.integration
def test_mpath():
    # """Main CLI."""
    for command in ["exists", "ls", "read-text", "read-json"]:
        run_cli(
            [command],
            expected_exit_code=2,
            output_contains="Error: Missing argument",
            raise_exc=False,
            cli=mpath,
        )


def test_exists(metadata_json):
    assert run_cli(["exists", str(metadata_json)])
