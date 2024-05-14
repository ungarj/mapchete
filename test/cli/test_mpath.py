from test.cli.default import run_cli

import pytest

from mapchete.cli.mpath import mpath


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
    assert run_cli(["exists", str(metadata_json)], cli=mpath)


def test_ls(metadata_json):
    assert run_cli(["ls", str(metadata_json.parent)], cli=mpath)


def test_ls_recursive(metadata_json):
    assert run_cli(["ls", str(metadata_json.parent), "--recursive"], cli=mpath)


def test_cp(metadata_json, mp_tmpdir):
    out_file = mp_tmpdir / metadata_json.name
    assert run_cli(["cp", str(metadata_json), out_file], cli=mpath)


def test_rm(metadata_json, mp_tmpdir):
    out_file = mp_tmpdir / metadata_json.name
    assert run_cli(["cp", str(metadata_json), out_file], cli=mpath)
    assert run_cli(["rm", out_file, "--force"], cli=mpath)


def test_read_json(metadata_json):
    assert run_cli(["read-json", str(metadata_json)], cli=mpath)


def test_read_text(metadata_json):
    assert run_cli(["read-text", str(metadata_json)], cli=mpath)
