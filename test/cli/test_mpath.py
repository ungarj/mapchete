from test.cli.default import run_cli

import pytest
from pytest_lazyfixture import lazy_fixture

from mapchete.cli.mpath import mpath
from mapchete.testing import ProcessFixture


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


def test_cp_directory(local_tiledir, mp_tmpdir):
    out_path = mp_tmpdir / local_tiledir.name
    assert run_cli(["cp", str(local_tiledir), str(out_path), "--recursive"], cli=mpath)
    assert out_path.ls()


def test_rm(metadata_json, mp_tmpdir):
    out_file = mp_tmpdir / metadata_json.name
    assert run_cli(["cp", str(metadata_json), out_file], cli=mpath)
    assert run_cli(["rm", out_file, "--force"], cli=mpath)


def test_read_json(metadata_json):
    assert run_cli(["read-json", str(metadata_json)], cli=mpath)


def test_read_text(metadata_json):
    assert run_cli(["read-text", str(metadata_json)], cli=mpath)


def test_read_yaml(example_mapchete: ProcessFixture):
    assert run_cli(["read-yaml", str(example_mapchete.path)], cli=mpath)


@pytest.mark.integration
@pytest.mark.parametrize(
    "tiledir",
    [
        lazy_fixture("local_tiledir"),
        # lazy_fixture("http_tiledir"),
        # lazy_fixture("secure_http_tiledir"),
    ],
)
def test_sync_dir(tiledir, mp_tmpdir):
    assert not mp_tmpdir.ls()
    assert run_cli(["sync", str(tiledir), str(mp_tmpdir), "--verbose"], cli=mpath)
    assert mp_tmpdir.ls()


@pytest.mark.integration
@pytest.mark.parametrize(
    "tiledir",
    [
        lazy_fixture("local_tiledir"),
        # lazy_fixture("http_tiledir"),
        # lazy_fixture("secure_http_tiledir"),
    ],
)
def test_sync_dir_count(tiledir, mp_tmpdir):
    assert not mp_tmpdir.ls()
    assert run_cli(["sync", str(tiledir), str(mp_tmpdir), "--count"], cli=mpath)
    assert mp_tmpdir.ls()
