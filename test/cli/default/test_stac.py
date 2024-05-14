from test.cli.default import run_cli

import pytest
import rasterio
from packaging import version

from mapchete.io import rasterio_open


def test_mapchete_file(cleantopo_br):
    run_cli(["execute", cleantopo_br.path])
    run_cli(["stac", "create-item", cleantopo_br.path, "-z", "5", "--force"])


@pytest.mark.integration
def test_tiledir(http_tiledir, mp_tmpdir):
    run_cli(
        [
            "stac",
            "create-item",
            http_tiledir,
            "-z",
            "5",
            "--force",
            "--item-path",
            f"{mp_tmpdir}/stac_example.json",
        ]
    )


@pytest.mark.skipif(
    version.parse(rasterio.__gdal_version__) < version.parse("3.3.0"),
    reason="required STACTA driver is only available in GDAL>=3.3.0",
)
def test_prototype_files(cleantopo_br):
    run_cli(["execute", cleantopo_br.path])
    run_cli(["stac", "create-prototype-files", cleantopo_br.path])
    rasterio_open(cleantopo_br.mp().config.output.stac_path)
