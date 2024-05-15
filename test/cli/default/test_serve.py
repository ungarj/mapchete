import warnings
from test.cli.default import run_cli

import pytest
from rasterio.io import MemoryFile


def test_serve_cli_params(cleantopo_br):
    """Test whether different CLI params pass."""
    # assert too few arguments error
    with pytest.raises(SystemExit):
        run_cli(["serve"])

    for args in [
        ["serve", cleantopo_br.path],
        ["serve", cleantopo_br.path, "--port", "5001"],
        ["serve", cleantopo_br.path, "--internal-cache", "512"],
        ["serve", cleantopo_br.path, "--zoom", "5"],
        ["serve", cleantopo_br.path, "--bounds", "-1", "-1", "1", "1"],
        ["serve", cleantopo_br.path, "--overwrite"],
        ["serve", cleantopo_br.path, "--readonly"],
        ["serve", cleantopo_br.path, "--memory"],
    ]:
        run_cli(args)


def test_serve(client):
    """Mapchete serve with default settings."""
    tile_base_url = "/wmts_simple/1.0.0/dem_to_hillshade/default/WGS84/"
    for url in ["/"]:
        response = client.get(url)
        assert response.status_code == 200
    for url in [
        tile_base_url + "5/30/62.png",
        tile_base_url + "5/30/63.png",
        tile_base_url + "5/31/62.png",
        tile_base_url + "5/31/63.png",
    ]:
        response = client.get(url)
        assert response.status_code == 200
        img = response.data
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with MemoryFile(img) as memfile:
                with memfile.open() as dataset:
                    data = dataset.read()
                    # get alpha band and assert some pixels are masked
                    assert data[3].any()
    # test outside zoom range
    response = client.get(tile_base_url + "6/31/63.png")
    assert response.status_code == 200
    img = response.data
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with MemoryFile(img) as memfile:
            with memfile.open() as dataset:
                data = dataset.read()
                assert not data.all()
    # test invalid url
    response = client.get(tile_base_url + "invalid_url")
    assert response.status_code == 404
