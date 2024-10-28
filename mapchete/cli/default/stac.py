import json
import logging

import click
import fsspec
import oyaml as yaml

import mapchete
from mapchete.cli import options
from mapchete.config.parse import raw_conf, raw_conf_output_pyramid
from mapchete.formats import read_output_metadata
from mapchete.io import MPath
from mapchete.stac import (
    create_prototype_files,
    tile_direcotry_item_to_dict,
    tile_directory_stac_item,
)
from mapchete.zoom_levels import ZoomLevels

logger = logging.getLogger(__name__)


@click.group(help="Tools to handle STAC metadata.")
def stac():
    pass


@stac.command(help="Create STAC item metadata.")
@options.arg_input
@click.option("--item-id", "-i", type=click.STRING, help="Unique item ID.")
@click.option(
    "--item-metadata",
    "-m",
    type=click.Path(),
    help="Optional additional item metadata to be appended. Must be a YAML file.",
)
@options.opt_zoom
@click.option("--item-path", "-p", type=click.Path(), help="Path of output STAC item.")
@click.option("--asset-basepath", type=click.Path(), help="Alternative asset basepath.")
@click.option("--relative-paths", is_flag=True, help="Use relative paths.")
@click.option(
    "--indent",
    type=click.INT,
    default=4,
    help="Indentation for output JSON. (default: 4)",
)
@options.opt_bounds
@options.opt_bounds_crs
@options.opt_force
@options.opt_debug
def create_item(
    input_,
    item_id=None,
    item_metadata=None,
    asset_basepath=None,
    zoom=None,
    bounds=None,
    bounds_crs=None,
    item_path=None,
    relative_paths=False,
    indent=None,
    force=None,
    **kwargs,
):
    (
        tile_pyramid,
        default_basepath,
        default_id,
        default_bounds,
        default_bounds_crs,
        default_zoom,
        default_item_metadata,
        band_asset_template,
    ) = output_info(input_)

    if default_zoom:
        zoom = zoom or default_zoom
    zoom = ZoomLevels.from_inp(zoom)

    if zoom is None:  # pragma: no cover
        raise ValueError("zoom must be set")

    if item_metadata:  # pragma: no cover
        with fsspec.open(item_metadata) as src:
            metadata = yaml.safe_load(src.read())
    else:
        metadata = default_item_metadata or {}

    item_id = item_id or metadata.get("id", default_id)
    logger.debug("use item ID %s", item_id)
    item_path = item_path or MPath.from_inp(default_basepath) / f"{item_id}.json"
    item = tile_directory_stac_item(
        item_id=item_id,
        item_metadata=metadata,
        tile_pyramid=tile_pyramid,
        zoom_levels=zoom,
        bounds=bounds or default_bounds,
        bounds_crs=bounds_crs or default_bounds_crs,
        item_path=item_path,
        asset_basepath=asset_basepath,
        relative_paths=relative_paths,
        band_asset_template=band_asset_template,
        bands_type=None,
        crs_unit_to_meter=1,
    )
    logger.debug("item_path: %s", item_path)
    item_json = json.dumps(tile_direcotry_item_to_dict(item), indent=indent)
    click.echo(item_json)
    if force or click.confirm(f"Write output to {item_path}?", abort=True):
        with fsspec.open(item_path, "w") as dst:
            dst.write(item_json)


def output_info(inp):
    path = MPath.from_inp(inp)
    if path.suffix == ".mapchete":
        conf = raw_conf(path)
        default_basepath = MPath.from_inp(conf["output"])
        return (
            raw_conf_output_pyramid(conf),
            default_basepath,
            default_basepath.name,
            conf.get("bounds"),
            conf.get("bounds_crs"),
            conf.get("zoom_levels"),
            conf["output"].get("stac"),
            conf["output"].get("tile_path_schema", "{zoom}/{row}/{col}.{extension}"),
        )

    output_metadata = read_output_metadata(path / "metadata.json")
    return (
        output_metadata["pyramid"],
        path,
        path.name,
        None,
        None,
        None,
        None,
        output_metadata.get("tile_path_schema", "{zoom}/{row}/{col}.{extension}"),
    )


@stac.command(name="create-prototype-files", help="Create STAC item prototype files.")
@options.arg_input
@options.opt_force
@options.opt_debug
def prototype_files(
    input_,
    force=None,
    **kwargs,
):
    with mapchete.open(input_, mode="readonly") as mp:
        create_prototype_files(mp)
