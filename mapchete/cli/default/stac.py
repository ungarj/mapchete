import click
import fsspec
import logging
import json
import os
import oyaml as yaml

from mapchete.cli import options
from mapchete.config import raw_conf, raw_conf_output_pyramid
from mapchete.formats import read_output_metadata
from mapchete.stac import tile_directory_stac_item
from mapchete.validate import validate_zooms


logger = logging.getLogger(__name__)


@click.group()
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
    ) = output_info(input_)

    if default_zoom:
        zoom = zoom or validate_zooms(default_zoom)

    if zoom is None:  # pragma: no cover
        raise ValueError("zoom must be set")
    min_zoom, max_zoom = min(zoom), max(zoom)

    if item_metadata:  # pragma: no cover
        with fsspec.open(item_metadata) as src:
            metadata = yaml.safe_load(src.read())
    else:
        metadata = default_item_metadata or {}

    item_id = item_id or metadata.get("id", default_id)
    logger.debug("use item ID %s", item_id)
    item_path = item_path or os.path.join(default_basepath, f"{item_id}.json")
    item = tile_directory_stac_item(
        item_id=item_id,
        item_metadata=metadata,
        tile_pyramid=tile_pyramid,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bounds=bounds or default_bounds,
        bounds_crs=bounds_crs or default_bounds_crs,
        item_path=item_path,
        asset_basepath=asset_basepath,
        relative_paths=None,
        bands_type=None,
        crs_unit_to_meter=1,
    )
    logger.debug("item_path: %s", item_path)
    item_json = json.dumps(item.to_dict(), indent=indent)
    click.echo(item_json)
    if force or click.confirm(f"Write output to {item_path}?", abort=True):
        with fsspec.open(item_path, "w") as dst:
            dst.write(item_json)


def output_info(inp):
    if inp.endswith(".mapchete"):
        conf = raw_conf(inp)
        default_basepath = os.path.dirname(conf["output"]["path"].strip("/") + "/")
        return (
            raw_conf_output_pyramid(conf),
            default_basepath,
            os.path.basename(default_basepath),
            conf.get("bounds"),
            conf.get("bounds_crs"),
            conf.get("zoom_levels"),
            conf["output"].get("stac"),
        )

    default_basepath = inp.strip("/")
    return (
        read_output_metadata(os.path.join(inp, "metadata.json"))["pyramid"],
        default_basepath,
        os.path.basename(default_basepath),
        None,
        None,
        None,
        None,
    )
