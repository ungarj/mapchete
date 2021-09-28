import click
import fsspec
import json
import os
import oyaml as yaml

from mapchete.cli import options
from mapchete.config import raw_conf, raw_conf_output_pyramid
from mapchete.formats import read_output_metadata
from mapchete.stac import create_stac_item


@click.group()
def stac():
    pass


@stac.command(help="Create STAC item metadata.")
@options.arg_input
@click.option("--item-id", "-i", type=click.STRING)
@click.option("--item-metadata", "-m", type=click.Path())
@options.opt_zoom
@click.option("--item-basepath", type=click.Path())
@click.option("--alternative-basepath", type=click.Path())
@click.option("--self-href", type=click.Path())
@click.option("--thumbnail-href", type=click.Path())
@click.option("--indent", type=click.INT, default=4)
@options.opt_force
@options.opt_out_path
def create_item(
    input_,
    item_id=None,
    item_metadata=None,
    item_basepath=None,
    alternative_basepath=None,
    zoom=None,
    self_href=None,
    thumbnail_href=None,
    out_path=None,
    indent=None,
    force=None,
):
    if zoom is None:
        raise ValueError("zoom must be set")
    elif len(zoom) == 1:
        min_zoom = zoom
        max_zoom = zoom
    else:
        min_zoom, max_zoom = zoom

    tile_pyramid, default_basepath, default_id = output_info(input_)

    if item_metadata:
        with fsspec.open(item_metadata) as src:
            metadata = yaml.safe_load(src.read())
    else:
        metadata = {}

    item_id = item_id or metadata.get("id", default_id)
    item = create_stac_item(
        item_id=item_id,
        item_metadata=metadata,
        tile_pyramid=tile_pyramid,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        item_basepath=item_basepath or default_basepath,
        alternative_basepath=alternative_basepath,
        self_href=self_href,
        thumbnail_href=thumbnail_href,
        relative_paths=None,
        bands_type=None,
        thumbnail_type=None,
        unit_to_meter=1,
    )
    out_json = out_path or os.path.join(default_basepath, f"{item_id}.json")
    out = item.to_dict()
    click.echo(json.dumps(out, indent=indent))
    if force or click.confirm(f"Write output to {out_json}?", abort=True):
        with fsspec.open(out_json, "w") as dst:
            dst.write(json.dumps(out, indent=indent))


def output_info(inp):
    if inp.endswith(".mapchete"):
        conf = raw_conf(inp)
        default_basepath = os.path.dirname(conf["output"]["path"].strip("/"))
        return (
            raw_conf_output_pyramid(conf),
            default_basepath,
            os.path.basename(default_basepath),
        )
    else:
        default_basepath = inp.strip("/")
        return (
            read_output_metadata(os.path.join(inp, "metadata.json"))["pyramid"],
            default_basepath,
            os.path.basename(default_basepath),
        )
