import click

from mapchete.cli import utils


@click.command(help="Create STAC asset metadata.")
@utils.arg_input
@utils.opt_out_path
def stac(input_, out_path=None):
    pass
