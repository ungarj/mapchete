import logging
import os

import click
import tqdm

from mapchete.cli import options
from mapchete.cli.progress_bar import PBar
from mapchete.path import MPath

logger = logging.getLogger(__name__)


@click.command(help="copy path.")
@options.arg_path
@options.arg_out_path
@options.opt_src_fs_opts
@options.opt_dst_fs_opts
@options.opt_overwrite
@options.opt_recursive
@click.option("--skip-existing", is_flag=True, help="Skip file if already exists.")
@click.option(
    "--chunksize",
    type=click.INT,
    default=1024 * 1024,
    show_default=True,
    help="Read and write chunk size in bytes.",
)
@options.opt_debug
def cp(
    path: MPath,
    out_path: MPath,
    overwrite: bool = False,
    recursive: bool = False,
    skip_existing: bool = False,
    debug: bool = False,
    chunksize: int = 1024 * 1024,
    **_,
):
    try:
        if path.is_directory():
            if not recursive:  # pragma: no cover
                raise click.UsageError(
                    "source path is directory, --recursive flag required"
                )
            for contents in tqdm.tqdm(path.walk(), disable=debug):
                for src_file in tqdm.tqdm(
                    contents.files,
                    leave=False,
                    disable=debug,
                ):
                    dst_file = out_path / os.path.relpath(
                        str(src_file.without_protocol()),
                        start=str(path.without_protocol()),
                    )
                    message = f"copy {str(src_file)} to {str(dst_file)} ..."
                    if debug:  # pragma: no cover
                        logger.debug(message)
                    else:
                        tqdm.tqdm.write(message)
                    with PBar(
                        total=100,
                        desc="chunks",
                        disable=debug,
                        leave=False,
                        bar_format="{percentage:3.0f}%|{bar}|{elapsed}<{remaining}",
                    ) as pbar:
                        src_file.cp(
                            dst_file,
                            overwrite=overwrite,
                            exists_ok=skip_existing,
                            observers=[pbar],
                            chunksize=chunksize,
                        )
        else:
            with PBar(
                total=100,
                disable=debug,
                bar_format="{percentage:3.0f}%|{bar}|{elapsed}<{remaining}",
            ) as pbar:
                path.cp(
                    out_path,
                    overwrite=overwrite,
                    exists_ok=skip_existing,
                    observers=[pbar],
                    chunksize=chunksize,
                )
    except Exception as exc:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(str(exc))
