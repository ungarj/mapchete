import logging
import os

import click
import tqdm

from mapchete.cli import options
from mapchete.cli.progress_bar import PBar
from mapchete.path import MPath

logger = logging.getLogger(__name__)


@click.command(help="Sync between paths.")
@options.arg_path
@options.arg_out_path
@options.opt_src_fs_opts
@options.opt_dst_fs_opts
@click.option(
    "--chunksize",
    type=click.INT,
    default=1024 * 1024,
    show_default=True,
    help="Read and write chunk size in bytes.",
)
@click.option(
    "--compare-checksums",
    is_flag=True,
    help="Calculate checksums of objects. WARNING: this will effectively read all of the data (source and destination)!",
)
@options.opt_debug
def sync(
    path: MPath,
    out_path: MPath,
    debug: bool = False,
    chunksize: int = 1024 * 1024,
    compare_checksums: bool = False,
    **_,
):
    try:
        if path.is_directory():
            tqdm.tqdm.write(f"sync {path} to {out_path} ...")
            for contents in path.walk(absolute_paths=True):
                dst_root = out_path / os.path.relpath(
                    str(contents.root.without_protocol()),
                    start=str(path.without_protocol()),
                )
                try:
                    dst_files = set([file.name for file in dst_root.ls()])
                except FileNotFoundError:
                    dst_files = set()
                for src_file in tqdm.tqdm(contents.files, desc="files", leave=False):
                    dst_file = out_path / os.path.relpath(
                        str(src_file.without_protocol()),
                        start=str(path.without_protocol()),
                    )
                    if (
                        src_file.name not in dst_files
                        or (  # file does not exist on destination
                            src_file.checksum()
                            != dst_file.checksum()  # file contents are not identical
                            if compare_checksums
                            else src_file.size() != dst_file.size()  # file sizes differ
                        )
                    ):
                        with PBar(
                            total=100,
                            disable=debug,
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1024,
                            # bar_format="{percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt}|{elapsed}<{remaining}",
                            desc=src_file.name,
                        ) as pbar:
                            src_file.cp(
                                dst_file,
                                overwrite=True,
                                chunksize=chunksize,
                                observers=[pbar],
                            )
        else:  # pragma: no cover
            raise NotImplementedError()
    except Exception as exc:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(str(exc))
