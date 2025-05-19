import logging
import os
from typing import Generator, Tuple

import click
import tqdm

from mapchete.cli import options
from mapchete.cli.progress_bar import PBar
from mapchete.enums import Concurrency
from mapchete.executor import get_executor
from mapchete.path import MPath
from mapchete.pretty import pretty_bytes
from mapchete.timer import Timer

logger = logging.getLogger(__name__)


@click.command(help="Sync between paths.")
@options.arg_path
@options.arg_out_path
@options.opt_src_fs_opts
@options.opt_dst_fs_opts
@click.option(
    "--chunksize",
    type=click.IntRange(min=1),
    default=1024 * 1024,
    show_default=True,
    help="Read and write chunk size in bytes.",
)
@click.option(
    "--compare-checksums",
    is_flag=True,
    help="Calculate checksums of objects. WARNING: this will effectively read all of the data (source and destination)!",
)
@options.opt_workers
@options.opt_debug
@options.opt_verbose
def sync(
    path: MPath,
    out_path: MPath,
    chunksize: int = 1024 * 1024,
    compare_checksums: bool = False,
    workers: int = 1,
    debug: bool = False,
    verbose: bool = False,
    **_,
):
    try:
        if path.is_directory():
            out_path.makedirs()
            with get_executor(
                concurrency=Concurrency.none if workers == 1 else Concurrency.threads,
                max_workers=workers,
            ) as executor:
                for future in tqdm.tqdm(
                    executor.as_completed(
                        sync_file,
                        _files_skip(
                            path, out_path, compare_checksums=compare_checksums
                        ),
                        fargs=None,
                        fkwargs=dict(chunksize=chunksize, debug=debug),
                        item_skip_bool=True,
                        max_submitted_tasks=workers * 10,
                    ),
                    desc="files",
                ):
                    if future.skipped:  # pragma: no cover
                        src, _ = future.result()
                        if verbose:
                            tqdm.tqdm.write(f"[SKIPPED] {str(src)}: {future.skip_info}")
                    else:
                        (src, dst), duration = future.result()
                        if verbose:
                            tqdm.tqdm.write(
                                f"[OK] {str(src)}: copied to {str(dst)} in {duration}"
                            )
        else:  # pragma: no cover
            raise NotImplementedError()
    except Exception as exc:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(str(exc))


def _files_skip(
    src_dir: MPath, dst_dir: MPath, compare_checksums: bool = False
) -> Generator[Tuple[Tuple[MPath, MPath], bool, str], None, None]:
    for contents in src_dir.walk(absolute_paths=True):
        dst_root = dst_dir / os.path.relpath(
            str(contents.root.without_protocol()),
            start=str(src_dir.without_protocol()),
        ).rstrip(".")
        try:
            dst_files = set([file.name for file in dst_root.ls()])
        except FileNotFoundError:
            dst_files = set()
        for src_file in contents.files:
            dst_file = dst_dir / os.path.relpath(
                str(src_file.without_protocol()),
                start=str(src_dir.without_protocol()),
            )
            if src_file.name in dst_files:  # pragma: no cover
                if (
                    src_file.checksum()
                    == dst_file.checksum()  # file contents are not identical
                    if compare_checksums
                    else src_file.size() == dst_file.size()  # file sizes differ
                ):
                    out = (src_file, dst_file), True, "already synced"
                else:
                    out = (src_file, dst_file), False, ""
            else:
                out = (src_file, dst_file), False, ""
            yield out


def sync_file(
    paths: Tuple[MPath, MPath], chunksize: int = 1024 * 1024, debug: bool = False
) -> Tuple[Tuple[MPath, MPath], str]:
    src_file, dst_file = paths
    with PBar(
        total=100,
        disable=debug,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=str(src_file),
        leave=False,
        print_messages=False,
    ) as pbar:
        with Timer() as duration:
            src_file.cp(
                dst_file,
                overwrite=True,
                chunksize=chunksize,
                observers=[pbar],
            )
    return paths, f"{duration} ({pretty_bytes(src_file.size() / duration.elapsed)}/s)"
