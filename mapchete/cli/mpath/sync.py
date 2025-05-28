import logging
import os
from typing import Generator, Tuple

import click
import click_spinner
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
@click.option(
    "--count",
    is_flag=True,
    help="Count all files from source path. WARNING: this will trigger more requests on S3.",
)
@options.opt_workers
@options.opt_debug
@options.opt_verbose
def sync(
    path: MPath,
    out_path: MPath,
    chunksize: int = 1024 * 1024,
    compare_checksums: bool = False,
    count: bool = False,
    workers: int = 1,
    debug: bool = False,
    verbose: bool = False,
    **_,
):
    try:
        if path.is_directory():
            out_path.makedirs()
            if count:
                tqdm.tqdm.write("counting files ...")
                with Timer() as duration:
                    with click_spinner.Spinner(disable=debug):
                        total = 0
                        size = 0
                        for page in path.paginate():
                            total += len(page)
                            for file in page:
                                size += file.size()
                msg = f"{str(path)}: contains {total} file(s) totalling {pretty_bytes(size)} and counted in {duration}"
                tqdm.tqdm.write(msg)
                logger.debug(msg)
            else:
                total = None

            with get_executor(
                concurrency=Concurrency.none if workers == 1 else Concurrency.threads,
                max_workers=workers,
            ) as executor:
                for future in tqdm.tqdm(
                    executor.as_completed(
                        sync_file,
                        check_files(
                            path, out_path, compare_checksums=compare_checksums
                        ),
                        fargs=None,
                        fkwargs=dict(chunksize=chunksize, debug=debug),
                        item_skip_bool=True,
                        max_submitted_tasks=workers * 10,
                    ),
                    disable=debug,
                    desc="files",
                    total=total,
                ):
                    if future.skipped:  # pragma: no cover
                        src, _ = future.result()
                        msg = f"[SKIPPED] {str(src)}: {future.skip_info}"
                    else:
                        (src, dst), duration = future.result()
                        msg = f"[OK] {str(src)}: copied to {str(dst)} in {duration}"
                    if verbose:
                        tqdm.tqdm.write(msg)
                    logger.debug(msg)
        else:  # pragma: no cover
            raise NotImplementedError()
    except Exception as exc:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(str(exc))


def check_files(
    src_dir: MPath, dst_dir: MPath, compare_checksums: bool = False
) -> Generator[Tuple[Tuple[MPath, MPath], bool, str], None, None]:
    for contents in src_dir.walk(absolute_paths=True):
        dst_root = dst_dir / os.path.relpath(
            str(contents.root.without_protocol()),
            start=str(src_dir.without_protocol()),
        ).rstrip(".")
        try:
            # make sure to keep destination MPath objects to avoid unnecessary
            # HEAD calls
            existing_dst_files = {file.name: file for file in dst_root.ls()}
        except FileNotFoundError:
            existing_dst_files = dict()
        for src_file in contents.files:
            if src_file.name in existing_dst_files:  # pragma: no cover
                dst_file = existing_dst_files[src_file.name]
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
                dst_file = dst_dir / os.path.relpath(
                    str(src_file.without_protocol()),
                    start=str(src_dir.without_protocol()),
                )
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
