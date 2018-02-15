#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import logging
from multiprocessing import cpu_count
import tqdm
import yaml

import mapchete
from mapchete.errors import MapcheteConfigError
from mapchete.tile import BufferedTilePyramid

class _TqdmLoggingHandler(logging.Handler):
    """
    Progress bar logging handler.

    This handler just passes on log messages and uses its own write() function
    to print to stdout so that progress bar would not be interrupted.
    """

    def __init__(self, level=logging.NOTSET):
        super(self.__class__, self).__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)


# set root logger to CRITICAL
logging.getLogger().setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
# use custom progress bar handler to print log output
tqdm_logger = _TqdmLoggingHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
tqdm_logger.setFormatter(formatter)
logging.getLogger().addHandler(tqdm_logger)


def main(args=None):
    """Execute a Mapchete process."""
    parsed = args
    multi = parsed.multi if parsed.multi else cpu_count()
    mode = "overwrite" if parsed.overwrite else "continue"

    if parsed.debug:
        logging.getLogger("mapchete").setLevel(logging.DEBUG)
    else:
        logging.getLogger("mapchete").setLevel(logging.INFO)

    # process single tile
    if parsed.tile:
        conf = yaml.load(open(parsed.mapchete_file, "r").read())
        if "output" not in conf:
            raise MapcheteConfigError("output definition missing")
        if "pyramid" not in conf or "grid" not in conf["pyramid"]:
            raise MapcheteConfigError("pyramid definition missing")

        tile = BufferedTilePyramid(
            conf["pyramid"]["grid"],
            metatiling=conf["pyramid"].get("metatiling", 1),
            pixelbuffer=conf["pyramid"].get("pixelbuffer", 0)
        ).tile(*parsed.tile)

        with mapchete.open(
            parsed.mapchete_file, mode=mode, bounds=tile.bounds,
            zoom=tile.zoom, single_input_file=parsed.input_file,
            debug=parsed.debug
        ) as mp:
            for restult in mp.batch_processor(tile=parsed.tile):
                pass

    # initialize and run process
    else:
        with mapchete.open(
            parsed.mapchete_file, bounds=parsed.bounds, zoom=parsed.zoom,
            mode=mode, single_input_file=parsed.input_file, debug=parsed.debug
        ) as mp:
            for result in tqdm.tqdm(
                mp.batch_processor(multi=multi, zoom=parsed.zoom),
                total=mp.count_tiles(
                    min(mp.config.init_zoom_levels),
                    max(mp.config.init_zoom_levels)),
                unit="tile"
            ):
                if result["success"] is True:
                    logger.info((
                        result["process_tile"].id, result["message"]))
                else:
                    logger.error((
                        result["process_tile"].id, result["message"]))
