"""Create index for process output."""

import logging
import tqdm

import mapchete
from mapchete.index import zoom_index_gen


# lower stream output log level
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.ERROR)
logging.getLogger().addHandler(stream_handler)
logger = logging.getLogger(__name__)


def index(args):
    if args.debug:
        logging.getLogger("mapchete").setLevel(logging.DEBUG)
        stream_handler.setLevel(logging.DEBUG)

    if not any([args.geojson, args.shapefile, args.vrt]):
        raise ValueError(
            "one of 'geojson', 'shapefile' or 'vrt' must be provided")
    if args.vrt:
        raise NotImplementedError("writing VRTs is not yet enabled")
    logger.debug("open mapchete file")
    with mapchete.open(
        args.mapchete_file,
        mode="readonly",
        zoom=args.zoom,
        bounds=args.bounds
    ) as mp:
        out_dir = args.out_dir if args.out_dir else mp.config.output.path
        logger.debug("process bounds: %s", mp.config.bounds)
        logger.debug("process zooms: %s", mp.config.init_zoom_levels)
        logger.debug("fieldname: %s", args.fieldname)
        for z in mp.config.init_zoom_levels:
            logger.debug("zoom %s", z)
            for tile in tqdm.tqdm(
                zoom_index_gen(
                    mp=mp,
                    zoom=z,
                    out_dir=out_dir,
                    geojson=args.geojson,
                    shapefile=args.shapefile,
                    txt=args.txt,
                    vrt=args.vrt,
                    fieldname=args.fieldname,
                    overwrite=args.overwrite),
                total=mp.count_tiles(z, z),
                unit="tile",
                disable=args.debug
            ):
                logger.debug(tile)
