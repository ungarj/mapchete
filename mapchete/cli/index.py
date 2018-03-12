"""Create index for process output."""

import logging
from shapely import wkt
import tqdm
import yaml

import mapchete
from mapchete.config import _map_to_new_config
from mapchete.index import zoom_index_gen
from mapchete.tile import BufferedTilePyramid


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

    if not any([args.geojson, args.gpkg, args.txt]):
        raise ValueError(
            "one of 'geojson', 'gpkg', or 'txt' must be provided")

    # process single tile
    if args.tile:
        conf = _map_to_new_config(
            yaml.load(open(args.mapchete_file, "r").read()))
        tile = BufferedTilePyramid(
            conf["pyramid"]["grid"],
            metatiling=conf["pyramid"].get("metatiling", 1),
            pixelbuffer=conf["pyramid"].get("pixelbuffer", 0)
        ).tile(*args.tile)
        with mapchete.open(
            args.mapchete_file, mode="readonly", bounds=tile.bounds,
            zoom=tile.zoom
        ) as mp:
            out_dir = args.out_dir if args.out_dir else mp.config.output.path
            for tile in tqdm.tqdm(
                zoom_index_gen(
                    mp=mp,
                    zoom=tile.zoom,
                    out_dir=out_dir,
                    geojson=args.geojson,
                    gpkg=args.gpkg,
                    txt=args.txt,
                    fieldname=args.fieldname,
                    basepath=args.basepath,
                    for_gdal=args.for_gdal),
                total=mp.count_tiles(tile.zoom, tile.zoom),
                unit="tile",
                disable=args.debug
            ):
                logger.debug(tile)

    else:
        if args.wkt_geometry:
            bounds = wkt.loads(args.wkt_geometry).bounds
        else:
            bounds = args.bounds
        with mapchete.open(
            args.mapchete_file, mode="readonly", zoom=args.zoom, bounds=bounds
        ) as mp:
            out_dir = args.out_dir if args.out_dir else mp.config.output.path
            logger.debug("process bounds: %s", mp.config.init_bounds)
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
                        gpkg=args.gpkg,
                        txt=args.txt,
                        fieldname=args.fieldname,
                        basepath=args.basepath,
                        for_gdal=args.for_gdal),
                    total=mp.count_tiles(z, z),
                    unit="tile",
                    disable=args.debug
                ):
                    logger.debug(tile)
