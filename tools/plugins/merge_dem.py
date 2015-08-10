#!/usr/bin/env python


def config_subparser(merge_dem_parser):

    merge_dem_parser.add_argument("--raster", required=True, nargs=1, type=str, dest="input_files")


def process():

    '''
    
    # TODO: default to union of input DEM bounding boxes; optional command
    # line parameter
    bounding_box = aster_envelope

    # Intersect bounding box with coastline dataset to determine footprint.
    with fiona.open(coastline_path, 'r') as coastline:
        geometries = []
        print "creating footprint using coastline ..."
        for feature in coastline:
            geometry = shape(feature['geometry'])
            if geometry.intersects(bounding_box):
                intersect = geometry.intersection(bounding_box)
                geometries.append(intersect)
        footprint = cascaded_union(geometries)
        print "done."

    # Get metatiles to be processed by footprint.
    metatiles = wgs84_meta.tiles_from_geom(footprint, zoom)
    #tiles = [(2150, 541)]
    #print "%s tiles to be processed" %(str(len(tiles)))
    zoomstring = "zoom%s" %(str(zoom))

    if debug:
        ## Write debug output.
        print "write tiling debug file ..."
        tiled_out_filename = zoomstring + ".geojson"
        tiled_out_path = os.path.join(output_folder, tiled_out_filename)
        schema = {
            'geometry': 'Polygon',
            'properties': {'col': 'int', 'row': 'int', 'metatile': 'str'}
        }
        try:
            os.remove(tiled_out_path)
        except:
            pass
        with fiona.open(tiled_out_path, 'w', 'GeoJSON', schema) as sink:
            for metatile in metatiles:
                zoom, col, row = metatile
                tiles = wgs84_meta.tiles_from_tilematrix(zoom, col, row, footprint)
                for tile in tiles:
                    zoom, col, row = tile
                    feature = {}
                    feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
                    feature['properties'] = {}
                    feature['properties']['col'] = col
                    feature['properties']['row'] = row
                    feature['properties']['metatile'] = str(metatile)
                    sink.write(feature)
        print "done."

    # Do the processing.
    for metatile in metatiles:
        # resample_dem(metatile, footprint, zoom)
        zoom, col, row = metatile
        tiles = wgs84_meta.tiles_from_tilematrix(zoom, col, row, footprint)

        metadata, rasterdata = read_raster_window(raster_file, wgs84_meta, metatile,
            pixelbuffer=5)

        out_metatile_folder = os.path.join(output_folder+"/metatile", zoomstring)
        metatile_name = "%s%s.tif" %(col, row)
        out_metatile = os.path.join(out_metatile_folder, metatile_name)
        if not os.path.exists(out_metatile_folder):
            os.makedirs(out_metatile_folder)
        try:
            os.remove(out_metatile)
        except:
            pass

        write_raster_window(out_metatile, wgs84_meta, metatile, metadata,
            rasterdata, pixelbuffer=5)

        for tile in tiles:
            zoom, col, row = tile
    
            tileindex = zoom, col, row
    
            out_tile_folder = os.path.join(output_folder, zoomstring)
            tile_name = "%s%s.tif" %(col, row)
            out_tile = os.path.join(out_tile_folder, tile_name)
            if not os.path.exists(out_tile_folder):
                os.makedirs(out_tile_folder)
            try:
                os.remove(out_tile)
            except:
                pass
    
            if isinstance(rasterdata, np.ndarray):
                write_raster_window(out_tile, wgs84, tileindex, metadata,
                    rasterdata, pixelbuffer=0)
            else:
                print "empty!"
    '''