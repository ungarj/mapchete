#!/usr/bin/env python

import os
import fiona
from shapely.geometry import shape, mapping, box
from geoalchemy2.shape import from_shape
from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    and_
    )
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
from rasterio.crs import CRS
import warnings
from itertools import chain
from tilematrix import clip_geometry_to_srs_bounds

from .io_funcs import reproject_geometry, clean_geometry_type

def read_vector(
    process,
    input_file,
    pixelbuffer=0,
    validity_check=True
    ):
    """
    This is a wrapper around the read_vector_window function of tilematrix.
    Tilematrix itself uses fiona to read vector data.
    This function returns a list of GeoJSON-like dictionaries containing the
    clipped vector data and attributes.
    """
    if input_file:
        features = read_vector_window(
            input_file,
            process.tile,
            pixelbuffer=pixelbuffer,
            validity_check=validity_check
        )
    else:
        features = None

    return features

def read_vector_window(
    input_file,
    tile,
    pixelbuffer=0,
    validity_check=True
    ):
    """
    Reads an input vector dataset with fiona using the tile bounding box as
    filter and clipping geometry. Returns a list of GeoJSON like features.
    """

    try:
        assert os.path.isfile(input_file)
    except:
        raise IOError("input file does not exist: %s" % input_file)

    try:
        assert pixelbuffer >= 0
    except:
        raise ValueError("pixelbuffer must be 0 or greater")

    try:
        assert isinstance(pixelbuffer, int)
    except:
        raise ValueError("pixelbuffer must be an integer")

    # Check if potentially tile boundaries exceed tile matrix boundaries on
    # the antimeridian, the northern or the southern boundary.
    tile_left, tile_bottom, tile_right, tile_top = tile.bounds(pixelbuffer)
    touches_left = tile_left <= tile.tile_pyramid.left
    touches_bottom = tile_bottom <= tile.tile_pyramid.bottom
    touches_right = tile_right >= tile.tile_pyramid.right
    touches_top = tile_top >= tile.tile_pyramid.top
    is_on_edge = touches_left or touches_bottom or touches_right or touches_top
    if pixelbuffer and is_on_edge:
        tile_boxes = clip_geometry_to_srs_bounds(
            tile.bbox(pixelbuffer),
            tile.tile_pyramid,
            multipart=True
            )
        return chain.from_iterable(
            _get_reprojected_features(
                input_file=input_file,
                dst_bounds=bbox.bounds,
                dst_crs=tile.crs,
                validity_check=validity_check
                )
            for bbox in tile_boxes
            )
        for polygon in tile_boxes:
            print polygon

    else:
        return _get_reprojected_features(
            input_file=input_file,
            dst_bounds=tile.bounds(pixelbuffer),
            dst_crs=tile.crs,
            validity_check=validity_check
            )


def _get_reprojected_features(
    input_file=None,
    dst_bounds=None,
    dst_crs=None,
    validity_check=None
    ):
    assert isinstance(input_file, str)
    assert isinstance(dst_bounds, tuple)
    assert isinstance(dst_crs, CRS)
    assert isinstance(validity_check, bool)

    with fiona.open(input_file, 'r') as vector:
        vector_crs = CRS(vector.crs)
        # Reproject tile bounding box to source file CRS for filter:
        if vector_crs == dst_crs:
            dst_bbox = box(*dst_bounds)
        else:
            dst_bbox = reproject_geometry(
                box(*dst_bounds),
                src_crs=dst_crs,
                dst_crs=vector_crs,
                validity_check=True
                )
        for feature in vector.filter(bbox=dst_bbox.bounds):
            feature_geom = shape(feature['geometry'])
            if not feature_geom.is_valid:
                try:
                    feature_geom = feature_geom.buffer(0)
                    assert feature_geom.is_valid
                    warnings.warn(
                        "fixed invalid vector input geometry"
                        )
                except AssertionError:
                    warnings.warn(
                        "irreparable geometry found in vector input file"
                        )
                    continue
            geom = clean_geometry_type(
                feature_geom.intersection(dst_bbox),
                feature_geom.geom_type
            )
            if geom:
                # Reproject each feature to tile CRS
                if vector_crs == dst_crs and validity_check:
                    assert geom.is_valid
                else:
                    try:
                        geom = reproject_geometry(
                            geom,
                            src_crs=vector_crs,
                            dst_crs=dst_crs,
                            validity_check=validity_check
                            )
                    except ValueError:
                        warnings.warn("feature reprojection failed")
                yield {
                    'properties': feature['properties'],
                    'geometry': mapping(geom)
                }


def write_vector(
    process,
    metadata,
    features,
    pixelbuffer=0,
    overwrite=False
    ):
    assert isinstance(metadata["output"].schema, dict)
    assert isinstance(metadata["output"].driver, str)
    assert isinstance(features, list)

    if process.output.is_db:

        # connect to db
        db_url = 'postgresql://%s:%s@%s:%s/%s' %(
            metadata["output"].db_params["user"],
            metadata["output"].db_params["password"],
            metadata["output"].db_params["host"],
            metadata["output"].db_params["port"],
            metadata["output"].db_params["db"]
        )
        engine = create_engine(db_url, poolclass=NullPool)
        meta = MetaData()
        meta.reflect(bind=engine)
        TargetTable = Table(
            metadata["output"].db_params["table"],
            meta,
            autoload=True,
            autoload_with=engine
        )
        Session = sessionmaker(bind=engine)
        session = Session()

        if overwrite:
            delete_old = TargetTable.delete(and_(
                TargetTable.c.zoom == process.tile.zoom,
                TargetTable.c.row == process.tile.row,
                TargetTable.c.col == process.tile.col)
                )
            session.execute(delete_old)

        for feature in features:
            try:
                geom = from_shape(
                    shape(feature["geometry"]).intersection(
                        process.tile.bbox(pixelbuffer=pixelbuffer)
                    ),
                    srid=process.tile.srid
                )
                # else:
                #     continue
            except Exception as e:
                warnings.warn("corrupt geometry: %s" %(e))
                continue

            properties = {}
            properties.update(
                zoom=process.tile.zoom,
                row=process.tile.row,
                col=process.tile.col,
                geom=geom
            )
            properties.update(feature["properties"])

            insert = TargetTable.insert().values(properties)
            session.execute(insert)

        session.commit()
        session.close()
        engine.dispose()

    else:
        process.tile.prepare_paths()

        if process.tile.exists():
            os.remove(process.tile.path)

        try:
            write_vector_window(
                process.tile.path,
                process.tile,
                metadata,
                features,
                pixelbuffer=pixelbuffer
            )
        except:
            if process.tile.exists():
                os.remove(process.tile.path)
            raise

def write_vector_window(
    output_file,
    tile,
    metadata,
    features,
    pixelbuffer=0):
    """
    Writes GeoJSON-like objects to GeoJSON.
    """
    try:
        assert pixelbuffer >= 0
    except:
        raise ValueError("pixelbuffer must be 0 or greater")

    try:
        assert isinstance(pixelbuffer, int)
    except:
        raise ValueError("pixelbuffer must be an integer")
    with fiona.open(
        output_file,
        'w',
        schema=metadata["output"].schema,
        driver=metadata["output"].driver,
        crs=tile.crs.to_dict()
        ) as dst:
        for feature in features:
            # clip with bounding box
            try:
                feature_geom = shape(feature["geometry"])
                clipped = feature_geom.intersection(
                    tile.bbox(pixelbuffer)
                )
                out_geom = clipped
                target_type = metadata["output"].schema["geometry"]
                if clipped.geom_type != target_type:
                    cleaned = clean_geometry_type(clipped, target_type)
                    out_geom = cleaned
                # write output
                if out_geom:
                    feature.update(
                        geometry=mapping(out_geom)
                    )
                    dst.write(feature)
            except ValueError:
                warnings.warn("failed geometry cleaning during writing")
