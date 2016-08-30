#!/usr/bin/env python

import os
import fiona
from shapely.geometry import shape, mapping
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

from .io_funcs import reproject_geometry, clean_geometry_type

def read_vector(
    process,
    input_file,
    pixelbuffer=0
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
            pixelbuffer=pixelbuffer
        )
    else:
        features = None

    return features

def read_vector_window(
    input_file,
    tile,
    pixelbuffer=0
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

    with fiona.open(input_file, 'r') as vector:
        tile_bbox = tile.bbox(pixelbuffer=pixelbuffer)
        vector_crs = CRS(vector.crs)

        # Reproject tile bounding box to source file CRS for filter:
        if vector_crs != tile.crs:
            bbox = tile.bbox(pixelbuffer=pixelbuffer)
            tile_bbox = reproject_geometry(bbox, src_crs=tile.crs, dst_crs=vector_crs)

        for feature in vector.filter(bbox=tile_bbox.bounds):
            feature_geom = shape(feature['geometry'])
            if not feature_geom.is_valid:
                warnings.warn("invalid geometry found in vector input file")
                continue
            geom = clean_geometry_type(
                feature_geom.intersection(tile_bbox),
                feature_geom.geom_type
            )
            if geom:
                # Reproject each feature to tile CRS
                if vector_crs != tile.crs:
                    geom = reproject_geometry(
                        geom,
                        src_crs=vector_crs,
                        dst_crs=tile.crs)
                yield {
                    'properties': feature['properties'],
                    'geometry': mapping(geom)
                }

def write_vector(
    process,
    metadata,
    data,
    pixelbuffer=0,
    overwrite=False
    ):
    assert isinstance(metadata["output"].schema, dict)
    assert isinstance(metadata["output"].driver, str)
    assert isinstance(data, list)

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

        for feature in data:
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
                data,
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
    data,
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
        for feature in data:
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
                warnings.warn("failed on geometry")

            dst.write(feature)
