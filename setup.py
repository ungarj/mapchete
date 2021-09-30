"""Install Mapchete."""

import os
from setuptools import find_packages, setup

# don't install dependencies when building win readthedocs
on_rtd = os.environ.get("READTHEDOCS") == "True"

# get version number
# from https://github.com/mapbox/rasterio/blob/master/setup.py#L55
with open("mapchete/__init__.py") as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip().strip('"').strip("'")
            break

# use README.rst for project long_description
with open("README.rst") as f:
    readme = f.read()


# dependencies for extra features
install_requires = [
    "cachetools",
    "cached_property",
    "click>=7.1.1,<8",
    "click-plugins",
    "click-spinner",
    "fiona>=1.8.13.post1",
    "fsspec",
    "importlib-metadata",
    "importlib-resources",
    "numpy>=1.16",
    "oyaml",
    "pyproj",
    "retry",
    "rasterio>=1.0.28,<1.2.7",
    "shapely",
    "tilematrix>=0.20",
    "tqdm",
]
req_contours = ["matplotlib"]
req_dask = ["dask", "distributed"]
req_geobuf = ["geobuf"]
req_http = ["fsspec[http]", "aiohttp", "requests"]
req_s3 = ["boto3", "fsspec[s3]", "s3fs>=0.5.1"]
req_serve = ["flask", "werkzeug>=0.15"]
req_stac = ["pystac"]
req_vrt = ["lxml"]
req_complete = (
    req_contours
    + req_dask
    + req_geobuf
    + req_http
    + req_s3
    + req_serve
    + req_stac
    + req_vrt
)

setup(
    name="mapchete",
    version=version,
    description="Tile-based geodata processing using rasterio & Fiona",
    long_description=readme,
    author="Joachim Ungar",
    author_email="joachim.ungar@gmail.com",
    url="https://github.com/ungarj/mapchete",
    license="MIT",
    packages=find_packages(),
    entry_points={
        "console_scripts": ["mapchete=mapchete.cli.main:main"],
        "mapchete.cli.commands": [
            "convert=mapchete.cli.default.convert:convert",
            "cp=mapchete.cli.default.cp:cp",
            "create=mapchete.cli.default.create:create",
            "execute=mapchete.cli.default.execute:execute",
            "formats=mapchete.cli.default.formats:formats",
            "index=mapchete.cli.default.index:index",
            "processes=mapchete.cli.default.processes:processes",
            "rm=mapchete.cli.default.rm:rm",
            "serve=mapchete.cli.default.serve:serve",
            "stac=mapchete.cli.default.stac:stac",
        ],
        "mapchete.formats.drivers": [
            "flatgeobuf=mapchete.formats.default.flatgeobuf",
            "geobuf=mapchete.formats.default.geobuf",
            "geojson=mapchete.formats.default.geojson",
            "gtiff=mapchete.formats.default.gtiff",
            "mapchete_input=mapchete.formats.default.mapchete_input",
            "png_hillshade=mapchete.formats.default.png_hillshade",
            "png=mapchete.formats.default.png",
            "raster_file=mapchete.formats.default.raster_file",
            "vector_file=mapchete.formats.default.vector_file",
            "tile_directory=mapchete.formats.default.tile_directory",
        ],
        "mapchete.processes": [
            "example_process=mapchete.processes.examples.example_process",
            "contours=mapchete.processes.contours",
            "convert=mapchete.processes.convert",
            "hillshade=mapchete.processes.hillshade",
        ],
    },
    package_dir={"static": "static"},
    package_data={"mapchete.static": ["*"]},
    install_requires=install_requires,
    extras_require={
        "complete": req_complete,
        "contours": req_contours,
        "dask": req_dask,
        "geobuf": req_geobuf,
        "s3": req_s3,
        "serve": req_serve,
        "stac": req_stac,
        "vrt": req_vrt,
    },
    tests_require=[
        "coveralls",
        "flake8",
        "mapchete[complete]",
        "pytest",
        "pytest-cov",
        "pytest-flask",
        "rio-cogeo",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
