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


def _parse_requirements(file):
    return sorted(set(
        line.partition("#")[0].strip()
        for line in open(os.path.join(os.path.dirname(__file__), file))
    ) - set(""))

# dependencies for extra features
req_contours = ["matplotlib"]
req_s3 = ["boto3"]
req_serve = ["flask"]
req_vrt = ["lxml"]
req_complete = req_contours + req_s3 + req_serve + req_vrt

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
        "console_scripts": [
            "mapchete=mapchete.cli.main:main"
        ],
        "mapchete.cli.commands": [
            "convert=mapchete.cli.default.convert:convert",
            "create=mapchete.cli.default.create:create",
            "execute=mapchete.cli.default.execute:execute",
            "formats=mapchete.cli.default.formats:formats",
            "index=mapchete.cli.default.index:index",
            "processes=mapchete.cli.default.processes:processes",
            "serve=mapchete.cli.default.serve:serve",
        ],
        "mapchete.formats.drivers": [
            "geojson=mapchete.formats.default.geojson",
            "gtiff=mapchete.formats.default.gtiff",
            "mapchete_input=mapchete.formats.default.mapchete_input",
            "png_hillshade=mapchete.formats.default.png_hillshade",
            "png=mapchete.formats.default.png",
            "raster_file=mapchete.formats.default.raster_file",
            "vector_file=mapchete.formats.default.vector_file",
            "tile_directory=mapchete.formats.default.tile_directory"
        ],
        "mapchete.processes": [
            "example_process=mapchete.processes.examples.example_process",
            "contours=mapchete.processes.contours",
            "convert=mapchete.processes.convert",
            "hillshade=mapchete.processes.hillshade"
        ]
    },
    package_dir={"static": "static"},
    package_data={"mapchete.static": ["*"]},
    install_requires=_parse_requirements("requirements.txt") if not on_rtd else [],
    extras_require={
        "complete": req_complete,
        "contours": req_contours,
        "s3": req_s3,
        "serve": req_serve,
        "vrt": req_vrt,
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
