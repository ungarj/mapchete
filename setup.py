#!/usr/bin/env python

"""Install Mapchete."""

from setuptools import setup
import os

# don't install dependencies when building win readthedocs
on_rtd = os.environ.get('READTHEDOCS') == 'True'

# get version number
# from https://github.com/mapbox/rasterio/blob/master/setup.py#L55
with open('mapchete/__init__.py') as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            continue

setup(
    name='mapchete',
    version=version,
    description='tile-based geodata processing',
    author='Joachim Ungar',
    author_email='joachim.ungar@gmail.com',
    url='https://github.com/ungarj/mapchete',
    license='MIT',
    packages=[
        'mapchete',
        'mapchete.cli',
        'mapchete.commons',
        'mapchete.formats',
        'mapchete.formats.default',
        'mapchete.io',
        'mapchete.static'
    ],
    entry_points={
        'console_scripts': [
            'mapchete=mapchete.cli.main:main'
        ],
        'mapchete.formats.drivers': [
            'geojson=mapchete.formats.default.geojson',
            'gtiff=mapchete.formats.default.gtiff',
            'mapchete_input=mapchete.formats.default.mapchete_input',
            'png_hillshade=mapchete.formats.default.png_hillshade',
            'png=mapchete.formats.default.png',
            'raster_file=mapchete.formats.default.raster_file',
            'vector_file=mapchete.formats.default.vector_file',
            'tile_directory=mapchete.formats.default.tile_directory'
        ]
    },
    package_dir={'static': 'static'},
    package_data={'mapchete.static': ['*']},
    install_requires=[
        'tilematrix>=0.12',
        'fiona',
        'pyyaml',
        'flask',
        'rasterio>=1.0a12',
        'cached_property',
        'pyproj',
        'cachetools',
        'tqdm'
    ] if not on_rtd else [],
    extra_require={'contours': ['matplotlib']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'pytest-flask']
)
