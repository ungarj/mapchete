#!/usr/bin/env python

"""Install Mapchete."""

from setuptools import setup
import os

on_rtd = os.environ.get('READTHEDOCS') == 'True'

setup(
    name='mapchete',
    version='0.7',
    description='tile-based geodata processing',
    author='Joachim Ungar',
    author_email='joachim.ungar@gmail.com',
    url='https://github.com/ungarj/mapchete',
    license='MIT',
    packages=[
        'mapchete',
        'mapchete.cli',
        'mapchete.commons',
        'mapchete.config',
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
            'vector_file=mapchete.formats.default.vector_file'
        ]
    },
    package_dir={'static': 'static'},
    package_data={'mapchete.static': ['*']},
    install_requires=[
        'tilematrix>=0.6',
        'fiona',
        'pyyaml',
        'flask',
        'Pillow',
        'rasterio>=1.0a2',
        'matplotlib',
        'gdal',
        'cached_property',
        'pyproj',
        'cachetools',
        'tqdm'
    ] if not on_rtd else [],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'pytest-flask']
)
