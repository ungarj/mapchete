#!/usr/bin/env python

from setuptools import setup

setup(
    name='mapchete',
    version='0.3',
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
        'mapchete.io',
        'mapchete.logging',
        'mapchete.static'
    ],
    entry_points={
        'console_scripts': [
            'mapchete = mapchete.cli.main:MapcheteCLI'
        ],
    },
    package_dir={'static': 'static'},
    package_data={'static': [
        'index.html', 'process_template.py', 'mapchete_template.mapchete']},
    install_requires=[
        'tilematrix>=0.4',
        'fiona',
        'pyyaml',
        'flask',
        'Pillow',
        'scipy',
        'rasterio>=0.36.0',
        'matplotlib',
        'gdal',
        'cached_property'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ]
)
