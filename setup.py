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
        'mapchete.io_utils',
        'static',
        'cli'
    ],
    entry_points={
        'console_scripts': [
            'mapchete_execute = cli.execute:main',
            'mapchete_serve = cli.serve:main',
            'raster2pyramid = cli.pyramid:main',
            'mapchete = cli.main:main'
        ],
    },
    package_dir={'static': 'static'},
    package_data={'static':['index.html', 'process_template.py',
        'mapchete_template.mapchete']},
    install_requires=[
        'tilematrix',
        'fiona',
        'pyyaml',
        'flask',
        'Pillow',
        'scipy',
        'psycopg2',
        # 'blosc==1.3.2',
        # 'bloscpack==0.10.0',
        'rasterio>=0.36.0',
        'matplotlib'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ]
)
