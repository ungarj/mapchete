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

# use README.rst for project long_description
with open('README.rst') as f:
    readme = f.read()

setup(
    name='mapchete',
    version=version,
    description='tile-based geodata processing',
    long_description=readme,
    author='Joachim Ungar',
    author_email='joachim.ungar@gmail.com',
    url='https://github.com/ungarj/mapchete',
    license='MIT',
    packages=[
        'mapchete',
        'mapchete.cli',
        'mapchete.cli.default',
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
        'mapchete.cli.commands': [
            'create=mapchete.cli.default.create:create',
            'execute=mapchete.cli.default.execute:execute',
            'formats=mapchete.cli.default.formats:formats',
            'index=mapchete.cli.default.index:index',
            'processes=mapchete.cli.default.processes:processes',
            'pyramid=mapchete.cli.default.pyramid:pyramid',
            'serve=mapchete.cli.default.serve:serve',
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
        ],
        'mapchete.processes': [
            'example_process=mapchete.processes.examples.example_process',
            'tilify=mapchete.processes.pyramid.tilify',
            'convert=mapchete.processes.convert'
        ]
    },
    package_dir={'static': 'static'},
    package_data={'mapchete.static': ['*']},
    install_requires=[
        'cachetools',
        'cached_property',
        'click>=7.0.0',
        'click-plugins',
        'click-spinner',
        'fiona>=1.8b1',
        'pyproj',
        'pyyaml',
        'retry',
        'rasterio>=1.0.2',
        'tilematrix>=0.18',
        'tqdm'
    ] if not on_rtd else [],
    extras_require={
        'contours': ['matplotlib'],
        's3': ['boto3'],
        'serve': ['flask'],
        'vrt': ['lxml']
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'pytest-flask']
)
