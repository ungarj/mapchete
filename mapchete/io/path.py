"""Module to handle local and remote paths."""
from collections import defaultdict
import concurrent.futures
import logging
import os
from urllib.request import urlopen
from urllib.error import HTTPError

from mapchete.io._misc import get_boto3_bucket


logger = logging.getLogger(__name__)

REMOTE_SCHEMES = {
    "http://": "curl",
    "https://": "curl",
    "s3://": "s3"
}


class FileSystem():
    """
    Class to abstract a file system.

    Loosely inspired by s3fs.S3FileSystem
    """

    def __init__(self, init_path=None, client_kwargs=None):
        """Initialize."""
        self.path, self.scheme, self.is_remote, self.gdal_prefix = _parse_path(init_path)

    @classmethod
    def from_path(path, init_path=None, client_kwargs=None):
        """Create FileSystem from example path."""
        return FileSystem(init_path=init_path, client_kwargs=client_kwargs)

    def open(self, path, mode="r"):
        """Open file object."""
        raise NotImplementedError()

    def makedirs(self, path):
        """
        Silently create all subdirectories of path if path is local.

        Parameters
        ----------
        path : path
        """
        if not self.is_remote:
            try:
                os.makedirs(path)
            except OSError:
                pass

    def exists(self, path, prefix=None, is_tile_directory=False):
        """Tell if path exists."""
        raise NotImplementedError()

    def paths_exist(self, paths, prefix=None, is_tile_directory=False):
        """Yield if each path exists."""
        raise NotImplementedError()


class Path():
    """
    Class to abstract local and remote paths.

    Loosely inspired by the rasterio.path.ParsedPath class.
    """

    def __init__(self, path):
        """Initialize."""
        # store original path
        self.name = path
        self.path, self.scheme, self.is_remote, self.gdal_prefix = _parse_path(path)

        for prefix, scheme in REMOTE_SCHEMES.items():
            vsi_prefix = "/vsi{}/".format(scheme)

            # for http://, https:// and s3:// paths
            if path.startswith(prefix):
                self.path = path
                self.scheme = scheme
                self.gdal_prefix = "/vsi{}/".format(scheme)

            # for /vsicurl/ and /vsis3/ paths
            elif path.startswith(vsi_prefix):
                self.path = (
                    path.replace(vsi_prefix, "") if scheme == "curl"
                    else path.replace(vsi_prefix, prefix)
                )
                self.scheme = scheme
                self.gdal_prefix = vsi_prefix

            else:
                continue
            self.is_remote = True
            break

        else:
            # for other yet unsupported paths like gs://
            if "://" in path:
                raise ValueError("unsupported URI: {}".format(path))
            self.path = path
            self.scheme = None
            self.is_remote = False
            self.gdal_prefix = None

    @property
    def vsi_path(self):
        """
        Return path in GDAL style.

        e.g. http://example.com/some-file.tif will become
        /vsicurl/http://example.com/some-file.tif
        """
        if self.gdal_prefix:
            # if path contains http:// or https://, just add /vsicurl/ otherwise remove
            # suffix (e.g. s3://) and add gdal_prefix
            return "{}{}".format(
                self.gdal_prefix,
                (
                    self.path if self.scheme == "curl"
                    else self.path.replace("{}://".format(self.scheme), "")
                )
            )
        else:
            return self.path

    @property
    def bucket(self):
        """Return bucket name in case of an S3 object."""
        return self.path.split("/")[2] if self.scheme == "s3" else None

    @property
    def basekey(self):
        """
        Return basekey name in case of an S3 object.

        e.g. from s3://my-bucket/foo/bar/ will return foo/bar/
        """
        return "/".join(self.path.split("/")[3:]) if self.scheme == "s3" else None

    def exists(self):
        """
        Return True if path exists locally or remotely.

        On http(s) paths this will make an HTTP request on the metadata, on s3 paths
        it will use boto3 to check whether the key exists and on local paths it will
        call `os.path.exists`.
        """
        if self.scheme == "curl":
            try:
                urlopen(self.path).info()
                return True
            except HTTPError as e:
                if e.code == 404:
                    return False
                else:
                    raise

        elif self.scheme == "s3":
            key = "/".join(self.path.split("/")[3:])
            for obj in get_boto3_bucket(self.path.split("/")[2]).objects.filter(
                Prefix=key
            ):
                if obj.key == key:
                    return True
            else:
                return False

        else:
            return os.path.exists(self.path)

    def absolute(self, base=None):
        """
        Return absolute path if path is local.

        Parameters
        ----------
        path : path to file
        base_dir : base directory used for absolute path

        Returns
        -------
        absolute path
        """
        if self.is_remote:
            return self.path
        else:
            if os.path.isabs(self.path):
                return self.path
            else:
                if base is None or not os.path.isabs(base):
                    raise TypeError("base_dir must be an absolute path.")
                return os.path.abspath(os.path.join(base, self.path))

    def relative_to(self, base=None):
        """
        Return relative path to base if both are local.

        Parameters
        ----------
        path : path to file
        base_dir : directory where path sould be relative to

        Returns
        -------
        relative path
        """
        if self.is_remote or Path(base).is_remote:
            return self.path
        else:
            return os.path.relpath(self.path, base)


def process_tiles_exist(config=None, process_tiles=None):
    """
    Yield process tiles and whether their output already exists or not.

    The S3 part of the function are loosely inspired by
    https://alexwlchan.net/2019/07/listing-s3-keys/

    Parameters
    ----------
    config : mapchete.config.MapcheteConfig
    process_tiles : iterator

    Yields
    ------
    tuple : (process_tile, exists)
    """
    basepath = Path(config.output_reader.path)

    # only on TileDirectories on S3
    if (
        not config.output_reader.path.endswith(config.output_reader.file_extension) and
        basepath.scheme == "s3"
    ):
        import boto3
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        # make process tiles unique
        process_tiles = set(process_tiles)
        # determine current zoom
        zoom = next(iter(process_tiles)).zoom
        # get all output tiles for process tiles
        output_tiles = set(
            [
                t
                for process_tile in process_tiles
                for t in config.output_pyramid.intersecting(process_tile)
            ]
        )
        # create a mapping between paths and process_tiles
        paths = dict()
        # group process_tiles by row
        rowgroups = defaultdict(list)
        # remember already yielded process_tiles
        yielded = set()
        for output_tile in output_tiles:
            if output_tile.zoom != zoom:
                raise ValueError("tiles of different zoom levels cannot be mixed")
            path = config.output_reader.get_path(output_tile)
            process_tile = config.process_pyramid.intersecting(output_tile)[0]
            paths[path] = process_tile
            rowgroups[process_tile.row].append(process_tile)
        # use prefix until row, page through api results
        for row, tiles in rowgroups.items():
            logger.debug("check existing tiles in row %s" % row)
            prefix = os.path.join(*[basepath.basekey, str(zoom), str(row)])
            logger.debug(
                "read keys %s*" % os.path.join("s3://" + basepath.bucket, prefix)
            )

            for page in paginator.paginate(Bucket=basepath.bucket, Prefix=prefix):
                logger.debug("read next page")
                try:
                    contents = page["Contents"]
                except KeyError:
                    break
                for obj in contents:
                    path = obj["Key"]
                    # get matching process_tile
                    process_tile = paths[os.path.join("s3://" + basepath.bucket, path)]
                    # store and yield process tile if it was not already yielded
                    if process_tile not in yielded:
                        yielded.add(process_tile)
                        yield (process_tile, True)

        # finally, yield all process tiles which were not yet yielded as False
        for process_tile in process_tiles.difference(yielded):
            yield (process_tile, False)

    else:
        def _exists(tile):
            return (tile, config.output_reader.tiles_exist(tile))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for future in concurrent.futures.as_completed(
                (executor.submit(_exists, tile) for tile in process_tiles)
            ):
                yield future.result()


def _parse_path(path):
    for prefix, scheme in REMOTE_SCHEMES.items():
        vsi_prefix = "/vsi{}/".format(scheme)

        # for http://, https:// and s3:// paths
        if path.startswith(prefix):
            gdal_prefix = "/vsi{}/".format(scheme)

        # for /vsicurl/ and /vsis3/ paths
        elif path.startswith(vsi_prefix):
            path = (
                path.replace(vsi_prefix, "") if scheme == "curl"
                else path.replace(vsi_prefix, prefix)
            )
            gdal_prefix = vsi_prefix

        else:
            continue
        is_remote = True
        break

    else:
        # for other yet unsupported paths like gs://
        if "://" in path:
            raise ValueError("unsupported URI: {}".format(path))
        path = path
        scheme = None
        is_remote = False
        gdal_prefix = None

    return path, scheme, is_remote, gdal_prefix


def makedirs(path):
    """
    Silently create all subdirectories of path if path is local.

    Parameters
    ----------
    path : path
    """
    if not Path(path).is_remote:
        try:
            os.makedirs(path)
        except OSError:
            pass
