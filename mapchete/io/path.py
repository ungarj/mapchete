"""Module to handle local and remote paths."""
import os
from urllib.request import urlopen
from urllib.error import HTTPError

from mapchete.io._misc import get_boto3_bucket


REMOTE_SCHEMES = {
    "http://": "curl",
    "https://": "curl",
    "s3://": "s3"
}


class Path():
    """
    Class to abstract local and remote paths.

    Loosely inspired by the rasterio.path.ParsedPath class.
    """

    def __init__(self, path):
        """Initialize."""
        # store original path
        self.name = path

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


def paths_exist(paths):
    """
    Check whether each path exists remotely or locally.

    Returns
    -------
    dict : keys are paths, values are True or False
    """
    raise NotImplementedError()


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
