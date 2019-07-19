import json
import logging
import os
from urllib.request import urlopen
from urllib.error import HTTPError

from mapchete.io._path import makedirs
from mapchete.io._misc import get_boto3_bucket


logger = logging.getLogger(__name__)


def write_json(path, params):
    """Write local or remote."""
    logger.debug("write %s to %s", params, path)
    if path.startswith("s3://"):
        bucket = get_boto3_bucket(path.split("/")[2])
        key = "/".join(path.split("/")[3:])
        logger.debug("upload %s", key)
        bucket.put_object(
            Key=key,
            Body=json.dumps(params, sort_keys=True, indent=4)
        )
    else:
        makedirs(os.path.dirname(path))
        with open(path, 'w') as dst:
            json.dump(params, dst, sort_keys=True, indent=4)


def read_json(path):
    """Read local or remote."""
    if path.startswith(("http://", "https://")):
        try:
            return json.loads(urlopen(path).read().decode())
        except HTTPError:
            raise FileNotFoundError("%s not found", path)
    elif path.startswith("s3://"):
        bucket = get_boto3_bucket(path.split("/")[2])
        key = "/".join(path.split("/")[3:])
        for obj in bucket.objects.filter(Prefix=key):
            if obj.key == key:
                return json.loads(obj.get()['Body'].read().decode())
        raise FileNotFoundError("%s not found", path)
    else:
        try:
            with open(path, "r") as src:
                return json.loads(src.read())
        except:
            raise FileNotFoundError("%s not found", path)
