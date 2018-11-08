import fiona
import os

import mapchete
from mapchete.index import zoom_index_gen
from mapchete.io import get_boto3_bucket


def test_remote_indexes(mp_s3_tmpdir, gtiff_s3):

    zoom = 7
    gtiff_s3.dict.update(zoom_levels=zoom)

    def gen_indexes_and_check():
        # generate indexes
        list(zoom_index_gen(
            mp=mp,
            zoom=zoom,
            out_dir=mp.config.output.path,
            geojson=True,
            txt=True,
        ))

        # assert GeoJSON exists
        with fiona.open(os.path.join(mp.config.output.path, "%s.geojson" % zoom)) as src:
            assert len(src) == 2

        # assert TXT exists
        txt_index = os.path.join(mp.config.output.path, "%s.txt" % zoom)
        bucket = get_boto3_bucket(txt_index.split("/")[2])
        key = "/".join(txt_index.split("/")[3:])
        for obj in bucket.objects.filter(Prefix=key):
            if obj.key == key:
                content = obj.get()['Body'].read().decode()
                assert len([l + '\n' for l in content.split('\n') if l]) == 2

    with mapchete.open(gtiff_s3.dict) as mp:
        # write output data
        mp.batch_process()

        # generate indexes and check
        gen_indexes_and_check()

        # generate indexes again and assert nothing has changes
        gen_indexes_and_check()
