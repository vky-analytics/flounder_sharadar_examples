# Common auxiliary methods used in the bundle-specific load.py files

import os
import numpy as np
from toolz import complement

import zipline.utils.paths as pth
from zipline.data.bundles import bundles
from zipline.data.bundles import UnknownBundle
from zipline.data.bundles import from_bundle_ingest_dirname

def most_recent_data(bundle_name, timestamp, environ=None):

    if bundle_name not in bundles:
        raise UnknownBundle(bundle_name)

    try:
        candidates = os.listdir(
            pth.data_path([bundle_name], environ=environ),
        )
        return pth.data_path(
            [bundle_name,
             max(
                 filter(complement(pth.hidden), candidates),
                 key=from_bundle_ingest_dirname,
             )],
            environ=environ,
        )
    except (ValueError, OSError) as e:
        if getattr(e, 'errno', errno.ENOENT) != errno.ENOENT:
            raise
        raise ValueError(
            'no data for bundle {bundle!r} on or before {timestamp}\n'
            'maybe you need to run: $ zipline ingest -b {bundle}'.format(
                bundle=bundle_name,
                timestamp=timestamp,
            ),
        )

