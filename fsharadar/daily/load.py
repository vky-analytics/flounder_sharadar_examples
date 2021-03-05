# load() and associated methods implemented from
# the corresponding originals within zipline/data/bundles/core.py
# for connecting BundleData with sharadar-specific bundle and reader.

import os
import errno
from toolz import complement
from contextlib2 import ExitStack
import pandas as pd

from trading_calendars import get_calendar

import zipline.utils.paths as pth
from zipline.assets import AssetFinder
from zipline.data.bundles import bundles
from zipline.data.bundles import UnknownBundle
from zipline.data.bundles import from_bundle_ingest_dirname
from zipline.data.bundles.core import BundleData
from zipline.data.bundles.core import asset_db_path, daily_equity_path

from fsharadar.daily.meta import bundle_name
from fsharadar.daily.bcolz_reader_int64 import SharadarDailyBcolzReader

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

def load():

    # original arguments
    name = bundle_name
    environ=os.environ
    timestamp=None

    if timestamp is None:
        timestamp = pd.Timestamp.utcnow()
    timestr = most_recent_data(name, timestamp, environ=environ)
    
    return BundleData(
        asset_finder=AssetFinder(
            asset_db_path(name, timestr, environ=environ),
        ),
        equity_minute_bar_reader=None,
        equity_daily_bar_reader=SharadarDailyBcolzReader(
            daily_equity_path(name, timestr, environ=environ),
        ),
        adjustment_reader=None,
    )

