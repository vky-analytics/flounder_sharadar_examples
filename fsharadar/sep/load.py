# load() and associated methods implemented from
# the corresponding originals within zipline/data/bundles/core.py
# for connecting BundleData with sharadar-specific bundle and reader.

import os
import errno

from contextlib2 import ExitStack
import pandas as pd

from trading_calendars import get_calendar

from zipline.assets import AssetFinder
from zipline.data.adjustments import SQLiteAdjustmentReader

from zipline.data.bundles.core import BundleData
from zipline.data.bundles.core import daily_equity_path
from zipline.data.bundles.core import asset_db_path, adjustment_db_path

from fsharadar.load import most_recent_data
from fsharadar.bcolz_reader_float64 import SharadarDailyBcolzReader

from fsharadar.sep.meta import bundle_name, bundle_tags


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
            bundle_tags=bundle_tags,
        ),
        adjustment_reader=SQLiteAdjustmentReader(
            adjustment_db_path(name, timestr, environ=environ),
        ),
    )

