# SharadarDailyBcolzReader class derived from BcolzDailyBarReader
# within zipline/data/bcolz_daily_bars.py for supporting float64 values.

import numpy as np
from numpy import (
    array,
    full,
    iinfo,
    nan,
)

from zipline.data.bcolz_daily_bars import BcolzDailyBarReader

from fsharadar.defs import bundle_missing_value
from fsharadar.cython_read_float64 import _read_bcolz_data
# from fsharadar.sep.meta import bundle_tags

class SharadarDailyBcolzReader(BcolzDailyBarReader):

    def __init__(self, table, read_all_threshold=3000, bundle_tags=None):
        BcolzDailyBarReader.__init__(self, table, read_all_threshold)
        self.bundle_tags = bundle_tags

    def load_raw_arrays(self, columns, start_date, end_date, assets):
        start_idx = self._load_raw_arrays_date_to_index(start_date)
        end_idx = self._load_raw_arrays_date_to_index(end_date)

        first_rows, last_rows, offsets = self._compute_slices(
            start_idx,
            end_idx,
            assets,
        )
        read_all = len(assets) > self._read_all_threshold
        return _read_bcolz_data(
            self._table,
            (end_idx - start_idx + 1, len(assets)),
            list(columns),
            first_rows,
            last_rows,
            offsets,
            read_all,
            self.bundle_tags, # new argument
        )

    def get_value(self, sid, dt, field):

        ix = self.sid_day_index(sid, dt)
        value = self._spot_col(field)[ix]
        
        if value == bundle_missing_value:
            return nan
        else:
            return value






