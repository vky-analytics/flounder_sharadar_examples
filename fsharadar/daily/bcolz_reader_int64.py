# SharadarDailyBcolzReader class derived from BcolzDailyBarReader
# within zipline/data/bcolz_daily_bars.py for supporting int64 values.

import numpy as np
from numpy import (
    array,
    full,
    iinfo,
    nan,
)

from fsharadar.daily.cython_read_int64 import _read_bcolz_data
from zipline.data.bcolz_daily_bars import BcolzDailyBarReader

from fsharadar.daily.meta import bundle_missing_value, int_to_float_factor

class SharadarDailyBcolzReader(BcolzDailyBarReader):

    def __init__(self, table, read_all_threshold=3000):
        BcolzDailyBarReader.__init__(self, table, read_all_threshold)

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
        )

    def get_value(self, sid, dt, field):

        ix = self.sid_day_index(sid, dt)
        value = self._spot_col(field)[ix]*int_to_float_factor
        
        if value == bundle_missing_value:
            return nan
        else:
            return value






