# SharadarDailyBcolzWriter class derived from BcolzDailyBarWriter
# within zipline/data/bcolz_daily_bars.py for supporting float64 values
# and sharadar sep tags.


import numpy as np
from numpy import (
    array,
    full,
    iinfo,
    nan,
)

import pandas as pd
from pandas import (
    DatetimeIndex,
    NaT,
    read_csv,
    to_datetime,
    Timestamp,
)

from functools import partial
import warnings
from toolz import compose

from bcolz import carray, ctable

from zipline.utils.functional import apply
from zipline.utils.input_validation import expect_element
from zipline.utils.numpy_utils import iNaT, float64_dtype, int64_dtype
from zipline.utils.cli import maybe_show_progress
from zipline.data.bcolz_daily_bars import BcolzDailyBarWriter

from fsharadar.defs import bundle_max_value, bundle_missing_value

# from fsharadar.sep.meta import bundle_tags
# BUNDLE_TAGS = frozenset(bundle_tags)
# BCOLZ_COLUMNS = tuple(bundle_tags + ['day', 'id'])

# UINT32_MAX = iinfo(np.uint32).max

def check_float64_safe(value, colname):
    if value > bundle_max_value: #  UINT32_MAX
        raise ValueError(
            "Value %s from column '%s' is too large" % (value, colname)
        )

@expect_element(invalid_data_behavior={'warn', 'raise', 'ignore'})
def winsorise_float64(df, invalid_data_behavior, *columns):

    columns = list(columns)
    mask = df[columns] > bundle_max_value # UINT32_MAX

    if invalid_data_behavior != 'ignore':
        mask |= df[columns].isnull()
    else:
        # we are not going to generate a warning or error for this so just use
        # nan_to_num
        df[columns] = np.nan_to_num(df[columns])

    mv = mask.values
    if mv.any():
        if invalid_data_behavior == 'raise':
            raise ValueError(
                '%d values out of bounds for float64: %r' % (
                    mv.sum(), df[mask.any(axis=1)],
                ),
            )
        if invalid_data_behavior == 'warn':
            warnings.warn(
                'Ignoring %d values because they are out of bounds for'
                ' float64: %r' % (
                    mv.sum(), df[mask.any(axis=1)],
                ),
                stacklevel=3,  # one extra frame for `expect_element`
            )

    df[mask] = bundle_missing_value #  0
    return df


class SharadarDailyBcolzWriter(BcolzDailyBarWriter):
    
    def __init__(self, filename, calendar, start_session, end_session, bundle_tags):
        BcolzDailyBarWriter.__init__(self, filename, calendar, start_session, end_session)

        self.bundle_tags = frozenset(bundle_tags)
        self.bcolz_columns = tuple(bundle_tags + ['day', 'id'])

    def write(self,
              data,
              assets=None,
              show_progress=False,
              invalid_data_behavior='warn'):
 
        ctx = maybe_show_progress(
            (
                (sid, self.to_ctable(sid, df, invalid_data_behavior))
                for sid, df in data
            ),
            show_progress=show_progress,
            item_show_func=self.progress_bar_item_show_func,
            label=self.progress_bar_message,
            length=len(assets) if assets is not None else None,
        )
        with ctx as it:
            return self._write_internal(it, assets)
        
    @expect_element(invalid_data_behavior={'warn', 'raise', 'ignore'})
    def to_ctable(self, sid, raw_data, invalid_data_behavior):
        
        if isinstance(raw_data, ctable):
            # we already have a ctable so do nothing
            return raw_data

        winsorise_float64(raw_data, invalid_data_behavior, *self.bundle_tags)
        processed = raw_data[list(self.bundle_tags)]
        dates = raw_data.index.values.astype('datetime64[s]')
        check_float64_safe(dates.max().view(np.float64), 'day')
        processed['day'] = dates.astype('float64')

        return ctable.fromdataframe(processed)

    
    def _write_internal(self, iterator, assets):
  
        total_rows = 0
        first_row = {}
        last_row = {}
        calendar_offset = {}

        # Maps column name -> output carray.
        columns = {
            k: carray(array([], dtype=float64_dtype))
            for k in self.bcolz_columns # BCOLZ_COLUMNS
        }

        earliest_date = None
        sessions = self._calendar.sessions_in_range(
            self._start_session, self._end_session
        )

        if assets is not None:
            @apply
            def iterator(iterator=iterator, assets=set(assets)):
                for asset_id, table in iterator:
                    if asset_id not in assets:
                        raise ValueError('unknown asset id %r' % asset_id)
                    yield asset_id, table

        for asset_id, table in iterator:
            nrows = len(table)
            for column_name in columns:
                if column_name == 'id':
                    # We know what the content of this column is, so don't
                    # bother reading it.
                    columns['id'].append(
                        full((nrows,), asset_id, dtype='float64'),
                    )
                    continue

                columns[column_name].append(table[column_name])

            if earliest_date is None:
                earliest_date = table["day"][0]
            else:
                earliest_date = min(earliest_date, table["day"][0])

            # Bcolz doesn't support ints as keys in `attrs`, so convert
            # assets to strings for use as attr keys.
            asset_key = str(asset_id)

            # Calculate the index into the array of the first and last row
            # for this asset. This allows us to efficiently load single
            # assets when querying the data back out of the table.
            first_row[asset_key] = total_rows
            last_row[asset_key] = total_rows + nrows - 1
            total_rows += nrows

            table_day_to_session = compose(
                self._calendar.minute_to_session_label,
                partial(Timestamp, unit='s', tz='UTC'),
            )
            asset_first_day = table_day_to_session(table['day'][0])
            asset_last_day = table_day_to_session(table['day'][-1])

            asset_sessions = sessions[
                sessions.slice_indexer(asset_first_day, asset_last_day)
            ]
            assert len(table) == len(asset_sessions), (
                'Got {} rows for daily bars table with first day={}, last '
                'day={}, expected {} rows.\n'
                'Missing sessions: {}\n'
                'Extra sessions: {}'.format(
                    len(table),
                    asset_first_day.date(),
                    asset_last_day.date(),
                    len(asset_sessions),
                    asset_sessions.difference(
                        to_datetime(
                            np.array(table['day']),
                            unit='s',
                            utc=True,
                        )
                    ).tolist(),
                    to_datetime(
                        np.array(table['day']),
                        unit='s',
                        utc=True,
                    ).difference(asset_sessions).tolist(),
                )
            )

            # Calculate the number of trading days between the first date
            # in the stored data and the first date of **this** asset. This
            # offset used for output alignment by the reader.
            calendar_offset[asset_key] = sessions.get_loc(asset_first_day)

        # This writes the table to disk.
        full_table = ctable(
            columns=[
                columns[colname]
                for colname in self.bcolz_columns # BCOLZ_COLUMNS
            ],
            names=self.bcolz_columns, # BCOLZ_COLUMNS
            rootdir=self._filename,
            mode='w',
        )

        full_table.attrs['first_trading_day'] = (
            earliest_date if earliest_date is not None else iNaT
        )

        full_table.attrs['first_row'] = first_row
        full_table.attrs['last_row'] = last_row
        full_table.attrs['calendar_offset'] = calendar_offset
        full_table.attrs['calendar_name'] = self._calendar.name
        full_table.attrs['start_session_ns'] = self._start_session.value
        full_table.attrs['end_session_ns'] = self._end_session.value
        full_table.flush()
        
        return full_table
