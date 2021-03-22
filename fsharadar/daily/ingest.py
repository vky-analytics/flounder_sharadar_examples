# ingest() and associated methods implemented from
# the corresponding originals within zipline/data/bundles/core.py
# and zipline/data/bundles/quandl.py for ingesting
# data from the sharadar daily file and supporting
# the sharadar sids supplied through the sharadar tickers file.

import os
import numpy as np
import pandas as pd
from six import iteritems
from contextlib2 import ExitStack
from trading_calendars import get_calendar

import zipline.utils.paths as pth
from zipline.utils.cache import (
    dataframe_cache,
    working_dir,
    working_file,
)

from zipline.assets import AssetDBWriter, AssetFinder

from zipline.data.bundles import UnknownBundle
from zipline.data.bundles import bundles, register
from zipline.data.bundles import to_bundle_ingest_dirname

from zipline.data.bundles.core import (
    cache_path,
    daily_equity_relative,
    asset_db_relative
)

from fsharadar.ingest import (
    parse_pricing_and_vol,
    get_ticker_sids,
    get_asset_metadata,
    get_exchanges,
)

from fsharadar.bcolz_writer_float64 import SharadarDailyBcolzWriter

from fsharadar.daily.meta import bundle_name, bundle_tags

def read_daily_file(daily_file, tickers_df):

    # read sep file
    usecols = ['ticker', 'date'] + bundle_tags
    raw_data = pd.read_csv(daily_file, parse_dates=['date'], usecols=usecols)
    raw_data.rename(columns={'ticker': 'symbol'}, inplace=True)

    # remove assets without sids
    all_tickers = tickers_df.ticker.unique()
    common_tickers = list(set(raw_data.symbol.unique()) & set(all_tickers))
    raw_data = raw_data.query('symbol in @common_tickers')
    
    return raw_data


@register(bundle_name, create_writers=True)
def ingest_sharadar_daily(environ,
                          asset_db_writer, 
                          minute_bar_writer, 
                          daily_bar_writer, 
                          adjustment_writer,
                          calendar,
                          start_session,
                          end_session, 
                          cache,
                          show_progress,
                          output_dir,
                          tickers_file, # new argument
                          daily_file): # new argument

    # read tickers_file
    tickers_df = pd.read_csv(tickers_file)

    # read daily_file
    raw_data = read_daily_file(daily_file, tickers_df)

    # write metadata
    asset_metadata = get_asset_metadata(raw_data[['symbol', 'date']], tickers_df)
    exchanges = get_exchanges(asset_metadata)
  
    asset_db_writer.write(asset_metadata, exchanges=exchanges)

    # write raw data
    
    raw_data.set_index(['date', 'symbol'], inplace=True)
    
    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)
    
    daily_bar_writer.write(
        parse_pricing_and_vol(
            raw_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )

def ingest(tickers_file=None, data_file=None): # new arguments of bundle.ingest

    # original argumenets
    
    name = bundle_name
    environ=os.environ
    timestamp=None
    assets_versions=()
    show_progress=False

    # select bundle
    try:
        bundle = bundles[name]
    except KeyError:
        raise UnknownBundle(name)

    # define output directory based on bundle name and timestamp

    calendar = get_calendar(bundle.calendar_name)
    start_session = calendar.first_session
    end_session = calendar.last_session

    timestamp = pd.Timestamp.utcnow()
    timestamp = timestamp.tz_convert('utc').tz_localize(None)

    timestr = to_bundle_ingest_dirname(timestamp)
    cachepath = cache_path(name, environ=environ)

    output_dir = pth.data_path([name, timestr], environ=environ)

    pth.ensure_directory(output_dir)
    pth.ensure_directory(cachepath)

    # create writers and ingest data
    
    with dataframe_cache(cachepath, clean_on_failure=False) as cache, \
            ExitStack() as stack:
            
    # we use `cleanup_on_failure=False` so that we don't purge the
    # cache directory if the load fails in the middle
        
        wd = stack.enter_context(working_dir(
            pth.data_path([], environ=environ))
        )
        
        daily_bars_path = wd.ensure_dir(
            *daily_equity_relative(name, timestr)
        )
                
        daily_bar_writer = SharadarDailyBcolzWriter(
            daily_bars_path,
            calendar,
            start_session,
            end_session,
            bundle_tags, # new argument
        )
                
        # Do an empty write to ensure that the daily ctables exist
        # when we create the SQLiteAdjustmentWriter below. The
        # SQLiteAdjustmentWriter needs to open the daily ctables so
        # that it can compute the adjustment ratios for the dividends.

        daily_bar_writer.write(())
            
        assets_db_path = wd.getpath(*asset_db_relative(name, timestr))
        asset_db_writer = AssetDBWriter(assets_db_path)

        minute_bar_writer = None 
        adjustment_db_writer = None
         
        bundle.ingest(
                environ,
                asset_db_writer,
                minute_bar_writer,
                daily_bar_writer,
                adjustment_db_writer,
                calendar,
                start_session,
                end_session,
                cache,
                show_progress,
                pth.data_path([name, timestr], environ=environ,),
                tickers_file,
                data_file
        )


 
