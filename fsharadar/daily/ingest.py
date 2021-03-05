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
from zipline.data.bundles.core import cache_path, daily_equity_relative, asset_db_relative

from fsharadar.daily.meta import bundle_name, bundle_tags, bundle_missing_value
from fsharadar.daily.bcolz_writer_int64 import SharadarDailyBcolzWriter

def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        ).fillna(bundle_missing_value) # 0.0
        yield asset_id, asset_data

def gen_asset_metadata(data, show_progress):

    data = data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )
    data.reset_index(inplace=True)
    data['start_date'] = data.date.amin
    data['end_date'] = data.date.amax
    del data['date']
    data.columns = data.columns.get_level_values(0)

    # data['exchange'] = 'QUANDL'
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)
    return data

def get_ticker_sids(tickers_df):
    tickers_dropna = tickers_df.dropna(subset=['ticker'])
    ticker_sids = pd.DataFrame(index=tickers_dropna.ticker.unique())
    ticker_sids['sid'] = tickers_dropna.groupby('ticker').apply(lambda x: x.permaticker.values[0])
    return ticker_sids

def get_ticker_exchanges(tickers_df):
    tickers_dropna = tickers_df.dropna(subset=['ticker'])
    ticker_exchanges = pd.DataFrame(index=tickers_dropna.ticker.unique())
    ticker_exchanges['exchange'] = tickers_dropna.groupby('ticker').apply(lambda x: x.exchange.values[0])
    return ticker_exchanges

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
                          sharadar_tickers_file, # new argument
                          sharadar_daily_file): # new argument

    # read tickers_file
    tickers_df = pd.read_csv(sharadar_tickers_file)

    # read daily_file
    usecols = ['ticker', 'date'] + bundle_tags
    raw_data = pd.read_csv(sharadar_daily_file, parse_dates=['date'], usecols=usecols)
    raw_data.rename(columns={'ticker': 'symbol'}, inplace=True)
    
    # generate metadata
    asset_metadata = gen_asset_metadata(
        raw_data[['symbol', 'date']],
        show_progress=True
    )

    # add sids from tickers_file
    ticker_sids = get_ticker_sids(tickers_df)
    asset_metadata = asset_metadata.join(ticker_sids, on='symbol')
    asset_metadata.dropna(inplace=True) # yes, there was one missing symbol in tickers_file
    asset_metadata['sid'] = asset_metadata.sid.astype(int)
    asset_metadata.set_index('sid', inplace=True)
        
    # add exchanges from tickers_file
    ticker_exchanges = get_ticker_exchanges(tickers_df)
    asset_metadata = asset_metadata.join(ticker_exchanges, on='symbol')

    asset_metadata_exchanges = asset_metadata.exchange.unique()
    exchanges = pd.DataFrame(asset_metadata_exchanges, columns=['exchange'])
    exchanges['canonical_name'] = asset_metadata_exchanges
    exchanges['country_code'] = 'US'

    # write metadata
    asset_db_writer.write(asset_metadata, exchanges=exchanges)

    # write raw data
    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)
    
    raw_data.set_index(['date', 'symbol'], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(
            raw_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )

def ingest(tickers_file=None, daily_file=None): # new arguments of bundle.ingest

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
                daily_file
        )


 
