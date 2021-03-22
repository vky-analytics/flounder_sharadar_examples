# ingest() and associated methods implemented from
# the corresponding originals within zipline/data/bundles/core.py
# and zipline/data/bundles/quandl.py for ingesting
# data from the sharadar sep and actions files and supporting
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
from zipline.data.adjustments import SQLiteAdjustmentWriter

from zipline.data.bundles import UnknownBundle
from zipline.data.bundles import bundles, register
from zipline.data.bundles import to_bundle_ingest_dirname

from zipline.data.bundles.core import (
    cache_path,
    daily_equity_relative,
    asset_db_relative,
    adjustment_db_relative,
)

from fsharadar.ingest import (
    parse_pricing_and_vol,
    get_ticker_sids,
    get_asset_metadata,
    get_exchanges,
)

from fsharadar.bcolz_writer_float64 import SharadarDailyBcolzWriter
from fsharadar.bcolz_reader_float64 import SharadarDailyBcolzReader

from fsharadar.sep.meta import bundle_name, bundle_tags

def read_sep_file(daily_file, tickers_df):

    # read sep file
    usecols = ['ticker', 'date'] + bundle_tags + ['dividends']
    raw_data = pd.read_csv(daily_file, parse_dates=['date'], usecols=usecols)
    raw_data.rename(columns={'ticker': 'symbol'}, inplace=True)

    # remove assets without sids
    all_tickers = tickers_df.ticker.unique()
    common_tickers = list(set(raw_data.symbol.unique()) & set(all_tickers))
    raw_data = raw_data.query('symbol in @common_tickers')

    # remove assets with dividends > close
    bad_data = raw_data[raw_data.dividends > raw_data.close]  
    bad_symbols = bad_data.symbol.unique()
    good_symbols = list(set(raw_data.symbol.unique()) - set(bad_symbols))
    raw_data = raw_data.query('symbol in @good_symbols')
    
    return raw_data

def read_actions_file(actions_file, tickers_df):

    # read actions file
    actions_df = pd.read_csv(actions_file, parse_dates=['date'])
    actions_df.rename(columns={'ticker': 'symbol'}, inplace=True)

    # remove assets without sids
    all_tickers = tickers_df.ticker.unique()
    common_tickers = list(set(actions_df.symbol.unique()) & set(all_tickers))
    actions_df = actions_df.query('symbol in @common_tickers')

    return actions_df
    

def unadjust_splits(sep_df, actions_df):

    actions_mi =  actions_df.set_index(['symbol', 'date'])
    actions_mi.sort_index(level=0, inplace=True)
    action_tickers = actions_mi.index.get_level_values(0).unique()

    sep_mi =  sep_df.set_index(['symbol', 'date'])
    sep_mi.sort_index(level=0, inplace=True)
    
    tickers = sep_df.symbol.unique()
    dfs = []

    for i, ticker in enumerate(tickers):
    
        sep_xs = sep_mi.xs(ticker)
        sep_xs = sep_xs.sort_index(ascending=False)
    
        if ticker not in action_tickers:
            dfs.append(sep_xs)
            continue

        # select splits
        actions_xs = actions_mi.xs(ticker)
        actions_xs = actions_xs.sort_index(ascending=False)
        splits = actions_xs[actions_xs.action == 'split'].value  

        # add splits to the sep frame
        sep_xs = sep_xs.join(splits, how='left')
        sep_xs = sep_xs.rename(columns={'value': 'split_ratio'})

        # calculate split factor
        sep_xs['split_ratio'] = sep_xs['split_ratio'].shift(1)
        sep_xs['split_ratio'] = sep_xs['split_ratio'].replace(np.nan, 1.0)
        sep_xs['split_factor'] = sep_xs['split_ratio'].cumprod()

        # apply split factor to prices
        for field in ['close', 'open', 'high', 'low', 'dividends']:
            sep_xs[field] = sep_xs['split_factor']*sep_xs[field]

        # apply split factor to volumes
        sep_xs['volume'] = sep_xs['volume']/sep_xs['split_factor']
    
        sep_xs.drop(['split_ratio', 'split_factor'], axis=1, inplace=True)
        dfs.append(sep_xs)

    unadj_df = pd.concat(dfs, keys=tickers)
    unadj_df = unadj_df.reset_index()
    unadj_df.rename(columns={'level_0': 'symbol'}, inplace=True)
    return unadj_df

def get_dividends(unadj_data, tickers_df):
    
    dividends = unadj_data[[
        'symbol',
        'date',
        'dividends',
    ]].loc[unadj_data.dividends != 0]

    # add sids from tickers_file
    ticker_sids = get_ticker_sids(tickers_df)
    dividends = dividends.join(ticker_sids, on='symbol', how='left')[['sid', 'date', 'dividends']]
    dividends = dividends.reset_index(drop=True)
    
    dividends['record_date'] = dividends['declared_date'] = dividends['pay_date'] = pd.NaT
    
    dividends.rename(
        columns={
            'dividends': 'amount',
            'date': 'ex_date',
        },
        inplace=True,
    )
    
    return dividends

def get_splits(actions_df, tickers_df):
    
    splits = actions_df[actions_df.action == 'split'][['date', 'symbol', 'value']]    
    splits.rename(columns={'value': 'split_ratio'}, inplace=True)
    splits = splits.reset_index(drop=True)
    
    # add sids
    ticker_sids = get_ticker_sids(tickers_df)
    splits = splits.join(ticker_sids, on='symbol', how='left')[['sid', 'date', 'split_ratio']]
    
    splits['split_ratio'] = 1.0 / splits.split_ratio
    
    splits.rename(
        columns={
            'split_ratio': 'ratio',
            'date': 'effective_date',
        },
        inplace=True,
    )
    
    return splits


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
                          daily_file, # new argument
                          actions_file): # new argument

    # read tickers_file (with sids)
    tickers_df = pd.read_csv(tickers_file)

    # read daily_file
    raw_data = read_sep_file(daily_file, tickers_df)

    # read actions_file (with splits)
    actions_df = read_actions_file(actions_file, tickers_df)

    # unadjust splits
    # unadj_data = unadjust_splits(raw_data, actions_df)
    unadj_data = raw_data

    # write metadata
    asset_metadata = get_asset_metadata(unadj_data[['symbol', 'date']], tickers_df)
    exchanges = get_exchanges(asset_metadata)
  
    asset_db_writer.write(asset_metadata, exchanges=exchanges)

    # write raw data
    
    unadj_data.set_index(['date', 'symbol'], inplace=True)
    
    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)
    
    daily_bar_writer.write(
        parse_pricing_and_vol(
            unadj_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )

    unadj_data = unadj_data.reset_index()

    # write splits and dividends
    splits = None # get_splits(actions_df, tickers_df)
    dividends = get_dividends(unadj_data, tickers_df)
    
    adjustment_writer.write(
        splits=splits,
        dividends=dividends
    )

def ingest(tickers_file=None, data_file=None, actions_file=None): # new arguments of bundle.ingest

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
        
        minute_bar_writer = None 
        
        # DB Writer
            
        assets_db_path = wd.getpath(*asset_db_relative(name, timestr))
        
        asset_db_writer = AssetDBWriter(assets_db_path)

        adjustment_db_writer = stack.enter_context(
            SQLiteAdjustmentWriter(
                wd.getpath(*adjustment_db_relative(name, timestr)),
                SharadarDailyBcolzReader(daily_bars_path, bundle_tags=bundle_tags),
                overwrite=True,
            )
        )
         
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
                data_file,
                actions_file,
        )
        
        


 
