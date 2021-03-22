# Approximate Quantopian function `get_pricing`

import os
import pandas as pd
from trading_calendars import get_calendar
from zipline.data.data_portal import DataPortal

from fsharadar import sep

def get_pricing(assets, start_date, end_date, field='close', trading_calendar=None):

    bundle_data = sep.load()

    # Get identifiers for asset symbols
    equities = bundle_data.asset_finder.lookup_symbols(assets, as_of_date=None)

    # Set the given start and end dates to Timestamps.
    end_date = pd.Timestamp(end_date, tz='utc')
    start_date = pd.Timestamp(start_date, tz='utc')

    # Get the locations of the start and end dates

    if trading_calendar is None:
        trading_calendar = get_calendar("NYSE")
    
    sessions = trading_calendar.sessions_in_range(start_date, end_date)
    bar_count = len(sessions)
     
    # Create a data portal
    data_portal = DataPortal(
        bundle_data.asset_finder,
        trading_calendar=trading_calendar,
        first_trading_day=bundle_data.equity_daily_bar_reader.first_trading_day,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader)    

    # return the historical data for the given window
    return data_portal.get_history_window(
                            assets=equities,
                            end_dt=end_date,
                            bar_count=bar_count,
                            frequency='1d',
                            field=field,
                            data_frequency='daily')
