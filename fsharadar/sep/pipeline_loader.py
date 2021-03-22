# PipelineLoader class implemented from EquityPricingLoader
# within zipline/pipeline/loader/equity_pricing_loader.py
# by removing adjustments_reader and fx_reader. 


from interface import implements
from zipline.lib.adjusted_array import AdjustedArray
from zipline.pipeline.loaders.utils import shift_dates
from zipline.pipeline.loaders.base import PipelineLoader
from zipline.pipeline.data.equity_pricing import EquityPricing

class PipelineLoader(implements(PipelineLoader)):
    
    """A PipelineLoader for loading daily raw data.

    Parameters
    ----------
    raw_reader : zipline.data.session_bars.SessionBarReader
        Reader providing raw characteristics.
    """

    def __init__(self, bundle_data):
      
        self.raw_reader = bundle_data.equity_daily_bar_reader

    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        # load_adjusted_array is called with dates on which the user's algo
        # will be shown data, which means we need to return the data that would
        # be known at the **start** of each date. We assume that the latest
        # data known on day N is the data from day (N - 1), so we shift all
        # query dates back by a trading session.
        
        sessions = domain.all_sessions()
        shifted_dates = shift_dates(sessions, dates[0], dates[-1], shift=1)

        raw_cols, currency_cols = self._split_column_types(columns)
        del columns  # From here on we should use raw_cols or currency_cols.
        raw_colnames = [c.name for c in raw_cols]

        raw_arrays = self.raw_reader.load_raw_arrays(
            raw_colnames,
            shifted_dates[0],
            shifted_dates[-1],
            sids,
        )

        out = {}
        for c, c_raw in zip(raw_cols, raw_arrays):
            out[c] = AdjustedArray(
                c_raw.astype(c.dtype),
                adjustments={},
                missing_value=c.missing_value,
            )

        return out

    def _split_column_types(self, columns):
        """Split out currency columns from raw columns.

        Parameters
        ----------
        columns : list[zipline.pipeline.data.BoundColumn]
            Columns to be loaded by ``load_adjusted_array``.

        Returns
        -------
        raw_columns : list[zipline.pipeline.data.BoundColumn]
            Price and volume columns from ``columns``.
        currency_columns : list[zipline.pipeline.data.BoundColumn]
            Currency code column from ``columns``, if present.
        """
        currency_name = EquityPricing.currency.name

        raw_columns = []
        currency_columns = []
        for c in columns:
            if c.name == currency_name:
                currency_columns.append(c)
            else:
                raw_columns.append(c)

        return raw_columns, currency_columns


