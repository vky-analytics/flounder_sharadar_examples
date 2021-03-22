import numpy as np

from six import with_metaclass
from zipline.utils.numpy_utils import float64_dtype, categorical_dtype
from zipline.pipeline.domain import US_EQUITIES
from zipline.pipeline.data.dataset import Column, DataSet, DataSetMeta

bundle_name = "flounder-sharadar-daily"
bundle_tags = ['marketcap', 'ev', 'evebit', 'evebitda', 'pb', 'pe', 'ps']

class FundamentalsMeta(DataSetMeta):
    
    def __new__(cls, name, bases, dct):
        
        for tag in bundle_tags:
            dct[tag] = Column(float64_dtype, currency_aware=True)
        dct['currency'] = Column(categorical_dtype)
        
        return super().__new__(cls, name, bases, dct)

class FundamentalsDataSet(with_metaclass(FundamentalsMeta, DataSet)):
    pass

Fundamentals = FundamentalsDataSet.specialize(US_EQUITIES)
