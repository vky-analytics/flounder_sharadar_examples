# Connecting Zipline with Sharadar US Equity Prices and Fundamentals

This repository demonstrates the connection of the [Zipline](https://github.com/quantopian/zipline)
algorithmic trading platform
with [US Equity Prices](https://www.quandl.com/databases/SEP/data) and Daily Metrics from [Core US Fundamental Data](https://www.quandl.com/databases/SF1/data)
provided by [Sharadar](https://www.quandl.com/publishers/sharadar). The corresponding
interface is implemented through the [fsharadar](https://flounderteam.github.io/fsharadar/) module. 

The application can be installed from basic conda with the following steps:

    conda create --name flounder python=3.6
    conda activate flounder
    conda install numpy
    conda install cython
    pip install fsharadar
    conda install jupyter

Associated notebooks in this repository show how to ingest the Sharadar data into
the Zipline data bundles and subsequently load them for running the Zipline pipeline.




