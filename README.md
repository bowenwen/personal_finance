# Get Finance Data

This is an implementation of the yfinance package in python to obtain historical stock prices and dividends.

This implementation has a data downloader and data aggregator to produce a master data file. The data downloader obtains data periodically from yfinance based on a json config file. The data aggregator collects all the data file and merge them into one master data file which can be used for the analysis of your own stock portfolio.

## Getting Started

### Set up python environment

```bash
conda remove --name yfinance --all
conda create -n yfinance -y python=3.6 ipykernel
conda activate yfinance
pip install yfinance --upgrade --no-cache-dir
ipython kernel install --user --name=yfinance
conda deactivate
```

### Get finance data

1. Change `config.json` with a list of your tickers
2. Run get_finance_data.py

``` bash
conda activate yfinance
python get_finance_data.py
```
