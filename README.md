# Personal Finance Helper

The personal finance helper downloads stock market data and analyze your trade acivities. The market data downloader is an implementation of the yfinance package in python to obtain historical stock prices and dividends. The trade activity analysis includes:

* asset allocation (based on inputs from `ticker_category.csv`)
* income from asset (dividend calculation)
* performance by stocks by ticker

## Getting Started

### Set up python environment

```bash
conda remove --name yfinance --all -y
conda create -n yfinance -y python=3.6 ipykernel
conda activate yfinance
pip install yfinance xlrd xlsxwriter openpyxl --upgrade --no-cache-dir
ipython kernel install --user --name=yfinance
conda deactivate
```

### Run the tool

1. Change `config.json` with a list of your tickers
2. Run main_py

``` bash
conda activate yfinance
python main.py
```
