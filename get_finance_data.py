import os
import json
import pandas as pd
import yfinance as yf


def data_downloader():

    # get stock price history
    for ticker in config['tickers']:
        price_history = yf.download(
            tickers=ticker,
            # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max (optional, default is '1mo')
            period=config['period'],
            # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo (optional, default is '1d')
            interval=config['interval']).round(2).reset_index()
        price_history['ticker_tmp'] = ticker
        price_history.insert(
            loc=0,
            column='Ticker',
            value=price_history['ticker_tmp'])
        price_history.drop(columns=['ticker_tmp'], inplace=True)
        price_history.to_csv('{}_{}_{}_{}.csv'.format(
            'history',
            ticker,
            str(price_history['Date'][0].date()),
            str(price_history['Date'][len(price_history)-1].date()),
        ), index=False)

    # get stock price history
    for ticker in config['tickers']:
        ticker_obj = yf.Ticker(ticker)
        div_history = ticker_obj.dividends
        div_history = pd.DataFrame(div_history).round(2).reset_index()
        div_history['ticker_tmp'] = ticker
        div_history.insert(
            loc=0,
            column='Ticker',
            value=div_history['ticker_tmp'])
        div_history.drop(columns=['ticker_tmp'], inplace=True)
        div_history.to_csv('{}_{}_{}_{}.csv'.format(
            'dividend',
            ticker,
            str(div_history['Date'][0].date()),
            str(div_history['Date'][len(div_history)-1].date()),
        ), index=False)


def data_aggregator():
    pattern_list = ['history', 'dividend']
    # aggregate files
    for pattern in pattern_list:
        dir_dict = {}
        i = 0
        for root, dirs, files in os.walk(os.getcwd()):
            for file in files:
                if file.startswith(pattern) and file.endswith(".csv"):
                    label = file
                    dir_dict[label] = os.path.join(root, file)
                    i = i+1

        master_df = pd.DataFrame()
        for filename in dir_dict:
            # load raw data
            master_df = pd.concat(
                [pd.read_csv(dir_dict[filename]), master_df],
                ignore_index=True)
        master_df.drop_duplicates(subset=None, keep='first', inplace=True)
        master_df.to_csv('{}_{}.csv'.format('master', pattern), index=False)
        del master_df, dir_dict


if __name__ == "__main__":
    config = {}
    # read config
    with open('config.json', 'r') as f:
        config = json.load(f)
    directory = os.path.abspath(os.getcwd() + config['data_folder'])
    if not os.path.exists(directory):
        os.makedirs(directory)
    os.chdir(directory)

    data_downloader()
    data_aggregator()
