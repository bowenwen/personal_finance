import os
import json
import pandas as pd
import yfinance as yf


class MarketData:
    """A class to retrieve market data using yfinance"""

    def getData(self):
        self.__data_downloader()
        self.__data_aggregator()

    def __init__(self, config_file="data/config.json"):
        self.config = {}
        # read self.config
        with open(config_file, "r") as f:
            self.config = json.load(f)
        self.directory = os.path.abspath(
            os.getcwd() + self.config["market_data_folder"]
        )
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def __data_downloader(self):
        # get stock price history
        for ticker in self.config["tickers"]:
            try:
                print(f"downloading history for {ticker}...")
                price_history = (
                    yf.download(
                        tickers=ticker,
                        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max (optional, default is '1mo')
                        period=self.config["period"],
                        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo (optional, default is '1d')
                        interval=self.config["interval"],
                    )
                    .round(2)
                    .reset_index()
                )
                price_history["ticker_tmp"] = ticker
                price_history.insert(
                    loc=0, column="Ticker", value=price_history["ticker_tmp"]
                )
                price_history.drop(columns=["ticker_tmp"], inplace=True)
                startDate = str(price_history["Date"][0].date())
                price_history.to_csv(
                    os.path.join(
                        self.directory,
                        "{}_{}_{}_{}.csv".format(
                            "history", ticker, startDate, self.config["period"]
                        ),
                    ),
                    index=False,
                )
            except:
                print(f"ticker {ticker} history download failed, skipping...")
        # get stock dividends
        for ticker in self.config["tickers"]:
            try:
                print(f"downloading dividend for {ticker}...")
                ticker_obj = yf.Ticker(ticker)
                div_history = ticker_obj.dividends
                div_history = pd.DataFrame(div_history).round(2).reset_index()
                div_history["ticker_tmp"] = ticker
                div_history.insert(
                    loc=0, column="Ticker", value=div_history["ticker_tmp"]
                )
                div_history.drop(columns=["ticker_tmp"], inplace=True)
                startDate = str(div_history["Date"][0].date())
                div_history.to_csv(
                    os.path.join(
                        self.directory,
                        "{}_{}_{}_{}.csv".format(
                            "dividend", ticker, startDate, self.config["period"]
                        ),
                    ),
                    index=False,
                )
            except:
                print(f"ticker {ticker} dividend download failed, skipping...")

    def __data_aggregator(self):
        pattern_list = ["history", "dividend"]
        # aggregate files
        for pattern in pattern_list:
            dir_dict = {}
            i = 0
            for root, dirs, files in os.walk(os.getcwd()):
                for file in files:
                    if file.startswith(pattern) and file.endswith(".csv"):
                        label = file
                        dir_dict[label] = os.path.join(root, file)
                        i = i + 1

            master_df = pd.DataFrame()
            for filename in dir_dict:
                # load raw data
                master_df = pd.concat(
                    [pd.read_csv(dir_dict[filename], skiprows=[1]), master_df],
                    ignore_index=True,
                )
            master_df.drop_duplicates(subset=None, keep="first", inplace=True)
            master_df.to_csv(
                os.path.join(
                    self.directory, self.config["market_data_export"][pattern]
                ),
                index=False,
            )
            del master_df, dir_dict
