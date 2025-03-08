import os
import json
import pandas as pd
import numpy as np


class TradeAnalyzer:
    """A class to analyze trade activities"""

    def analyzeData(self):
        self.__asset_allocation()

    def __init__(self, config_file="data/config.json"):
        self.config = {}
        # read self.config
        with open(config_file, "r") as f:
            self.config = json.load(f)
        self.data_directory = os.path.abspath(
            os.getcwd() + self.config["market_data_folder"]
        )
        self.trades_directory = os.path.abspath(
            os.getcwd() + self.config["trade_activities_folder"]
        )
        # market data checks
        if not os.path.exists(self.data_directory):
            os.makedirs(self.data_directory)
            if not os.path.exists(
                os.path.join(
                    self.data_directory, self.config["market_data_export"]["history"]
                )
            ):
                raise FileNotFoundError("No market dividend data found.")
            if not os.path.exists(
                os.path.join(
                    self.data_directory, self.config["market_data_export"]["dividend"]
                )
            ):
                raise FileNotFoundError("No market history data found.")
        # trade activities checks
        if not os.path.exists(self.trades_directory):
            os.makedirs(self.trades_directory)
            if not os.path.exists(
                os.path.join(
                    self.data_directory,
                    self.config["trade_activities_inputs"]["trade_activities_excel"],
                )
            ):
                raise FileNotFoundError("No trade activities found.")
            if not os.path.exists(
                os.path.join(
                    self.data_directory,
                    self.config["trade_activities_inputs"]["ticker_category_csv"],
                )
            ):
                raise FileNotFoundError("No ticker category defined.")

        #
        # read files and build basic tables
        #

        # read history df
        self.history_df = pd.read_csv(
            os.path.join(
                self.data_directory, self.config["market_data_export"]["history"]
            )
        )
        # process ticker
        self.history_df["Ticker"] = self.history_df["Ticker"].map(
            lambda x: x.lstrip("").rstrip(".TO")
        )
        # read activities and ticker category
        activities_file = os.path.join(
            self.trades_directory,
            self.config["trade_activities_inputs"]["trade_activities_excel"],
        )
        activities_excel = pd.ExcelFile(activities_file, engine="openpyxl")
        activities_excel_list = []
        for i in activities_excel.sheet_names:
            activities_excel_list.append(
                pd.read_excel(activities_file, sheet_name=i, engine="openpyxl")
            )
        self.activities_df = pd.concat(activities_excel_list)
        self.activities_df = self.activities_df.sort_values(by=["Date"])
        # add additional dates

        self.activities_df["Date"] = self.activities_df["Date"].dt.strftime("%Y-%m-%d")
        self.ticker_cat_df = pd.read_csv(
            os.path.join(
                self.trades_directory,
                self.config["trade_activities_inputs"]["ticker_category_csv"],
            )
        )
        # # get full trade df
        # self.fulltrade_df = self.ticker_cat_df\
        #     .merge(self.activities_df, on='Ticker')\
        #     .merge(self.history_df, on=['Ticker', 'Date'])

        # read dividend df
        self.dividend_df = pd.read_csv(
            os.path.join(
                self.data_directory, self.config["market_data_export"]["dividend"]
            )
        )

    def __asset_allocation(self):
        """Analyze allocation of asset"""
        # asset over time
        assetovertime_df = (
            self.activities_df[["Account", "Date", "Ticker", "Quantity"]]
            .merge(self.ticker_cat_df, on="Ticker")
            .merge(self.history_df, on=["Ticker", "Date"])
        )
        # asset cumulative quantity over time by account
        assetacctime_df = (
            pd.pivot_table(
                assetovertime_df,
                values="Quantity",
                index="Date",
                columns=["Account", "Ticker"],
                aggfunc=np.sum,
            )
            .fillna(0)
            .aggregate(np.cumsum)
            .stack(level=[0, 1])
            .reset_index()
            .rename(columns={0: "Quantity"})
            .merge(self.history_df, on=["Ticker", "Date"])
        )
        # calculate market value of asset over time
        assetacctime_df["Market_Value"] = (
            assetacctime_df["Close"] * assetacctime_df["Quantity"]
        )
        # generate pivot table of market asset value
        assetacctime_pivot_df = (
            pd.pivot_table(
                assetacctime_df,
                values="Market_Value",
                index="Date",
                columns="Account",
                aggfunc=np.sum,
            )
            .fillna(0)
            .reset_index()
        )
        assetacctime_pivot_df["Date"] = assetacctime_pivot_df["Date"].astype(
            "datetime64[ns]"
        )
        # asset cumulative quantity over time by category
        assetcattime_df = (
            pd.pivot_table(
                assetovertime_df,
                values="Quantity",
                index="Date",
                columns=["Category", "Ticker"],
                aggfunc=np.sum,
            )
            .fillna(0)
            .aggregate(np.cumsum)
            .stack(level=[0, 1])
            .reset_index()
            .rename(columns={0: "Quantity"})
            .merge(self.history_df, on=["Ticker", "Date"])
        )
        # calculate market value of asset over time
        assetcattime_df["Market_Value"] = (
            assetcattime_df["Close"] * assetcattime_df["Quantity"]
        )
        # generate pivot table of market asset value
        assetcattime_pivot_df = (
            pd.pivot_table(
                assetcattime_df,
                values="Market_Value",
                index="Date",
                columns="Category",
                aggfunc=np.sum,
            )
            .fillna(0)
            .reset_index()
        )
        assetcattime_pivot_df["Date"] = assetcattime_pivot_df["Date"].astype(
            "datetime64[ns]"
        )
        # generate pivot table of market asset allocation
        asset_pct_df = (
            pd.pivot_table(
                assetcattime_df,
                values="Market_Value",
                index=["Date", "Category"],
                aggfunc=np.sum,
            )
            .fillna(0)
            .groupby(level=0)
            .apply(lambda x: 100 * x / float(x.sum()))
            .reset_index()
        )
        asset_pct_pivot_df = (
            pd.pivot_table(
                asset_pct_df,
                values="Market_Value",
                index="Date",
                columns="Category",
                aggfunc=np.sum,
            )
            .fillna(0)
            .round(2)
            .reset_index()
        )
        asset_pct_pivot_df["Date"] = asset_pct_pivot_df["Date"].astype("datetime64[ns]")

        # write to excel
        with pd.ExcelWriter(
            self.config["analysis_output_excel"], engine="xlsxwriter"
        ) as writer:  # pylint: disable=abstract-class-instantiated
            assetacctime_pivot_df.to_excel(
                writer, sheet_name="Asset_Accounts", index=False
            )
            asset_pct_pivot_df.to_excel(
                writer, sheet_name="Asset_Allocation", index=False
            )
            assetcattime_pivot_df.to_excel(
                writer, sheet_name="Asset_Values", index=False
            )

        # write raw data to csv
        assetacctime_pivot_df.to_csv(
            self.config["analysis_output_excel"].replace(".xlsx", "_asset_acct.csv"),
            index=False,
        )
        asset_pct_pivot_df.to_csv(
            self.config["analysis_output_excel"].replace(".xlsx", "_asset_allo.csv"),
            index=False,
        )
        assetcattime_pivot_df.to_csv(
            self.config["analysis_output_excel"].replace(".xlsx", "_asset_val.csv"),
            index=False,
        )

    def __income_by_asset(self):
        """Analyze income by asset"""
        pass
