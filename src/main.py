from market_data import MarketData
from trade_analyzer import TradeAnalyzer

if __name__ == "__main__":
    market_data = MarketData()
    market_data.getData()
    trades = TradeAnalyzer()
    trades.analyzeData()
    print('done')
