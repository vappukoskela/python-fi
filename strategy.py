try:
    import alpaca_trade_api
    print("OLD SDK: alpaca_trade_api is installed")
except ImportError:
    print("OLD SDK: NOT installed")

try:
    from alpaca.trading.client import TradingClient
    print("NEW SDK: alpaca-py is installed")
except ImportError:
    print("NEW SDK: NOT installed")

